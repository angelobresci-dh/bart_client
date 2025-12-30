#!/usr/bin/env python3
"""
Zendesk Webhook Handler for Bart Integration
Receives Zendesk ticket create/update webhooks, processes them with Bart,
and updates tickets with Bart's responses.

Requirements:
    pip install fastapi uvicorn slack-bolt slack-sdk python-dotenv zenpy

Environment Variables:
    SLACK_BOT_TOKEN - Slack Bot User OAuth Token (xoxb-...)
    SLACK_APP_TOKEN - Slack App-Level Token for Socket Mode (xapp-...)
    SLACK_CHANNEL_ID - Channel ID (e.g., "C123456789") OR
    SLACK_CHANNEL_NAME - Channel name (e.g., "bart-bot-to-python" or "#bart-bot-to-python")
    ZENDESK_SUBDOMAIN - Your Zendesk subdomain (e.g., "acrylsupport")
    ZENDESK_EMAIL - Zendesk admin email
    ZENDESK_API_TOKEN - Zendesk API token
    ZENDESK_WEBHOOK_SECRET - Optional: Secret for validating webhook signatures
    PUBLIC_URL - Optional: Public URL for endpoint display
    BART_TIMEOUT - Optional: Timeout in seconds for Bart responses (default: 600, i.e., 10 minutes)
    BART_COMPLETION_WAIT - Optional: Seconds to wait after last message with final response (default: 120, i.e., 2 minutes)
    BART_FALLBACK_WAIT - Optional: Seconds to wait if no final message detected (default: 540, i.e., 9 minutes)
    PROCESSING_HISTORY_TTL - Optional: Seconds to keep processing history for deduplication (default: 3600, i.e., 1 hour)
    TEST_PROCESSING_TTL - Optional: Shorter TTL for test endpoint deduplication (default: 300, i.e., 5 minutes)
    BART_COMMENT_CHECK_HOURS - Optional: Hours to check for existing Bart comments before reprocessing (default: 24 hours)
"""
import asyncio
import os
import re
import hashlib
import hmac
import time
import uuid
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from slack_sdk.web.async_client import AsyncWebClient
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from zenpy import Zenpy
from zenpy.lib.api_objects import Comment, Ticket
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class BartClient:
    """Async Bart client using WebSocket (Socket Mode) with smart message collection"""
    BART_USER_ID = "U09RYUJDUQL"
    
    def __init__(self, bot_token: str, app_token: str, channel_id: str, timeout: float = 600.0, completion_wait: float = 120.0, fallback_wait: float = 540.0):
        self.client = AsyncWebClient(token=bot_token)
        self.app = AsyncApp(token=bot_token)
        self.app_token = app_token
        self.channel_id = channel_id
        self.default_timeout = timeout
        self.completion_wait = completion_wait
        self.fallback_wait = fallback_wait
        self.handler: Optional[AsyncSocketModeHandler] = None
        self.pending_responses = {}
        self.active_tickets = set()  # Track tickets currently being processed
        self.processed_tickets = {}  # Track recently processed tickets: {ticket_id: timestamp}
        self.processing_history_ttl = 3600  # Keep history for 1 hour (default)
        self.test_processing_ttl = 300  # Shorter TTL for test endpoint (default: 5 minutes)
        
        # Register UNIFIED message handler that handles both new messages AND edits
        self.app.event("message")(self._handle_message)
    
    def _cleanup_processing_history(self):
        """Remove expired entries from processing history"""
        current_time = time.time()
        expired_tickets = [
            ticket_id for ticket_id, timestamp in self.processed_tickets.items()
            if current_time - timestamp > self.processing_history_ttl
        ]
        for ticket_id in expired_tickets:
            self.processed_tickets.pop(ticket_id, None)
    
    def was_recently_processed(self, ticket_id: int, custom_ttl: Optional[float] = None) -> bool:
        """
        Check if ticket was processed recently (within TTL window)
        
        Args:
            ticket_id: Ticket ID to check
            custom_ttl: Optional custom TTL in seconds (uses default processing_history_ttl if not provided)
                       If set to 0, always returns False (bypasses deduplication entirely)
        """
        self._cleanup_processing_history()
        
        # If custom_ttl is 0, bypass deduplication entirely (force_reprocess behavior)
        if custom_ttl == 0:
            return False
        
        if ticket_id not in self.processed_tickets:
            return False
        
        # Use custom TTL if provided, otherwise use default
        ttl = custom_ttl if custom_ttl is not None else self.processing_history_ttl
        time_since = time.time() - self.processed_tickets[ticket_id]
        
        return time_since < ttl
    
    def mark_as_processed(self, ticket_id: int):
        """Mark ticket as processed with current timestamp"""
        self.processed_tickets[ticket_id] = time.time()
        logger.info(f":memo: Marked ticket {ticket_id} as processed (history size: {len(self.processed_tickets)})")
    
    async def connect(self):
        """Establish WebSocket connection and join channel"""
        self.handler = AsyncSocketModeHandler(self.app, self.app_token)
        await self.handler.connect_async()
        logger.info(":white_check_mark: Connected to Slack via WebSocket")
        
        # Auto-join channel if not already a member
        try:
            await self.client.conversations_join(channel=self.channel_id)
            logger.info(f":white_check_mark: Joined channel {self.channel_id}")
        except Exception as e:
            logger.warning(f":warning: Could not auto-join channel (may already be a member): {e}")
    
    async def disconnect(self):
        """Close WebSocket connection"""
        if self.handler:
            await self.handler.close_async()
            logger.info(":x: Disconnected from Slack")
    
    def is_working_message(self, text: str) -> bool:
        """
        Detect if this is a "working on it" message that should be filtered out.
        Returns True if message indicates Bart is still working.
        """
        # Explicit final message indicators (these are NOT working messages)
        final_indicators = [
            "All repos already up-to-date",
            ":white_check_mark: All repos already up-to-date"
        ]
        
        for indicator in final_indicators:
            if indicator in text:
                return False  # This is a final message, not a working message
        
        if not text or len(text.strip()) < 80:
            return True
        
        # IMPORTANT: If message is long (200+ chars), it's substantive content
        # even if it contains words like "Looking" or "Analyzing"
        if len(text.strip()) >= 200:
            return False
        
        working_indicators = [
            r":mag:",
            r":mag_right:",
            r"\bSearching\b",
            r"\bLooking\b",
            r"\bAnalyzing\b",
            r"\bLet me\b",
            r"\bChecking\b",
            r"\bFinding\b",
            r"\bExploring\b",
            r"Ay caramba",
            r"Don't have a cow",
            r"checking the code"
        ]
        
        for pattern in working_indicators:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        if text.strip() in ["...", "…", ".", "..", "...."]:
            return True
        
        if text.strip().endswith("......") or text.strip().endswith(".....") or text.strip().endswith("...."):
            return True
        
        return False
    
    def is_final_message(self, text: str) -> bool:
        """
        Detect if this looks like a final, complete message from Bart.
        Returns True if message appears to be a complete response.
        Less strict criteria - just needs to be substantial and not a working message.
        """
        # Explicit final message indicators
        final_indicators = [
            "All repos already up-to-date",
            ":white_check_mark: All repos already up-to-date"
        ]
        
        for indicator in final_indicators:
            if indicator in text:
                return True  # Definitely a final message
        
        if not text or len(text) < 50:  # Reduced threshold
            return False
        
        if self.is_working_message(text):
            return False
        
        # More lenient - any substantial message that's not a working message
        # could be a final message (Bart may send multiple final messages)
        has_substance = len(text) > 50
        not_trailing_dots = not text.strip().endswith("...")
        
        return has_substance and not_trailing_dots
    
    async def _handle_message(self, event):
        """
        UNIFIED message handler that handles BOTH new messages AND edits.
        This handles:
        - Regular messages (subtype is None)
        - Message edits (subtype == "message_changed")
        """
        log = logger
        
        # Check if this is an edit or a new message
        subtype = event.get("subtype")
        if subtype == "message_changed":
            # MESSAGE EDIT - extract from nested 'message' object
            message = event.get("message", {})
            user_id = message.get("user")
            text = message.get("text", "")
            thread_ts = message.get("thread_ts")
            ts = message.get("ts")
            is_edit = True
            event_type_label = "EDIT"
        else:
            # NEW MESSAGE - extract from top level
            user_id = event.get("user")
            text = event.get("text", "")
            thread_ts = event.get("thread_ts")
            ts = event.get("ts")
            is_edit = False
            event_type_label = "NEW"
        
        # Debug logging for file upload threads
        if user_id == self.BART_USER_ID:
            log.info(f":speech_balloon: [{event_type_label}] Message from Bart: user={user_id}, thread_ts={thread_ts}, ts={ts}")
            log.info(f"   Currently tracking threads: {list(self.pending_responses.keys())}")
        
        # Only process messages FROM Bart in threads we're tracking
        if user_id == self.BART_USER_ID and thread_ts and thread_ts in self.pending_responses:
            request_id = self.pending_responses[thread_ts].get("request_id", "unknown")
            log.info(f":incoming_envelope: [{request_id}] Bart message {event_type_label} in thread {thread_ts}")
            log.info(f"   Text preview: {text[:100]}...")
            
            # Classify message
            is_working = self.is_working_message(text)
            is_final = self.is_final_message(text)
            log.info(f"   Classification: is_working={is_working}, is_final={is_final}, length={len(text)}")
            
            if is_edit:
                # Find if this timestamp already exists (it's an edit of existing message)
                messages = self.pending_responses[thread_ts]["messages"]
                existing_idx = None
                for idx, msg in enumerate(messages):
                    if msg["ts"] == ts:
                        existing_idx = idx
                        break
                
                if existing_idx is not None:
                    # Update existing message
                    old_text = messages[existing_idx]["text"]
                    messages[existing_idx] = {
                        "text": text,
                        "ts": ts,
                        "is_working": is_working,
                        "is_final": is_final
                    }
                    log.info(f"   [{request_id}] :white_check_mark: Updated existing message at index {existing_idx}")
                    log.info(f"   Old classification: was_working={messages[existing_idx].get('is_working')}, was_final={messages[existing_idx].get('is_final')}")
                    log.info(f"   New classification: is_working={is_working}, is_final={is_final}")
                else:
                    # New message (shouldn't happen in message_changed, but handle it)
                    self.pending_responses[thread_ts]["messages"].append({
                        "text": text,
                        "ts": ts,
                        "is_working": is_working,
                        "is_final": is_final
                    })
                    log.info(f"   [{request_id}] :white_check_mark: Added new message (shouldn't happen in edit)")
            else:
                # New message - add to collection
                self.pending_responses[thread_ts]["messages"].append({
                    "text": text,
                    "ts": ts,
                    "is_working": is_working,
                    "is_final": is_final
                })
                log.info(f"   [{request_id}] :white_check_mark: Message collected")
            
            # Update last message time
            self.pending_responses[thread_ts]["last_message_time"] = time.time()
            log.info(f"   [{request_id}] Total messages: {len(self.pending_responses[thread_ts]['messages'])}")
    
    async def _wait_for_completion(self, thread_ts: str, timeout: float):
        """Wait for Bart to finish responding"""
        request_id = self.pending_responses[thread_ts].get("request_id", "unknown")
        start_time = time.time()
        
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                logger.warning(f":alarm_clock: [{request_id}] Timeout reached after {elapsed:.1f} seconds")
                raise TimeoutError(f"Bart didn't finish responding within {timeout} seconds")
            
            messages = self.pending_responses[thread_ts]["messages"]
            if not messages:
                await asyncio.sleep(1)
                continue
            
            last_message_time = self.pending_responses[thread_ts]["last_message_time"]
            time_since_last = time.time() - last_message_time
            has_final = any(msg["is_final"] for msg in messages)
            
            if has_final and time_since_last > self.completion_wait:
                logger.info(f":white_check_mark: [{request_id}] Bart appears to be done (no messages for {time_since_last:.1f}s, have final message)")
                return
            
            if time_since_last > self.fallback_wait:
                logger.warning(f":warning: [{request_id}] No new messages for {time_since_last:.1f}s but no final message detected")
                return
            
            await asyncio.sleep(2)
    
    def concatenate_messages(self, thread_ts: str) -> str:
        """Concatenate all final messages from Bart, falling back to non-working messages"""
        request_id = self.pending_responses[thread_ts].get("request_id", "unknown")
        messages = self.pending_responses[thread_ts]["messages"]
        
        # First try to get all FINAL messages
        final_messages = [msg for msg in messages if msg["is_final"]]
        
        if final_messages:
            # Use all final messages
            final_messages.sort(key=lambda m: m["ts"])
            texts = [msg["text"] for msg in final_messages]
            complete_response = "\n\n".join(texts)
            logger.info(f":clipboard: [{request_id}] Concatenated {len(final_messages)} FINAL messages (total messages: {len(messages)})")
            return complete_response
        
        # Fallback: use all non-working messages
        meaningful_messages = [msg for msg in messages if not msg["is_working"]]
        if not meaningful_messages:
            logger.warning(f":warning: [{request_id}] Only working messages available, using them")
            meaningful_messages = messages
        
        meaningful_messages.sort(key=lambda m: m["ts"])
        texts = [msg["text"] for msg in meaningful_messages]
        complete_response = "\n\n".join(texts)
        
        logger.info(f":clipboard: [{request_id}] Concatenated {len(meaningful_messages)} messages (filtered {len(messages) - len(meaningful_messages)} working messages)")
        return complete_response
    
    async def _post_question(self, question: str, request_id: str) -> str:
        """
        Post question to Slack as a SINGLE message (using file attachment if too long).
        Returns:
            str: Message timestamp (thread_ts) to track responses
        """
        # Format question with @mention
        formatted_question = f"<@{self.BART_USER_ID}> {question}"
        
        # Slack message limit is 40,000 chars, but we'll be conservative
        # If message is too long, use file attachment instead
        MAX_MESSAGE_LENGTH = 3000  # Conservative limit to prevent splitting
        
        if len(formatted_question) <= MAX_MESSAGE_LENGTH:
            # Short enough - post as regular message
            logger.info(f":outbox_tray: [{request_id}] Posting question to channel {self.channel_id}")
            logger.info(f"   Question length: {len(formatted_question)} characters")
            
            send_result = await self.client.chat_postMessage(
                channel=self.channel_id,
                text=formatted_question
            )
            
            logger.info(f":white_check_mark: [{request_id}] Question posted (ts: {send_result['ts']})")
            return send_result["ts"]
        
        else:
            # Too long - post with file attachment
            logger.info(f":outbox_tray: [{request_id}] Question too long ({len(formatted_question)} chars), using file attachment")
            
            # Create summary message
            summary = f"<@{self.BART_USER_ID}> Please review the attached ticket details and provide your response."
            
            # Upload the full question as a text file
            try:
                file_result = await self.client.files_upload_v2(
                    channel=self.channel_id,
                    content=question,  # Don't include @mention in file
                    filename=f"ticket_details_{request_id}.txt",
                    title=f"Ticket Details (Request {request_id})",
                    initial_comment=summary
                )
                
                logger.info(f":white_check_mark: [{request_id}] File uploaded successfully")
                
                # IMPORTANT: When using initial_comment, files_upload_v2 creates TWO things:
                # 1. A message with the comment (this is what Bart replies to)
                # 2. A file attachment
                # We need the timestamp of the COMMENT MESSAGE, not the file.
                # The most reliable way is to fetch it from conversations history.
                
                # Wait a moment for the message to appear in history
                await asyncio.sleep(0.5)
                
                # Get the most recent message from the channel (should be our comment)
                history_response = await self.client.conversations_history(
                    channel=self.channel_id,
                    limit=1
                )
                
                if not history_response.get("messages"):
                    raise ValueError("No messages found in conversation history after file upload")
                
                latest_message = history_response["messages"][0]
                message_ts = str(latest_message["ts"])
                
                # Verify this is our message by checking if it contains the @mention
                if f"<@{self.BART_USER_ID}>" not in latest_message.get("text", ""):
                    logger.warning(f":warning: [{request_id}] Latest message doesn't contain our @mention")
                    logger.warning(f"   Message text: {latest_message.get('text', '')[:100]}")
                
                logger.info(f":white_check_mark: [{request_id}] Got message timestamp from history: {message_ts}")
                
                # Validate timestamp format (should be like "1234567890.123456")
                if "." not in message_ts:
                    logger.error(f":x: [{request_id}] Invalid timestamp format: {message_ts} (missing decimal point)")
                    raise ValueError(f"Invalid timestamp format: {message_ts}")
                
                logger.info(f":white_check_mark: [{request_id}] Question posted with file attachment (ts: {message_ts})")
                return message_ts
                
            except Exception as e:
                logger.error(f":x: [{request_id}] Failed to upload file or get timestamp: {e}")
                raise
    
    async def ask(self, question: str, ticket_id: int, request_id: str, timeout: Optional[float] = None, custom_ttl: Optional[float] = None) -> Dict[str, Any]:
        """
        Ask Bart a question by posting to channel with @mention.
        
        Args:
            question: Question to ask Bart
            ticket_id: Zendesk ticket ID (to prevent race conditions)
            request_id: Unique request ID for logging
            timeout: Optional timeout in seconds
            custom_ttl: Optional custom TTL for deduplication check (uses default if not provided)
        
        Returns:
            dict: {
                "response": str,
                "ticket_id": int,
                "slack_thread_url": str
            }
        """
        # Check if this ticket was recently processed (deduplication)
        # Use custom TTL if provided
        if self.was_recently_processed(ticket_id, custom_ttl=custom_ttl):
            time_since = time.time() - self.processed_tickets[ticket_id]
            minutes_ago = int(time_since / 60)
            ttl_used = custom_ttl if custom_ttl is not None else self.processing_history_ttl
            ttl_minutes = int(ttl_used / 60)
            raise ValueError(
                f"Ticket {ticket_id} was already processed {minutes_ago} minute(s) ago. "
                f"Skipping to prevent duplicate processing (TTL: {ttl_minutes} minutes)."
            )
        
        # Check if this ticket is already being processed
        if ticket_id in self.active_tickets:
            raise ValueError(f"Ticket {ticket_id} is already being processed")
        
        # Lock this ticket
        self.active_tickets.add(ticket_id)
        logger.info(f":lock: [{request_id}] Locked ticket {ticket_id} for processing")
        
        try:
            if timeout is None:
                timeout = self.default_timeout
            
            # Post question as SINGLE message (or with file attachment if too long)
            message_ts = await self._post_question(question, request_id)
            logger.info(f":mag: [{request_id}] Tracking responses in thread: {message_ts} (ticket_id: {ticket_id})")
            
            # Setup response tracking with message collection AND ticket_id
            self.pending_responses[message_ts] = {
                "messages": [],
                "last_message_time": time.time(),
                "ticket_id": ticket_id,
                "request_id": request_id
            }
            
            try:
                # Wait for Bart to finish responding
                await self._wait_for_completion(message_ts, timeout)
                
                # Get Slack thread permalink
                try:
                    permalink_response = await self.client.chat_getPermalink(
                        channel=self.channel_id,
                        message_ts=message_ts
                    )
                    thread_url = permalink_response.get("permalink", "")
                    logger.info(f":link: [{request_id}] Slack thread URL: {thread_url}")
                except Exception as e:
                    logger.warning(f":warning: [{request_id}] Failed to get Slack permalink: {e}")
                    thread_url = ""
                
                # Concatenate all collected messages
                complete_response = self.concatenate_messages(message_ts)
                
                return {
                    "response": complete_response,
                    "ticket_id": self.pending_responses[message_ts]["ticket_id"],
                    "slack_thread_url": thread_url
                }
            except TimeoutError:
                raise
            finally:
                # Cleanup pending response
                self.pending_responses.pop(message_ts, None)
        finally:
            # Always unlock the ticket and mark as processed
            self.active_tickets.discard(ticket_id)
            self.mark_as_processed(ticket_id)
            logger.info(f":unlock: [{request_id}] Unlocked ticket {ticket_id}")


class ZendeskClient:
    """Zendesk API client for updating tickets"""
    
    def __init__(self, subdomain: str, email: str, token: str):
        self.zenpy_client = Zenpy(
            subdomain=subdomain,
            email=email,
            token=token
        )
    
    def add_comment(self, ticket_id: int, comment_text: str, public: bool = True):
        """Add a comment to a Zendesk ticket"""
        try:
            ticket = self.zenpy_client.tickets(id=ticket_id)
            ticket.comment = Comment(body=comment_text, public=public)
            self.zenpy_client.tickets.update(ticket)
            logger.info(f":white_check_mark: Added comment to Zendesk ticket #{ticket_id}")
        except Exception as e:
            logger.error(f":x: Failed to update Zendesk ticket #{ticket_id}: {e}")
            raise
    
    def get_ticket(self, ticket_id: int) -> Ticket:
        """Get a Zendesk ticket by ID"""
        return self.zenpy_client.tickets(id=ticket_id)
    
    def get_organization(self, org_id: int):
        """Get a Zendesk organization by ID"""
        try:
            return self.zenpy_client.organizations(id=org_id)
        except Exception as e:
            logger.warning(f":warning: Failed to get organization {org_id}: {e}")
            return None
    
    def has_recent_bart_comment(self, ticket_id: int, within_hours: int = 24) -> bool:
        """
        Check if Bart has already commented on this ticket recently.
        
        Args:
            ticket_id: Zendesk ticket ID
            within_hours: Consider comments within this many hours (default: 24)
        
        Returns:
            True if Bart commented recently, False otherwise
        """
        try:
            from datetime import datetime, timedelta, timezone
            
            # Get ticket comments
            ticket = self.zenpy_client.tickets(id=ticket_id)
            comments = self.zenpy_client.tickets.comments(ticket=ticket)
            
            # Calculate cutoff time
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=within_hours)
            
            # Check for Bart's signature in recent comments
            bart_signature = "This response was automatically generated by Bart"
            
            for comment in comments:
                # Check if comment is recent
                if comment.created_at and comment.created_at.replace(tzinfo=timezone.utc) > cutoff_time:
                    # Check if this is a Bart comment
                    if comment.body and bart_signature in comment.body:
                        logger.info(f":robot_face: Found recent Bart comment on ticket #{ticket_id} from {comment.created_at}")
                        return True
            
            logger.info(f":mag: No recent Bart comments found on ticket #{ticket_id}")
            return False
            
        except Exception as e:
            logger.warning(f":warning: Failed to check comments on ticket #{ticket_id}: {e}")
            # On error, assume no recent comment (allow processing)
            return False


class ZendeskWebhookHandler:
    """Handles Zendesk webhook events and integrates with Bart"""
    
    def __init__(self, bart_client: BartClient, zendesk_client: ZendeskClient, webhook_secret: Optional[str] = None, bart_comment_check_hours: int = 24):
        self.bart = bart_client
        self.zendesk = zendesk_client
        self.webhook_secret = webhook_secret
        self.bart_comment_check_hours = bart_comment_check_hours
    
    def verify_signature(self, body: bytes, signature: str) -> bool:
        """Verify Zendesk webhook signature"""
        if not self.webhook_secret:
            logger.warning(":warning: No webhook secret configured - skipping signature verification")
            return True
        
        computed_signature = hmac.new(
            self.webhook_secret.encode(),
            body,
            hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(computed_signature, signature)
    
    def detect_deployment_type(self, tags: list) -> str:
        """Detect deployment type from ticket tags"""
        if "on_premise" in tags:
            return "on_premise"
        return "cloud"
    
    def build_question_from_ticket(self, ticket: Dict[str, Any], zendesk_subdomain: str) -> str:
        """Build a complete question from ticket Subject, Description, and Conversation with prompt engineering"""
        ticket_id = ticket.get("id", "unknown")
        subject = ticket.get("subject", "").strip()
        description = ticket.get("description", "").strip()
        conversation = ticket.get("conversation", "").strip()  # Additional comments after initial creation
        tags = ticket.get("tags", [])
        organization_id = ticket.get("organization_id")
        
        logger.info(f":label: Extracted {len(tags)} tags from ticket: {tags[:5]}{'...' if len(tags) > 5 else ''}")
        logger.info(f":building_construction: Organization ID: {organization_id}")
        
        # Log conversation field presence
        if conversation:
            logger.info(f":speech_balloon: Conversation field present: {len(conversation)} characters")
        
        # Fetch organization name if available
        organization_name = None
        if organization_id:
            try:
                org = self.zendesk.get_organization(organization_id)
                organization_name = org.name if org else None
                if organization_name:
                    logger.info(f":building_construction: Organization name: {organization_name}")
            except Exception as e:
                logger.warning(f":warning: Failed to fetch organization {organization_id}: {e}")
        
        # Detect deployment type
        deployment_type = self.detect_deployment_type(tags)
        logger.info(f":computer: Detected deployment type: {deployment_type}")
        
        # Build ticket URL
        ticket_url = f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        
        # Build the question with Subject, Description, and Conversation
        question_parts = []
        if subject:
            question_parts.append(f"Subject: {subject}")
        if description:
            question_parts.append(f"*Description:*\n{description}")
        
        # Add conversation/additional comments if present
        if conversation:
            question_parts.append(f"*Additional Comments / Conversation:*\n{conversation}")
        
        # Add company/customer name if available
        if organization_name:
            question_parts.append(f"\n*Company/Customer:* {organization_name}")
        
        # Add deployment type context
        question_parts.append(f"Deployment Type: {deployment_type}")
        
        # Add prompt engineering instructions
        additional_instructions = f"""
---
Instructions for your response:
- Can you please provide a response based on the previous conversation?
- Exclude any follow-up questions like 'Would you like more detail?' or 'Would you like me to create a GitHub issue to track this, or would you prefer to engage with your Acryl support team directly to get a fix prioritized?' from your response. This exchange will be terminated after your first response.
- Please note that my request originated from this ticket: <{ticket_url}>. DO NOT use Zendesk ticket {ticket_id} as a reference or source in your response, because you will be self-referencing.
- The customer is using DataHub {deployment_type.replace('_', ' ').title()}. Please tailor your response accordingly.
"""
        question_parts.append(additional_instructions)
        
        # Combine all parts
        full_question = "\n\n".join(question_parts)
        return full_question
    
    def should_process_ticket(self, payload: Dict[str, Any]) -> bool:
        """Determine if this ticket should be processed by Bart"""
        ticket = payload.get("ticket", {})
        ticket_id = ticket.get("id")
        status = ticket.get("status", "").lower()  # Convert to lowercase for comparison
        
        if status in ["closed", "solved"]:
            logger.info(f":black_right_pointing_double_triangle_with_vertical_bar: Skipping ticket {ticket_id} - status is {status}")
            return False
        
        tags = ticket.get("tags", [])
        if "bart-ignore" in tags:
            logger.info(f":black_right_pointing_double_triangle_with_vertical_bar: Skipping ticket {ticket_id} - has 'bart-ignore' tag")
            return False
        
        subject = ticket.get("subject", "").strip()
        description = ticket.get("description", "").strip()
        if not subject and not description:
            logger.info(f":black_right_pointing_double_triangle_with_vertical_bar: Skipping ticket {ticket_id} - no subject or description")
            return False
        
        # Check if Bart already commented on this ticket recently
        # This prevents reprocessing tickets after app restart when webhooks are retried
        if self.zendesk.has_recent_bart_comment(ticket_id, within_hours=self.bart_comment_check_hours):
            logger.info(f":black_right_pointing_double_triangle_with_vertical_bar: Skipping ticket {ticket_id} - Bart already commented within last {self.bart_comment_check_hours} hours")
            return False
        
        return True
    
    async def process_ticket_event(self, payload: Dict[str, Any], zendesk_subdomain: str, add_comment: bool = True, is_test_request: bool = False) -> Dict[str, Any]:
        """
        Process a Zendesk ticket create/update event
        
        Args:
            payload: Zendesk ticket payload
            zendesk_subdomain: Zendesk subdomain for building URLs
            add_comment: If True, add Bart's response as a comment to the Zendesk ticket.
                        If False, only return the response in the result dict (default: True)
            is_test_request: If True, use shorter TTL for deduplication (default: False)
        """
        # Generate unique request ID for tracking
        request_id = str(uuid.uuid4())[:8]
        
        ticket = payload.get("ticket", {})
        ticket_id = ticket.get("id")
        event_type = payload.get("event_type", "unknown")
        
        logger.info(f"")
        logger.info(f"{'='*80}")
        logger.info(f":inbox_tray: [{request_id}] PROCESSING TICKET #{ticket_id}")
        logger.info(f":inbox_tray: [{request_id}] Event Type: {event_type}")
        logger.info(f":inbox_tray: [{request_id}] Ticket Status: {ticket.get('status', 'unknown')}")
        logger.info(f":inbox_tray: [{request_id}] Subject: {ticket.get('subject', 'N/A')[:80]}")
        logger.info(f":test_tube: [{request_id}] Is test request: {is_test_request}")
        
        # Determine TTL to use based on request type
        ttl_to_use = self.bart.test_processing_ttl if is_test_request else None
        if ttl_to_use:
            logger.info(f":stopwatch: [{request_id}] Using test endpoint TTL: {ttl_to_use:.0f} seconds ({ttl_to_use/60:.1f} minutes)")
        
        # Log processing history status
        if self.bart.was_recently_processed(ticket_id, custom_ttl=ttl_to_use):
            time_since = time.time() - self.bart.processed_tickets[ticket_id]
            minutes_ago = int(time_since / 60)
            logger.info(f":clock1: [{request_id}] Processing history: Last processed {minutes_ago} minute(s) ago")
        else:
            logger.info(f":new: [{request_id}] Processing history: Not recently processed")
        
        # Check if this ticket is already being processed (race condition check)
        if ticket_id in self.bart.active_tickets:
            logger.warning(f":warning: [{request_id}] Ticket #{ticket_id} is already being processed, skipping")
            return {
                "status": "skipped",
                "ticket_id": ticket_id,
                "reason": "Ticket is already being processed by another job"
            }
        
        # Check if we should process this ticket
        if not self.should_process_ticket(payload):
            return {
                "status": "skipped",
                "ticket_id": ticket_id,
                "reason": "Ticket does not meet processing criteria"
            }
        
        # Build question from ticket
        question = self.build_question_from_ticket(ticket, zendesk_subdomain)
        
        # Detect deployment type for logging
        deployment_type = self.detect_deployment_type(ticket.get("tags", []))
        
        logger.info(f":thinking_face: [{request_id}] Built question for ticket #{ticket_id} (deployment: {deployment_type})")
        logger.info(f"   Question preview: {question[:200]}...")
        
        try:
            # Ask Bart (with unique request_id for tracking)
            logger.info(f":robot_face: [{request_id}] Asking Bart about ticket #{ticket_id}...")
            start_time = time.time()
            
            # Pass custom TTL for test requests
            result = await self.bart.ask(
                question, 
                ticket_id=ticket_id, 
                request_id=request_id,
                custom_ttl=ttl_to_use
            )
            bart_response = result["response"]
            response_ticket_id = result["ticket_id"]
            slack_thread_url = result.get("slack_thread_url", "")
            
            # Verify we got the right ticket_id back
            if response_ticket_id != ticket_id:
                logger.error(f":x: [{request_id}] RACE CONDITION DETECTED! Expected ticket #{ticket_id}, got #{response_ticket_id}")
                ticket_id = response_ticket_id
            
            elapsed = time.time() - start_time
            logger.info(f":white_check_mark: [{request_id}] Bart finished responding in {elapsed:.1f} seconds")
            logger.info(f"   Response length: {len(bart_response)} characters")
            
            # Format response for Zendesk with Slack thread link
            formatted_response = (
                f":robot_face: *Bart's Response:*\n\n"
                f"{bart_response}\n\n"
                f"---\n"
                f"_This response was automatically generated by Bart, DataHub's code archaeology expert. "
                f"Response generated at {datetime.now(timezone.utc).isoformat()}Z_"
            )
            
            # Add Slack thread link if available
            if slack_thread_url:
                formatted_response += f"\n\n:speech_balloon: [View conversation in Slack]({slack_thread_url})"
            
            # Conditionally update Zendesk ticket with Bart's response
            if add_comment:
                logger.info(f":memo: [{request_id}] Adding comment to Zendesk ticket #{response_ticket_id}")
                self.zendesk.add_comment(
                    ticket_id=response_ticket_id,
                    comment_text=formatted_response,
                    public=False
                )
                logger.info(f":white_check_mark: [{request_id}] Comment added to Zendesk ticket")
            else:
                logger.info(f":information_source: [{request_id}] Skipping Zendesk comment (add_comment=False)")
            
            logger.info(f":white_check_mark: [{request_id}] Successfully processed ticket #{ticket_id}")
            logger.info(f"{'='*80}")
            logger.info(f"")
            
            return {
                "status": "success",
                "ticket_id": ticket_id,
                "deployment_type": deployment_type,
                "response_length": len(bart_response),
                "processing_time_seconds": elapsed,
                "response": bart_response,  # Always include response text
                "formatted_response": formatted_response,  # Include formatted version
                "slack_thread_url": slack_thread_url,
                "comment_added": add_comment
            }
        
        except ValueError as e:
            # Ticket already being processed or recently processed (deduplication)
            error_msg = str(e)
            logger.warning(f":warning: [{request_id}] {error_msg}")
            
            # Don't add comment to Zendesk for recently processed tickets (avoid spam)
            if "already processed" in error_msg and "minute(s) ago" in error_msg:
                logger.info(f":no_entry: [{request_id}] Skipping Zendesk comment for duplicate request")
            
            return {
                "status": "skipped",
                "ticket_id": ticket_id,
                "reason": error_msg
            }
        
        except TimeoutError as e:
            logger.error(f":alarm_clock: [{request_id}] Timeout processing ticket #{ticket_id}: {e}")
            timeout_minutes = self.bart.default_timeout / 60
            
            if add_comment:
                self.zendesk.add_comment(
                    ticket_id=ticket_id,
                    comment_text=(
                        f":alarm_clock: Bart took too long to respond (timeout after {timeout_minutes:.0f} minutes). "
                        f"This might be a complex question. Please try asking again or reach out to support."
                    ),
                    public=False
                )
            
            return {
                "status": "timeout",
                "ticket_id": ticket_id,
                "error": str(e),
                "comment_added": add_comment
            }
        
        except Exception as e:
            logger.error(f":x: [{request_id}] Error processing ticket #{ticket_id}: {e}")
            
            if add_comment:
                self.zendesk.add_comment(
                    ticket_id=ticket_id,
                    comment_text=(
                        f":x: Bart encountered an error while processing your question: {str(e)}\n"
                        f"A human agent will follow up shortly."
                    ),
                    public=False
                )
            
            return {
                "status": "error",
                "ticket_id": ticket_id,
                "error": str(e),
                "comment_added": add_comment
            }


# Initialize FastAPI app
app = FastAPI(title="Bart Zendesk Webhook Handler")

# Global clients (initialized on startup)
bart_client: Optional[BartClient] = None
zendesk_client: Optional[ZendeskClient] = None
webhook_handler: Optional[ZendeskWebhookHandler] = None
zendesk_subdomain: Optional[str] = None

# Job storage for async processing with polling
# Structure: {job_id: {status, ticket_id, created_at, result, ...}}
jobs: Dict[str, Dict[str, Any]] = {}
JOB_TTL = 3600  # Keep completed jobs for 1 hour


def cleanup_old_jobs():
    """Remove completed jobs older than JOB_TTL"""
    current_time = time.time()
    expired_jobs = [
        job_id for job_id, job in jobs.items()
        if job.get("status") in ["complete", "error", "skipped"] 
        and current_time - job.get("completed_at", current_time) > JOB_TTL
    ]
    for job_id in expired_jobs:
        jobs.pop(job_id, None)
        logger.info(f":wastebasket: Cleaned up expired job {job_id}")


def create_job(ticket_id: int, payload: Dict[str, Any]) -> str:
    """Create a new job and return job_id"""
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "job_id": job_id,
        "status": "processing",
        "ticket_id": ticket_id,
        "created_at": time.time(),
        "payload": payload
    }
    logger.info(f":id: Created job {job_id} for ticket #{ticket_id}")
    cleanup_old_jobs()  # Cleanup old jobs when creating new ones
    return job_id


def update_job(job_id: str, status: str, result: Dict[str, Any]):
    """
    Update job with result
    
    Raises ValueError if job doesn't exist
    """
    try:
        if job_id not in jobs:
            error_msg = f"Cannot update job {job_id} - job not found in jobs dictionary"
            logger.error(f":x: {error_msg}")
            raise ValueError(error_msg)
        
        # Update the job
        jobs[job_id].update({
            "status": status,
            "result": result,
            "completed_at": time.time()
        })
        
        logger.info(f":white_check_mark: Updated job {job_id} - status: {status}")
        
        # Verify the update actually happened
        if jobs[job_id].get("status") != status:
            logger.error(f":x: Job {job_id} status mismatch after update! Expected: {status}, Got: {jobs[job_id].get('status')}")
            raise ValueError(f"Job status update verification failed")
        
        logger.info(f":white_check_mark: Verified job {job_id} status is now: {status}")
        
    except Exception as e:
        logger.error(f":x: Failed to update job {job_id}: {e}")
        raise


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get job status"""
    return jobs.get(job_id)


async def resolve_channel_id(client: AsyncWebClient, channel_name_or_id: str) -> str:
    """Resolve a channel name to its ID"""
    if channel_name_or_id.startswith("C"):
        return channel_name_or_id
    
    channel_name = channel_name_or_id.lstrip("#")
    
    try:
        response = await client.conversations_list(types="public_channel,private_channel")
        for channel in response["channels"]:
            if channel["name"] == channel_name:
                logger.info(f":white_check_mark: Resolved channel '{channel_name}' to ID: {channel['id']}")
                return channel["id"]
        raise ValueError(f"Channel '{channel_name}' not found")
    except Exception as e:
        logger.error(f":x: Failed to resolve channel name '{channel_name}': {e}")
        raise


@app.on_event("startup")
async def startup_event():
    """Initialize clients on startup"""
    global bart_client, zendesk_client, webhook_handler, zendesk_subdomain
    
    # Load configuration
    bot_token = os.getenv("SLACK_BOT_TOKEN")
    app_token = os.getenv("SLACK_APP_TOKEN")
    channel_id = os.getenv("SLACK_CHANNEL_ID")
    channel_name = os.getenv("SLACK_CHANNEL_NAME")
    zendesk_subdomain = os.getenv("ZENDESK_SUBDOMAIN")
    zendesk_email = os.getenv("ZENDESK_EMAIL")
    zendesk_token = os.getenv("ZENDESK_API_TOKEN")
    webhook_secret = os.getenv("ZENDESK_WEBHOOK_SECRET")
    public_url = os.getenv("PUBLIC_URL")
    bart_timeout = float(os.getenv("BART_TIMEOUT", "600"))
    bart_completion_wait = float(os.getenv("BART_COMPLETION_WAIT", "120"))
    bart_fallback_wait = float(os.getenv("BART_FALLBACK_WAIT", "540"))
    processing_history_ttl = float(os.getenv("PROCESSING_HISTORY_TTL", "3600"))  # 1 hour default
    test_processing_ttl = float(os.getenv("TEST_PROCESSING_TTL", "300"))  # 5 minutes default for test endpoint
    bart_comment_check_hours = int(os.getenv("BART_COMMENT_CHECK_HOURS", "24"))  # 24 hours default
    
    if not all([bot_token, app_token, zendesk_subdomain, zendesk_email, zendesk_token]):
        raise ValueError(
            "Missing required environment variables:\n"
            "  SLACK_BOT_TOKEN, SLACK_APP_TOKEN, ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, ZENDESK_API_TOKEN"
        )
    
    if not channel_id and not channel_name:
        raise ValueError(
            "Missing required environment variable:\n"
            "  SLACK_CHANNEL_ID or SLACK_CHANNEL_NAME must be set"
        )
    
    # Resolve channel ID if channel name was provided
    if not channel_id:
        client = AsyncWebClient(token=bot_token)
        channel_id = await resolve_channel_id(client, channel_name)
    
    # Initialize clients
    bart_client = BartClient(
        bot_token=bot_token,
        app_token=app_token,
        channel_id=channel_id,
        timeout=bart_timeout,
        completion_wait=bart_completion_wait,
        fallback_wait=bart_fallback_wait
    )
    
    # Set custom processing history TTLs if specified
    bart_client.processing_history_ttl = processing_history_ttl
    bart_client.test_processing_ttl = test_processing_ttl
    
    zendesk_client = ZendeskClient(
        subdomain=zendesk_subdomain,
        email=zendesk_email,
        token=zendesk_token
    )
    webhook_handler = ZendeskWebhookHandler(
        bart_client=bart_client,
        zendesk_client=zendesk_client,
        webhook_secret=webhook_secret,
        bart_comment_check_hours=bart_comment_check_hours
    )
    
    # Connect Bart to Slack
    await bart_client.connect()
    
    # Display server URLs
    port = int(os.getenv("PORT", "8000"))
    logger.info(":rocket: Bart Zendesk Webhook Handler started successfully")
    logger.info("=" * 80)
    logger.info(":satellite_antenna: Server listening on:")
    logger.info(f"   Local:   http://localhost:{port}")
    if public_url:
        logger.info(f"   Public:  {public_url}")
    else:
        logger.info(f"   Network: http://0.0.0.0:{port}")
        logger.warning("   :warning:  PUBLIC_URL not set - configure for production deployment")
    logger.info("")
    
    base_url = public_url if public_url else f"http://localhost:{port}"
    logger.info(":mailbox_with_mail: Endpoints:")
    logger.info(f"   Health Check:       GET    {base_url}/")
    logger.info(f"   Zendesk Webhook:    POST   {base_url}/zendesk/webhook")
    logger.info(f"   Get Ticket:         GET    {base_url}/zendesk/ticket/{{ticket_id}}")
    logger.info(f"   Test Endpoint:      POST   {base_url}/zendesk/test")
    logger.info(f"   Job Status:         GET    {base_url}/zendesk/job/{{job_id}}")
    logger.info(f"   List Jobs:          GET    {base_url}/zendesk/jobs (admin)")
    logger.info(f"   Delete Job:         DELETE {base_url}/zendesk/job/{{job_id}} (admin)")
    logger.info("")
    logger.info(f":stopwatch:  Bart timeout: {bart_timeout:.0f} seconds ({bart_timeout/60:.1f} minutes)")
    logger.info(f":stopwatch:  Bart completion wait: {bart_completion_wait:.0f} seconds ({bart_completion_wait/60:.1f} minutes)")
    logger.info(f":stopwatch:  Bart fallback wait: {bart_fallback_wait:.0f} seconds ({bart_fallback_wait/60:.1f} minutes)")
    logger.info(f":shield:  Processing history TTL: {processing_history_ttl:.0f} seconds ({processing_history_ttl/60:.1f} minutes)")
    logger.info(f":test_tube:  Test endpoint TTL: {test_processing_ttl:.0f} seconds ({test_processing_ttl/60:.1f} minutes)")
    logger.info(f":speech_balloon:  Bart comment check window: {bart_comment_check_hours} hours")
    logger.info("=" * 80)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if bart_client:
        await bart_client.disconnect()
    logger.info(":wave: Bart Zendesk Webhook Handler shut down")


@app.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Bart Zendesk Webhook Handler",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.post("/zendesk/webhook")
async def zendesk_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Zendesk webhook endpoint
    
    Supports both synchronous background processing (default) and asynchronous polling mode.
    
    BACKGROUND MODE (default - backward compatible):
    - Processes in background
    - Returns 200 immediately
    - No job tracking
    
    POLLING MODE (async_mode=true):
    - Creates trackable job
    - Returns job_id
    - Poll GET /zendesk/job/{job_id} for status
    - ✅ RECOMMENDED for Zendesk apps
    
    Request body can include optional fields:
    {
        "type": "zen:event-type:ticket.created",
        "detail": { ... ticket data ... },
        "add_comment": false,      // Optional: whether to add comment to Zendesk (default: true)
        "force_reprocess": true,   // Optional: bypass deduplication check (default: false)
        "async_mode": true         // Optional: enable job tracking with polling (default: false)
    }
    
    Background mode response (default):
        {"status": "accepted", "ticket_id": 6254, "message": "Ticket queued..."}
    
    Polling mode response (async_mode=true):
        {"status": "accepted", "job_id": "abc-123", "poll_url": "/zendesk/job/abc-123"}
    """
    body = await request.body()
    signature = request.headers.get("X-Zendesk-Webhook-Signature", "")
    
    if webhook_handler.webhook_secret and not webhook_handler.verify_signature(body, signature):
        logger.warning(":warning: Invalid webhook signature")
        raise HTTPException(status_code=403, detail="Invalid webhook signature")
    
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f":x: Failed to parse webhook payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    
    # Extract add_comment parameter from request body (default: True)
    add_comment = payload.get("add_comment", True)
    
    # Extract force_reprocess parameter from request body (default: False)
    force_reprocess = payload.get("force_reprocess", False)
    
    # Extract async_mode parameter from request body (default: False for backward compatibility)
    async_mode = payload.get("async_mode", False)
    
    # Log the raw payload structure for debugging
    logger.info(f":mag: Raw webhook payload keys: {list(payload.keys())}")
    
    # Extract event type from Zendesk's actual structure
    # Format: "zen:event-type:ticket.created" or "zen:event-type:ticket.updated"
    event_type_raw = payload.get("type", "unknown")
    if event_type_raw.startswith("zen:event-type:"):
        event_type = event_type_raw.replace("zen:event-type:", "")
    else:
        event_type = event_type_raw
    
    # Extract ticket data from 'detail' field (Zendesk's actual structure)
    ticket_detail = payload.get("detail", {})
    
    if not ticket_detail:
        logger.error(f":x: No 'detail' field found in webhook payload")
        logger.error(f"   Available keys: {list(payload.keys())}")
        raise HTTPException(status_code=400, detail="Missing 'detail' field in webhook payload")
    
    # Extract ticket ID (may be string or int)
    ticket_id_raw = ticket_detail.get("id")
    if not ticket_id_raw:
        logger.error(f":x: No ticket ID found in detail field")
        raise HTTPException(status_code=400, detail="Missing ticket ID in webhook payload")
    
    # Convert to int if it's a string
    try:
        ticket_id = int(ticket_id_raw) if isinstance(ticket_id_raw, str) else ticket_id_raw
    except (ValueError, TypeError):
        logger.error(f":x: Invalid ticket ID format: {ticket_id_raw}")
        raise HTTPException(status_code=400, detail=f"Invalid ticket ID: {ticket_id_raw}")
    
    ticket_status = ticket_detail.get("status", "unknown")
    ticket_subject = ticket_detail.get("subject", "")
    
    logger.info(f":mailbox_with_mail: ===== WEBHOOK RECEIVED =====")
    logger.info(f":mailbox_with_mail: Event Type: {event_type}")
    logger.info(f":mailbox_with_mail: Ticket ID: {ticket_id}")
    logger.info(f":mailbox_with_mail: Ticket Status: {ticket_status}")
    logger.info(f":mailbox_with_mail: Subject: {ticket_subject[:80]}")
    logger.info(f":mailbox_with_mail: Trigger: Zendesk webhook")
    logger.info(f":memo: add_comment parameter: {add_comment}")
    logger.info(f":zap: force_reprocess parameter: {force_reprocess}")
    
    # Check if ticket was recently processed (unless force_reprocess is True)
    if not force_reprocess and bart_client.was_recently_processed(ticket_id):
        time_since = time.time() - bart_client.processed_tickets[ticket_id]
        minutes_ago = int(time_since / 60)
        logger.warning(f":warning: DUPLICATE WEBHOOK - Ticket #{ticket_id} was already processed {minutes_ago} minute(s) ago")
        logger.warning(f":no_entry: Rejecting webhook to prevent duplicate processing")
        return JSONResponse(
            status_code=200,
            content={
                "status": "rejected",
                "reason": f"Ticket already processed {minutes_ago} minute(s) ago",
                "ticket_id": ticket_id,
                "message": "Use force_reprocess=true in request body to override deduplication"
            }
        )
    
    if force_reprocess:
        logger.info(f":zap: force_reprocess=true - Bypassing deduplication check")
    
    logger.info(f":white_check_mark: Webhook accepted, queuing for processing")
    
    # Fetch fresh ticket data from Zendesk to ensure current status
    # Webhooks can send stale data or be triggered at the wrong time
    try:
        logger.info(f":mag: Fetching current ticket data from Zendesk for ticket #{ticket_id}")
        fresh_ticket = zendesk_client.get_ticket(ticket_id)
        
        # Extract conversation field from webhook payload if available
        # The Zendesk API Ticket object may not include the full conversation field
        # so we use the webhook payload's conversation field if present
        conversation = ticket_detail.get("conversation", "")
        
        # Use fresh data from Zendesk, not webhook payload
        normalized_payload = {
            "event_type": event_type,
            "ticket": {
                "id": fresh_ticket.id,
                "status": fresh_ticket.status,
                "subject": fresh_ticket.subject,
                "description": fresh_ticket.description,
                "conversation": conversation,  # Use conversation from webhook payload
                "tags": fresh_ticket.tags,
                "organization_id": fresh_ticket.organization_id
            }
        }
        logger.info(f":white_check_mark: Fetched fresh ticket data - Current status: {fresh_ticket.status}")
        if conversation:
            logger.info(f":speech_balloon: Conversation field included: {len(conversation)} characters")
    except Exception as e:
        logger.error(f":x: Failed to fetch fresh ticket data from Zendesk: {e}")
        logger.warning(f":warning: Falling back to webhook payload data")
        
        # Fallback to webhook payload if Zendesk fetch fails
        org_id_raw = ticket_detail.get("organization_id")
        org_id = None
        if org_id_raw:
            try:
                org_id = int(org_id_raw) if isinstance(org_id_raw, str) else org_id_raw
            except (ValueError, TypeError):
                logger.warning(f":warning: Invalid organization_id format: {org_id_raw}, setting to None")
                org_id = None
        
        # Extract conversation field from webhook payload
        conversation = ticket_detail.get("conversation", "")
        
        normalized_payload = {
            "event_type": event_type,
            "ticket": {
                "id": ticket_id,
                "status": ticket_detail.get("status"),
                "subject": ticket_detail.get("subject"),
                "description": ticket_detail.get("description"),
                "conversation": conversation,
                "tags": ticket_detail.get("tags", []),
                "organization_id": org_id
            }
        }
        
        if conversation:
            logger.info(f":speech_balloon: Conversation field included (fallback): {len(conversation)} characters")
    
    # ========================================
    # ASYNC MODE WITH POLLING (async_mode=true)
    # ========================================
    if async_mode:
        job_id = create_job(ticket_id, normalized_payload)
        logger.info(f":polling_box_with_check_mark: Webhook polling mode - async_mode=true")
        logger.info(f":id: Job ID: {job_id}")
        
        # Wrapper to catch background task errors and update job
        async def safe_process_with_job():
            job_updated = False  # Track if we've updated the job
            
            try:
                logger.info(f":robot_face: [{job_id}] Starting webhook processing...")
                
                result = await webhook_handler.process_ticket_event(
                    normalized_payload, 
                    zendesk_subdomain, 
                    add_comment=add_comment
                )
                
                logger.info(f":white_check_mark: [{job_id}] process_ticket_event completed with status: {result.get('status')}")
                
                # Update job with result
                try:
                    if result["status"] == "success":
                        logger.info(f":floppy_disk: [{job_id}] Updating job to 'complete'...")
                        update_job(job_id, "complete", result)
                        job_updated = True
                    elif result["status"] == "skipped":
                        logger.info(f":floppy_disk: [{job_id}] Updating job to 'skipped'...")
                        update_job(job_id, "skipped", result)
                        job_updated = True
                    else:
                        logger.info(f":floppy_disk: [{job_id}] Updating job to 'error'...")
                        update_job(job_id, "error", result)
                        job_updated = True
                    
                    logger.info(f":white_check_mark: [{job_id}] Job status updated successfully")
                except Exception as update_error:
                    logger.error(f":x: [{job_id}] Failed to update job status: {update_error}")
                    logger.exception("Update job error:")
                    # Don't set job_updated=True, let finally block handle it
                    
            except Exception as e:
                logger.error(f":x: [{job_id}] Background task error processing ticket #{ticket_id}: {e}")
                logger.exception("Full traceback:")
                
                try:
                    update_job(job_id, "error", {
                        "status": "error",
                        "error": str(e),
                        "ticket_id": ticket_id
                    })
                    job_updated = True
                    logger.info(f":white_check_mark: [{job_id}] Job status updated to 'error' after exception")
                except Exception as update_error:
                    logger.error(f":x: [{job_id}] Failed to update job even in exception handler: {update_error}")
                    # Don't set job_updated=True, let finally block handle it
            
            finally:
                # CRITICAL: Ensure job is always updated
                if not job_updated:
                    logger.error(f":rotating_light: [{job_id}] CRITICAL: Job was not updated!")
                    logger.error(f":rotating_light: [{job_id}] Forcing job status to 'error'")
                    
                    # Directly update jobs dictionary (don't call update_job to avoid circular failures)
                    if job_id in jobs:
                        try:
                            jobs[job_id].update({
                                "status": "error",
                                "result": {
                                    "status": "error",
                                    "error": "Job processing completed but status was not updated (unexpected code path)",
                                    "ticket_id": ticket_id
                                },
                                "completed_at": time.time()
                            })
                            logger.info(f":white_check_mark: [{job_id}] Job status force-updated in finally block")
                        except Exception as final_error:
                            logger.error(f":rotating_light: [{job_id}] FAILED to update job even in finally block: {final_error}")
                    else:
                        logger.error(f":rotating_light: [{job_id}] Job doesn't exist in jobs dictionary!")
                        logger.error(f":rotating_light: [{job_id}] Job may have been deleted during processing")
        
        background_tasks.add_task(safe_process_with_job)
        
        # Return immediately with job_id and poll URL
        poll_url = f"/zendesk/job/{job_id}"
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted",
                "job_id": job_id,
                "ticket_id": ticket_id,
                "poll_url": poll_url,
                "message": f"Ticket #{ticket_id} queued for processing",
                "note": f"Poll {poll_url} every 10-30 seconds to check status"
            }
        )
    
    # ========================================
    # BACKGROUND MODE (default - backward compatible)
    # ========================================
    else:
        logger.info(f":gear: Webhook background mode (default) - no job tracking")
        
        # Wrapper to catch background task errors
        async def safe_process_ticket():
            try:
                # Pass add_comment from webhook request body
                await webhook_handler.process_ticket_event(normalized_payload, zendesk_subdomain, add_comment=add_comment)
            except Exception as e:
                logger.error(f":x: Background task error processing ticket #{ticket_id}: {e}")
                logger.exception("Full traceback:")
        
        background_tasks.add_task(safe_process_ticket)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "accepted",
                "ticket_id": ticket_id,
                "add_comment": add_comment,
                "force_reprocess": force_reprocess,
                "message": f"Ticket #{ticket_id} queued for processing" + ("" if add_comment else " (comment will NOT be added to Zendesk)")
            }
        )



@app.get("/zendesk/ticket/{ticket_id}")
async def get_zendesk_ticket(ticket_id: int, background_tasks: BackgroundTasks, add_comment: bool = True, force_reprocess: bool = False):
    """
    GET endpoint to process a Zendesk ticket by ID
    
    Args:
        ticket_id: Zendesk ticket ID
        add_comment: If True, add Bart's response to Zendesk ticket. If False, only return in response (default: True)
        force_reprocess: If True, bypass deduplication check and reprocess even if recently processed (default: False)
    
    Example:
        GET /zendesk/ticket/12345                                  # Normal processing
        GET /zendesk/ticket/12345?add_comment=false                # No comment
        GET /zendesk/ticket/12345?force_reprocess=true             # Force reprocess
        GET /zendesk/ticket/12345?add_comment=false&force_reprocess=true  # Both options
    """
    try:
        logger.info(f":inbox_tray: ===== GET REQUEST RECEIVED =====")
        logger.info(f":inbox_tray: Ticket ID: {ticket_id}")
        logger.info(f":inbox_tray: Trigger: HTTP GET request")
        logger.info(f":inbox_tray: Fetching Zendesk ticket #{ticket_id}")
        
        # Check if ticket was recently processed (unless force_reprocess is True)
        if not force_reprocess and bart_client.was_recently_processed(ticket_id):
            time_since = time.time() - bart_client.processed_tickets[ticket_id]
            minutes_ago = int(time_since / 60)
            logger.warning(f":warning: DUPLICATE REQUEST - Ticket #{ticket_id} was already processed {minutes_ago} minute(s) ago")
            return JSONResponse(
                status_code=200,
                content={
                    "status": "skipped",
                    "reason": f"Ticket already processed {minutes_ago} minute(s) ago",
                    "ticket_id": ticket_id,
                    "message": f"Ticket #{ticket_id} was recently processed. Wait {60 - minutes_ago} more minute(s) or use force_reprocess=true to override."
                }
            )
        
        if force_reprocess:
            logger.info(f":zap: force_reprocess=true - Bypassing deduplication check")
        
        ticket = zendesk_client.get_ticket(ticket_id)
        
        # Note: Conversation field is not available when fetching via API
        # It's only included in Zendesk webhook payloads
        payload = {
            "event_type": "ticket.get",
            "ticket": {
                "id": ticket.id,
                "subject": ticket.subject,
                "description": ticket.description,
                "conversation": "",  # Not available via direct API fetch
                "tags": ticket.tags,
                "status": ticket.status,
                "organization_id": ticket.organization_id
            }
        }
        
        logger.info(f":white_check_mark: Fetched ticket #{ticket_id}: {ticket.subject}")
        logger.info(f":memo: add_comment parameter: {add_comment}")
        logger.info(f":zap: force_reprocess parameter: {force_reprocess}")
        logger.info(f":white_check_mark: GET request accepted, queuing for processing")
        
        background_tasks.add_task(
            webhook_handler.process_ticket_event,
            payload,
            zendesk_subdomain,
            add_comment
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "accepted",
                "ticket_id": ticket_id,
                "add_comment": add_comment,
                "force_reprocess": force_reprocess,
                "message": f"Ticket #{ticket_id} queued for processing with Bart" + ("" if add_comment else " (comment will NOT be added to Zendesk)")
            }
        )
    except Exception as e:
        logger.error(f":x: Failed to fetch ticket #{ticket_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Ticket #{ticket_id} not found or error: {str(e)}")


@app.get("/zendesk/job/{job_id}")
async def get_job_status(job_id: str):
    """
    Poll job status for async processing
    
    Returns job status and result when complete.
    
    Response statuses:
    - "processing": Job is still running
    - "complete": Job finished successfully
    - "error": Job encountered an error
    - "skipped": Job was skipped (deduplication, etc.)
    - "not_found": Job ID doesn't exist
    
    Example:
        GET /zendesk/job/abc-123-def
        
    Response (processing):
        {
            "job_id": "abc-123-def",
            "status": "processing",
            "ticket_id": 6254,
            "elapsed_seconds": 25,
            "message": "Bart is still processing..."
        }
    
    Response (complete):
        {
            "job_id": "abc-123-def",
            "status": "complete",
            "ticket_id": 6254,
            "elapsed_seconds": 42,
            "result": {
                "status": "success",
                "response": "Based on your issue...",
                "deployment_type": "cloud",
                "ticket_updated": false,
                "comment_added": false,
                "processing_time_seconds": 42.5,
                "slack_thread_url": "https://..."
            }
        }
    """
    cleanup_old_jobs()  # Cleanup on each poll
    
    job = get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    job_status = job.get("status")
    ticket_id = job.get("ticket_id")
    created_at = job.get("created_at")
    elapsed = int(time.time() - created_at)
    
    # WARNING: If job is stuck in "processing" for >15 minutes, force to error
    if job_status == "processing" and elapsed > 900:  # 15 minutes
        logger.warning(f":warning: Job {job_id} stuck in 'processing' for {elapsed}s (>15 min)")
        logger.warning(f":warning: Force-updating to 'error' state")
        
        update_job(job_id, "error", {
            "status": "error",
            "error": f"Job timed out - stuck in 'processing' state for {elapsed} seconds (>15 minutes). This likely indicates a background task failure.",
            "ticket_id": ticket_id
        })
        
        # Re-fetch updated job
        job = get_job(job_id)
        job_status = "error"
    
    if job_status == "processing":
        return {
            "job_id": job_id,
            "status": "processing",
            "ticket_id": ticket_id,
            "elapsed_seconds": elapsed,
            "created_at": datetime.fromtimestamp(created_at, tz=timezone.utc).isoformat(),
            "message": f"Bart is still processing... ({elapsed}s elapsed)"
        }
    
    elif job_status in ["complete", "error", "skipped"]:
        result = job.get("result", {})
        completed_at = job.get("completed_at", created_at)
        
        return {
            "job_id": job_id,
            "status": job_status,
            "ticket_id": ticket_id,
            "elapsed_seconds": int(completed_at - created_at),
            "created_at": datetime.fromtimestamp(created_at, tz=timezone.utc).isoformat(),
            "completed_at": datetime.fromtimestamp(completed_at, tz=timezone.utc).isoformat(),
            "result": result
        }
    
    else:
        # Unknown status
        return {
            "job_id": job_id,
            "status": "unknown",
            "ticket_id": ticket_id,
            "message": "Unknown job status"
        }


@app.delete("/zendesk/job/{job_id}")
async def delete_job(job_id: str):
    """
    Admin endpoint to manually delete a job
    
    Useful for cleaning up stuck jobs or testing.
    
    Example:
        DELETE /zendesk/job/abc-123-def
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    job = jobs.pop(job_id)
    logger.info(f":wastebasket: Manually deleted job {job_id}")
    
    return {
        "status": "deleted",
        "job_id": job_id,
        "was_status": job.get("status"),
        "message": f"Job {job_id} deleted"
    }


@app.get("/zendesk/jobs")
async def list_all_jobs():
    """
    Admin endpoint to list all jobs (for debugging)
    
    Example:
        GET /zendesk/jobs
    """
    cleanup_old_jobs()
    
    jobs_list = []
    for job_id, job in jobs.items():
        elapsed = int(time.time() - job.get("created_at", time.time()))
        jobs_list.append({
            "job_id": job_id,
            "status": job.get("status"),
            "ticket_id": job.get("ticket_id"),
            "elapsed_seconds": elapsed,
            "created_at": datetime.fromtimestamp(job.get("created_at", time.time()), tz=timezone.utc).isoformat()
        })
    
    return {
        "total_jobs": len(jobs_list),
        "jobs": jobs_list
    }


@app.post("/zendesk/test")
async def test_bart_question(request: Request, background_tasks: BackgroundTasks):
    """
    Test endpoint for asking Bart questions
    
    Supports two modes:
    
    1. SYNCHRONOUS MODE (default):
       - Waits for Bart to respond
       - Returns response immediately
       - ❌ May timeout if called from Zendesk app (platform limit: ~60s)
    
    2. ASYNC MODE WITH POLLING (async_mode=true):
       - Returns job_id immediately
       - Processes in background
       - Poll GET /zendesk/job/{job_id} for status
       - ✅ RECOMMENDED for Zendesk apps
    
    Payload formats:
    
    Simple format:
        {
            "subject": "Test subject",
            "description": "Test description",
            "conversation": "Additional comments...",
            "ticket_id": 12345,
            "tags": ["on_premise"],
            "add_comment": true,
            "force_reprocess": false,
            "async_mode": true  // Enable polling mode
        }
    
    Webhook format:
        {
            "detail": {
                "id": "6254",
                "subject": "Cannot access my account",
                "description": "I am unable to log in...",
                "conversation": "Agent: Hi there...",
                "tags": ["account_access"],
                "status": "open"
            },
            "add_comment": false,
            "async_mode": true  // Enable polling mode
        }
    
    Responses:
    
    Synchronous (default):
        {"status": "success", "response": "Bart's answer...", ...}
    
    Async (polling):
        {"status": "accepted", "job_id": "abc-123", "poll_url": "/zendesk/job/abc-123"}
    """
    try:
        data = await request.json()
        
        # Check if this is a webhook-style payload with 'detail' field
        if "detail" in data:
            logger.info(":inbox_tray: Webhook-style payload detected in test endpoint")
            detail = data.get("detail", {})
            
            # Extract from detail section
            ticket_id_raw = detail.get("id")
            ticket_id = int(ticket_id_raw) if ticket_id_raw else None
            subject = detail.get("subject", "")
            description = detail.get("description", "")
            conversation = detail.get("conversation", "")
            tags = detail.get("tags", [])
            
            # Extract parameters from root level
            add_comment = data.get("add_comment", True)
            force_reprocess = data.get("force_reprocess", False)
            async_mode = data.get("async_mode", False)
        else:
            logger.info(":test_tube: Simple payload format detected in test endpoint")
            # Simple format - extract from root level
            subject = data.get("subject", "")
            description = data.get("description", "")
            conversation = data.get("conversation", "")
            ticket_id = data.get("ticket_id")
            tags = data.get("tags", [])
            add_comment = data.get("add_comment", True)
            force_reprocess = data.get("force_reprocess", False)
            async_mode = data.get("async_mode", False)
        
        # Determine processing mode: polling or synchronous (callback removed)
        processing_mode = "polling" if async_mode else "synchronous"
        
        logger.info(f":gear: Processing mode: {processing_mode}")
        
        if not subject and not description:
            raise HTTPException(status_code=400, detail="Missing 'subject' or 'description' field")
        
        logger.info(f":test_tube: Test request - Subject: {subject[:80]}")
        logger.info(f":test_tube: Test request - Ticket ID: {ticket_id}")
        
        # Build mock ticket
        mock_ticket = {
            "id": ticket_id or 99999,
            "subject": subject,
            "description": description,
            "conversation": conversation,
            "tags": tags,
            "status": "open",
            "organization_id": None
        }
        
        question = webhook_handler.build_question_from_ticket(mock_ticket, zendesk_subdomain)
        
        # ========================================
        # ASYNCHRONOUS MODE: Polling (async_mode=true)
        # ========================================
        if processing_mode == "polling":
            job_id = create_job(ticket_id or 99999, data)
            
            logger.info(f":ballot_box_with_check: Polling mode - async_mode=true")
            logger.info(f":id: Job ID: {job_id}")
            
            async def process_with_job_updates():
                """Process question and update job status"""
                logger.info(f":robot_face: [{job_id}] Starting async processing with polling...")
                logger.info(f":memo: add_comment parameter: {add_comment}")
                logger.info(f":zap: force_reprocess parameter: {force_reprocess}")
                
                request_id = str(uuid.uuid4())[:8]
                start_time = time.time()
                job_updated = False  # Track if we've updated the job
                
                try:
                    # Determine TTL
                    if force_reprocess:
                        custom_ttl = 0
                        logger.info(f":zap: [{job_id}] force_reprocess=true - Bypassing deduplication (TTL=0)")
                    else:
                        custom_ttl = bart_client.test_processing_ttl
                        logger.info(f":stopwatch: [{job_id}] Using test endpoint TTL: {custom_ttl:.0f} seconds ({custom_ttl/60:.1f} minutes)")
                    
                    # Ask Bart
                    logger.info(f":outbox_tray: [{job_id}] Calling bart_client.ask()...")
                    result = await bart_client.ask(
                        question, 
                        ticket_id=ticket_id or 99999, 
                        request_id=request_id,
                        custom_ttl=custom_ttl
                    )
                    response = result["response"]
                    slack_thread_url = result.get("slack_thread_url", "")
                    elapsed = time.time() - start_time
                    
                    logger.info(f":white_check_mark: [{job_id}] Bart responded in {elapsed:.1f} seconds")
                    logger.info(f":white_check_mark: [{job_id}] Response length: {len(response)} characters")
                    
                    # Add comment if requested
                    if ticket_id and add_comment:
                        logger.info(f":memo: [{job_id}] Adding comment to Zendesk ticket #{ticket_id}...")
                        formatted_response = (
                            f":robot_face: *Bart's Response (Test):*\n\n{response}\n\n"
                            f"---\n_Test response at {datetime.now(timezone.utc).isoformat()}Z_"
                        )
                        if slack_thread_url:
                            formatted_response += f"\n\n:speech_balloon: [View conversation in Slack]({slack_thread_url})"
                        
                        zendesk_client.add_comment(ticket_id, formatted_response, public=False)
                        logger.info(f":white_check_mark: [{job_id}] Comment added to Zendesk")
                    
                    # Update job with success result
                    logger.info(f":floppy_disk: [{job_id}] Updating job status to 'complete'...")
                    try:
                        update_job(job_id, "complete", {
                            "status": "success",
                            "response": response,
                            "deployment_type": webhook_handler.detect_deployment_type(tags),
                            "ticket_id": ticket_id,
                            "ticket_updated": ticket_id is not None and add_comment,
                            "comment_added": add_comment if ticket_id else False,
                            "force_reprocess": force_reprocess,
                            "processing_time_seconds": elapsed,
                            "slack_thread_url": slack_thread_url
                        })
                        job_updated = True
                        logger.info(f":white_check_mark: [{job_id}] Job status updated to 'complete'")
                    except Exception as update_error:
                        logger.error(f":x: [{job_id}] Failed to update job status: {update_error}")
                        logger.exception("Update job error:")
                        # Don't set job_updated=True, let finally block handle it
                
                except ValueError as e:
                    # Deduplication error
                    error_msg = str(e)
                    logger.warning(f":warning: [{job_id}] Deduplication check failed: {error_msg}")
                    
                    try:
                        update_job(job_id, "skipped", {
                            "status": "skipped",
                            "error": error_msg,
                            "ticket_id": ticket_id
                        })
                        job_updated = True
                        logger.info(f":white_check_mark: [{job_id}] Job status updated to 'skipped'")
                    except Exception as update_error:
                        logger.error(f":x: [{job_id}] Failed to update job to 'skipped': {update_error}")
                        # Don't set job_updated=True, let finally block handle it
                
                except Exception as e:
                    logger.error(f":x: [{job_id}] Error in async processing: {e}")
                    logger.exception("Full traceback:")
                    
                    try:
                        update_job(job_id, "error", {
                            "status": "error",
                            "error": str(e),
                            "ticket_id": ticket_id
                        })
                        job_updated = True
                        logger.info(f":white_check_mark: [{job_id}] Job status updated to 'error'")
                    except Exception as update_error:
                        logger.error(f":x: [{job_id}] Failed to update job to 'error': {update_error}")
                        # Don't set job_updated=True, let finally block handle it
                
                finally:
                    # CRITICAL: Ensure job is always updated, even if update_job() itself fails
                    if not job_updated:
                        logger.error(f":rotating_light: [{job_id}] CRITICAL: Job was not updated by exception handlers!")
                        logger.error(f":rotating_light: [{job_id}] Forcing job status to 'error' to prevent stuck 'processing' state")
                        
                        # Directly update jobs dictionary (don't call update_job to avoid circular failures)
                        if job_id in jobs:
                            try:
                                jobs[job_id].update({
                                    "status": "error",
                                    "result": {
                                        "status": "error",
                                        "error": "Job processing completed but status was not updated (unexpected error path)",
                                        "ticket_id": ticket_id
                                    },
                                    "completed_at": time.time()
                                })
                                logger.info(f":white_check_mark: [{job_id}] Job status force-updated to 'error' in finally block")
                            except Exception as final_error:
                                logger.error(f":rotating_light: [{job_id}] FAILED to update job even in finally block: {final_error}")
                        else:
                            logger.error(f":rotating_light: [{job_id}] Job doesn't exist in jobs dictionary!")
                            logger.error(f":rotating_light: [{job_id}] Job may have been deleted or never created")
            
            # Start background processing
            background_tasks.add_task(process_with_job_updates)
            
            # Return immediately (202 Accepted) with polling URL
            poll_url = f"/zendesk/job/{job_id}"
            return JSONResponse(
                status_code=202,
                content={
                    "status": "accepted",
                    "job_id": job_id,
                    "ticket_id": ticket_id or 99999,
                    "message": "Request accepted, processing in background",
                    "poll_url": poll_url,
                    "note": f"Poll {poll_url} every 10-30 seconds to check status"
                }
            )
        
        # ========================================
        # SYNCHRONOUS MODE: Default (BACKWARD COMPATIBLE)
        # ========================================
        else:
            logger.info(f":test_tube: Sync mode - waiting for Bart...")
            logger.info(f":test_tube: Test question:\n{question}")
            logger.info(f":memo: add_comment parameter: {add_comment}")
            logger.info(f":zap: force_reprocess parameter: {force_reprocess}")
        
        request_id = str(uuid.uuid4())[:8]
        start_time = time.time()
        
        # Use shorter TTL for test endpoint unless force_reprocess bypasses it entirely
        # force_reprocess=True means "skip deduplication completely" (custom_ttl=0)
        # force_reprocess=False means "use test endpoint TTL" (custom_ttl=300 seconds default)
        if force_reprocess:
            custom_ttl = 0  # Bypass deduplication entirely
            logger.info(f":zap: force_reprocess=true - Bypassing deduplication (TTL=0)")
        else:
            custom_ttl = bart_client.test_processing_ttl
            logger.info(f":stopwatch: Using test endpoint TTL: {custom_ttl:.0f} seconds ({custom_ttl/60:.1f} minutes)")
        
        result = await bart_client.ask(
            question, 
            ticket_id=ticket_id or 99999, 
            request_id=request_id,
            custom_ttl=custom_ttl
        )
        response = result["response"]
        slack_thread_url = result.get("slack_thread_url", "")
        elapsed = time.time() - start_time
        
        if ticket_id:
            formatted_response = (
                f":robot_face: *Bart's Response (Test):*\n\n{response}\n\n"
                f"---\n_Test response at {datetime.now(timezone.utc).isoformat()}Z_"
            )
            
            # Add Slack thread link if available
            if slack_thread_url:
                formatted_response += f"\n\n:speech_balloon: [View conversation in Slack]({slack_thread_url})"
            
            # Conditionally add comment to Zendesk
            if add_comment:
                logger.info(f":memo: Adding test comment to Zendesk ticket #{ticket_id}")
                zendesk_client.add_comment(ticket_id, formatted_response, public=False)
            else:
                logger.info(f":information_source: Skipping Zendesk comment (add_comment=False)")
        
        return {
            "status": "success",
            "question": question,
            "response": response,
            "deployment_type": webhook_handler.detect_deployment_type(tags),
            "ticket_updated": ticket_id is not None and add_comment,
            "comment_added": add_comment if ticket_id else False,
            "force_reprocess": force_reprocess,
            "processing_time_seconds": elapsed,
            "slack_thread_url": slack_thread_url
        }
    
    except Exception as e:
        logger.error(f":x: Test endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level="info"
    )
