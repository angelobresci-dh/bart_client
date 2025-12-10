#!/usr/bin/env python3
"""
Zendesk Webhook Handler for Bart Integration
Receives Zendesk ticket create/update webhooks, processes them with Bart,
and updates tickets with Bart's responses.

Requirements:
    pip install fastapi uvicorn slack-bolt slack-sdk python-dotenv httpx zenpy

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
        self.processing_history_ttl = 3600  # Keep history for 1 hour
        
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
    
    def was_recently_processed(self, ticket_id: int) -> bool:
        """Check if ticket was processed recently (within TTL window)"""
        self._cleanup_processing_history()
        return ticket_id in self.processed_tickets
    
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
    
    async def ask(self, question: str, ticket_id: int, request_id: str, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Ask Bart a question by posting to channel with @mention.
        
        Args:
            question: Question to ask Bart
            ticket_id: Zendesk ticket ID (to prevent race conditions)
            request_id: Unique request ID for logging
            timeout: Optional timeout in seconds
        
        Returns:
            dict: {
                "response": str,
                "ticket_id": int,
                "slack_thread_url": str
            }
        """
        # Check if this ticket was recently processed (deduplication)
        if self.was_recently_processed(ticket_id):
            time_since = time.time() - self.processed_tickets[ticket_id]
            minutes_ago = int(time_since / 60)
            raise ValueError(
                f"Ticket {ticket_id} was already processed {minutes_ago} minute(s) ago. "
                f"Skipping to prevent duplicate processing."
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
        """Build a complete question from ticket Subject and Description with prompt engineering"""
        ticket_id = ticket.get("id", "unknown")
        subject = ticket.get("subject", "").strip()
        description = ticket.get("description", "").strip()
        tags = ticket.get("tags", [])
        organization_id = ticket.get("organization_id")
        
        # Fetch organization name if available
        organization_name = None
        if organization_id:
            try:
                org = self.zendesk.get_organization(organization_id)
                organization_name = org.name if org else None
            except Exception as e:
                logger.warning(f":warning: Failed to fetch organization {organization_id}: {e}")
        
        # Detect deployment type
        deployment_type = self.detect_deployment_type(tags)
        
        # Build ticket URL
        ticket_url = f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        
        # Build the question with Subject and Description
        question_parts = []
        if subject:
            question_parts.append(f"Subject: {subject}")
        if description:
            question_parts.append(f"*Description:*\n{description}")
        
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
        status = ticket.get("status", "")
        
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
    
    async def process_ticket_event(self, payload: Dict[str, Any], zendesk_subdomain: str) -> Dict[str, Any]:
        """Process a Zendesk ticket create/update event"""
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
        
        # Log processing history status
        if self.bart.was_recently_processed(ticket_id):
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
            
            result = await self.bart.ask(question, ticket_id=ticket_id, request_id=request_id)
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
            
            # Update Zendesk ticket with Bart's response (private comment)
            self.zendesk.add_comment(
                ticket_id=response_ticket_id,
                comment_text=formatted_response,
                public=False
            )
            
            logger.info(f":white_check_mark: [{request_id}] Successfully processed ticket #{ticket_id}")
            logger.info(f"{'='*80}")
            logger.info(f"")
            
            return {
                "status": "success",
                "ticket_id": ticket_id,
                "deployment_type": deployment_type,
                "response_length": len(bart_response),
                "processing_time_seconds": elapsed
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
                "error": str(e)
            }
        
        except Exception as e:
            logger.error(f":x: [{request_id}] Error processing ticket #{ticket_id}: {e}")
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
                "error": str(e)
            }


# Initialize FastAPI app
app = FastAPI(title="Bart Zendesk Webhook Handler")

# Global clients (initialized on startup)
bart_client: Optional[BartClient] = None
zendesk_client: Optional[ZendeskClient] = None
webhook_handler: Optional[ZendeskWebhookHandler] = None
zendesk_subdomain: Optional[str] = None


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
    
    # Set custom processing history TTL if specified
    bart_client.processing_history_ttl = processing_history_ttl
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
    logger.info(f"   Health Check:       GET  {base_url}/")
    logger.info(f"   Zendesk Webhook:    POST {base_url}/zendesk/webhook")
    logger.info(f"   Get Ticket:         GET  {base_url}/zendesk/ticket/{{ticket_id}}")
    logger.info(f"   Test Endpoint:      POST {base_url}/zendesk/test")
    logger.info("")
    logger.info(f":stopwatch:  Bart timeout: {bart_timeout:.0f} seconds ({bart_timeout/60:.1f} minutes)")
    logger.info(f":stopwatch:  Bart completion wait: {bart_completion_wait:.0f} seconds ({bart_completion_wait/60:.1f} minutes)")
    logger.info(f":stopwatch:  Bart fallback wait: {bart_fallback_wait:.0f} seconds ({bart_fallback_wait/60:.1f} minutes)")
    logger.info(f":shield:  Processing history TTL: {processing_history_ttl:.0f} seconds ({processing_history_ttl/60:.1f} minutes)")
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
    """Zendesk webhook endpoint"""
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
    
    event_type = payload.get("event_type", "unknown")
    ticket = payload.get("ticket", {})
    ticket_id = ticket.get("id", "unknown")
    ticket_status = ticket.get("status", "unknown")
    
    logger.info(f":mailbox_with_mail: ===== WEBHOOK RECEIVED =====")
    logger.info(f":mailbox_with_mail: Event Type: {event_type}")
    logger.info(f":mailbox_with_mail: Ticket ID: {ticket_id}")
    logger.info(f":mailbox_with_mail: Ticket Status: {ticket_status}")
    logger.info(f":mailbox_with_mail: Trigger: Zendesk webhook")
    
    # Check if ticket was recently processed
    if bart_client.was_recently_processed(ticket_id):
        time_since = time.time() - bart_client.processed_tickets[ticket_id]
        minutes_ago = int(time_since / 60)
        logger.warning(f":warning: DUPLICATE WEBHOOK - Ticket #{ticket_id} was already processed {minutes_ago} minute(s) ago")
        logger.warning(f":no_entry: Rejecting webhook to prevent duplicate processing")
        return JSONResponse(
            status_code=200,
            content={
                "status": "rejected",
                "reason": f"Ticket already processed {minutes_ago} minute(s) ago",
                "ticket_id": ticket_id
            }
        )
    
    logger.info(f":white_check_mark: Webhook accepted, queuing for processing")
    
    background_tasks.add_task(
        webhook_handler.process_ticket_event,
        payload,
        zendesk_subdomain
    )
    
    return JSONResponse(
        status_code=200,
        content={
            "status": "accepted",
            "message": f"Ticket #{ticket_id} queued for processing"
        }
    )


@app.get("/zendesk/ticket/{ticket_id}")
async def get_zendesk_ticket(ticket_id: int, background_tasks: BackgroundTasks):
    """GET endpoint to process a Zendesk ticket by ID"""
    try:
        logger.info(f":inbox_tray: ===== GET REQUEST RECEIVED =====")
        logger.info(f":inbox_tray: Ticket ID: {ticket_id}")
        logger.info(f":inbox_tray: Trigger: HTTP GET request")
        logger.info(f":inbox_tray: Fetching Zendesk ticket #{ticket_id}")
        
        # Check if ticket was recently processed
        if bart_client.was_recently_processed(ticket_id):
            time_since = time.time() - bart_client.processed_tickets[ticket_id]
            minutes_ago = int(time_since / 60)
            logger.warning(f":warning: DUPLICATE REQUEST - Ticket #{ticket_id} was already processed {minutes_ago} minute(s) ago")
            return JSONResponse(
                status_code=200,
                content={
                    "status": "skipped",
                    "reason": f"Ticket already processed {minutes_ago} minute(s) ago",
                    "ticket_id": ticket_id,
                    "message": f"Ticket #{ticket_id} was recently processed. Wait {60 - minutes_ago} more minute(s) or restart app to reprocess."
                }
            )
        
        ticket = zendesk_client.get_ticket(ticket_id)
        
        payload = {
            "event_type": "ticket.get",
            "ticket": {
                "id": ticket.id,
                "subject": ticket.subject,
                "description": ticket.description,
                "tags": ticket.tags,
                "status": ticket.status,
                "organization_id": ticket.organization_id
            }
        }
        
        logger.info(f":white_check_mark: Fetched ticket #{ticket_id}: {ticket.subject}")
        logger.info(f":white_check_mark: GET request accepted, queuing for processing")
        
        background_tasks.add_task(
            webhook_handler.process_ticket_event,
            payload,
            zendesk_subdomain
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "accepted",
                "ticket_id": ticket_id,
                "message": f"Ticket #{ticket_id} queued for processing with Bart"
            }
        )
    except Exception as e:
        logger.error(f":x: Failed to fetch ticket #{ticket_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Ticket #{ticket_id} not found or error: {str(e)}")


@app.post("/zendesk/test")
async def test_bart_question(request: Request):
    """Test endpoint for asking Bart questions directly"""
    try:
        data = await request.json()
        subject = data.get("subject", "")
        description = data.get("description", "")
        ticket_id = data.get("ticket_id")
        tags = data.get("tags", [])
        
        if not subject and not description:
            raise HTTPException(status_code=400, detail="Missing 'subject' or 'description' field")
        
        mock_ticket = {
            "id": ticket_id or 99999,
            "subject": subject,
            "description": description,
            "tags": tags,
            "status": "open",
            "organization_id": None
        }
        
        question = webhook_handler.build_question_from_ticket(mock_ticket, zendesk_subdomain)
        logger.info(f":test_tube: Test question:\n{question}")
        
        request_id = str(uuid.uuid4())[:8]
        start_time = time.time()
        
        result = await bart_client.ask(question, ticket_id=ticket_id or 99999, request_id=request_id)
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
            
            zendesk_client.add_comment(ticket_id, formatted_response, public=False)
        
        return {
            "status": "success",
            "question": question,
            "response": response,
            "deployment_type": webhook_handler.detect_deployment_type(tags),
            "ticket_updated": ticket_id is not None,
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
