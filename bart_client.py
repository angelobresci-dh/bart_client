#!/usr/bin/env python3
"""
Zendesk Webhook Handler for Bart Integration - COMPLETE MERGED VERSION
Combines original job tracking with today's improvements:
- Block text extraction for long messages
- Regression analysis prompts
- Emoji decoding and formatting
- Increased Zendesk timeout

Requirements:
    pip install fastapi uvicorn slack-bolt slack-sdk python-dotenv zenpy aiohttp

Environment Variables:
    SLACK_BOT_TOKEN - Slack Bot User OAuth Token (xoxb-...)
    SLACK_APP_TOKEN - Slack App-Level Token for Socket Mode (xapp-...)
    SLACK_CHANNEL_ID - Channel ID OR
    SLACK_CHANNEL_NAME - Channel name
    ZENDESK_SUBDOMAIN - Your Zendesk subdomain
    ZENDESK_EMAIL - Zendesk admin email
    ZENDESK_API_TOKEN - Zendesk API token
    ZENDESK_WEBHOOK_SECRET - Optional: Webhook signature secret
    PUBLIC_URL - Optional: Public URL
    BART_TIMEOUT - Default: 600
    BART_COMPLETION_WAIT - Default: 120
    BART_FALLBACK_WAIT - Default: 540
    PROCESSING_HISTORY_TTL - Default: 3600 (1 hour)
    TEST_PROCESSING_TTL - Default: 300 (5 minutes for test endpoint)
    BART_COMMENT_CHECK_HOURS - Default: 24
"""
import asyncio
import os
import re
import hashlib
import hmac
import time
import uuid
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
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
    
    def __init__(self, bot_token: str, app_token: str, channel_id: str, zendesk_subdomain: str,
                 timeout: float = 600.0, completion_wait: float = 120.0, fallback_wait: float = 540.0):
        self.client = AsyncWebClient(token=bot_token)
        self.app = AsyncApp(token=bot_token)
        self.app_token = app_token
        self.channel_id = channel_id
        self.zendesk_subdomain = zendesk_subdomain
        self.default_timeout = timeout
        self.completion_wait = completion_wait
        self.fallback_wait = fallback_wait
        self.handler: Optional[AsyncSocketModeHandler] = None
        self.pending_responses = {}
        self.active_tickets = set()
        self.processed_tickets = {}
        self.processing_history_ttl = 3600
        self.test_processing_ttl = 300
        
        # Register message handler
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
        """Check if ticket was processed recently"""
        self._cleanup_processing_history()
        
        if custom_ttl == 0:
            return False
        
        if ticket_id not in self.processed_tickets:
            return False
        
        ttl = custom_ttl if custom_ttl is not None else self.processing_history_ttl
        time_since = time.time() - self.processed_tickets[ticket_id]
        
        return time_since < ttl
    
    def mark_as_processed(self, ticket_id: int):
        """Mark ticket as processed"""
        self.processed_tickets[ticket_id] = time.time()
        logger.info(f":memo: Marked ticket {ticket_id} as processed")
    
    async def connect(self):
        """Establish WebSocket connection"""
        self.handler = AsyncSocketModeHandler(self.app, self.app_token)
        await self.handler.connect_async()
        logger.info(":white_check_mark: Connected to Slack via WebSocket")
        
        try:
            await self.client.conversations_join(channel=self.channel_id)
            logger.info(f":white_check_mark: Joined channel {self.channel_id}")
        except Exception as e:
            logger.warning(f":warning: Could not join channel: {e}")
    
    async def disconnect(self):
        """Close WebSocket connection"""
        if self.handler:
            await self.handler.close_async()
            logger.info(":x: Disconnected from Slack")
    
    def is_working_message(self, text: str) -> bool:
        """Detect if message is a working status update"""
        final_indicators = [
            "All repos already up-to-date",
            ":white_check_mark: All repos already up-to-date"
        ]
        
        for indicator in final_indicators:
            if indicator in text:
                return False
        
        if not text or len(text.strip()) < 80:
            return True
        
        if len(text.strip()) >= 200:
            return False
        
        working_indicators = [
            r":mag:", r"\bSearching\b", r"\bLooking\b", r"\bAnalyzing\b", r":book:", r":wrench:",
            r"\bChecking\b", r"Ay caramba", r"Don't have a cow"
        ]
        
        for pattern in working_indicators:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        if text.strip().endswith("..."):
            return True
        
        return False
    
    def is_final_message(self, text: str) -> bool:
        """Detect if message is a final response"""
        final_indicators = [
            "All repos already up-to-date",
            ":white_check_mark: All repos already up-to-date",
            "Regression Analysis",
            "Regression analysis",
            "REGRESSION ANALYSIS",
            "Citations",
            "citations",
            "Response"
        ]
        
        for indicator in final_indicators:
            if indicator in text:
                return True
        
        if not text or len(text) < 50:
            return False
        
        if self.is_working_message(text):
            return False
        
        return len(text) > 50 and not text.strip().endswith("...")
    
    async def _handle_message(self, event):
        """
        UNIFIED message handler - handles new messages AND edits
        IMPROVEMENT 1: Block text extraction for long messages
        """
        log = logger
        
        subtype = event.get("subtype")
        if subtype == "message_changed":
            message = event.get("message", {})
            user_id = message.get("user")
            text = message.get("text", "")
            thread_ts = message.get("thread_ts")
            ts = message.get("ts")
            is_edit = True
            event_type_label = "EDIT"
        else:
            user_id = event.get("user")
            text = event.get("text", "")
            thread_ts = event.get("thread_ts")
            ts = event.get("ts")
            is_edit = False
            event_type_label = "NEW"
        
        if user_id == self.BART_USER_ID:
            log.info(f":speech_balloon: [{event_type_label}] Message from Bart: thread_ts={thread_ts}")
        
        if user_id == self.BART_USER_ID and thread_ts and thread_ts in self.pending_responses:
            request_id = self.pending_responses[thread_ts].get("request_id", "unknown")
            log.info(f":incoming_envelope: [{request_id}] Bart message {event_type_label}")
            
            # IMPROVEMENT 1: Extract full text from blocks for long messages
            if subtype == "message_changed":
                msg_obj = event.get("message", {})
            else:
                msg_obj = event
            
            if msg_obj.get("blocks") and len(text) < 1000:
                try:
                    block_texts = []
                    for block in msg_obj.get("blocks", []):
                        if block.get("type") == "rich_text":
                            section_texts = []
                            for element in block.get("elements", []):
                                if element.get("type") == "rich_text_section":
                                    for item in element.get("elements", []):
                                        if item.get("text"):
                                            section_texts.append(item["text"])
                                elif element.get("type") == "rich_text_list":
                                    for list_item in element.get("elements", []):
                                        for item in list_item.get("elements", []):
                                            if item.get("text"):
                                                section_texts.append(item["text"])
                            if section_texts:
                                block_texts.append("".join(section_texts))
                        elif block.get("text"):
                            if isinstance(block["text"], dict):
                                block_texts.append(block["text"].get("text", ""))
                            else:
                                block_texts.append(block["text"])
                    
                    if block_texts:
                        full_text = "\n".join(block_texts)
                        if len(full_text) > len(text):
                            log.info(f"   :mag: Extracted fuller text from blocks: {len(full_text)} chars (vs {len(text)})")
                            text = full_text
                except Exception as e:
                    log.warning(f"   :warning: Failed to extract from blocks: {e}")
            
            log.info(f"   Text preview: {text[:100]}...")
            
            is_working = self.is_working_message(text)
            is_final = self.is_final_message(text)
            log.info(f"   Classification: is_working={is_working}, is_final={is_final}, length={len(text)}")
            
            if is_edit:
                messages = self.pending_responses[thread_ts]["messages"]
                existing_idx = None
                for idx, msg in enumerate(messages):
                    if msg["ts"] == ts:
                        existing_idx = idx
                        break
                
                if existing_idx is not None:
                    messages[existing_idx] = {
                        "text": text,
                        "ts": ts,
                        "is_working": is_working,
                        "is_final": is_final
                    }
                    log.info(f"   [{request_id}] :white_check_mark: Updated message at index {existing_idx}")
                else:
                    self.pending_responses[thread_ts]["messages"].append({
                        "text": text,
                        "ts": ts,
                        "is_working": is_working,
                        "is_final": is_final
                    })
            else:
                self.pending_responses[thread_ts]["messages"].append({
                    "text": text,
                    "ts": ts,
                    "is_working": is_working,
                    "is_final": is_final
                })
                log.info(f"   [{request_id}] :white_check_mark: Message collected")
            
            self.pending_responses[thread_ts]["last_message_time"] = time.time()
            log.info(f"   [{request_id}] Total messages: {len(self.pending_responses[thread_ts]['messages'])}")
    
    async def _wait_for_completion(self, thread_ts: str, timeout: float):
        """Wait for Bart to finish responding with periodic polling to catch missed events"""
        request_id = self.pending_responses[thread_ts].get("request_id", "unknown")
        start_time = time.time()
        last_fetch_time = time.time()  # Track last poll time
        
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                logger.warning(f":alarm_clock: [{request_id}] Timeout after {elapsed:.1f}s")
                raise TimeoutError(f"Bart didn't finish within {timeout} seconds")
            
            messages = self.pending_responses[thread_ts]["messages"]
            
            # Periodically poll thread to catch messages missed by WebSocket
            if time.time() - last_fetch_time > 15:  # Poll every 15 seconds
                try:
                    logger.info(f":arrows_counterclockwise: [{request_id}] Polling thread for updates (in case WebSocket missed edits)...")
                    thread_history = await self.client.conversations_replies(
                        channel=self.channel_id,
                        ts=thread_ts,
                        limit=100
                    )
                    
                    thread_messages = thread_history.get("messages", [])[1:]  # Skip our question
                    for msg in thread_messages:
                        if msg.get("user") == self.BART_USER_ID:
                            text = msg.get("text", "")
                            ts = msg.get("ts")
                            
                            # Extract from blocks if needed
                            if msg.get("blocks") and len(text) < 1000:
                                try:
                                    block_texts = []
                                    for block in msg.get("blocks", []):
                                        if block.get("type") == "rich_text":
                                            section_texts = []
                                            for element in block.get("elements", []):
                                                if element.get("type") == "rich_text_section":
                                                    for item in element.get("elements", []):
                                                        if item.get("text"):
                                                            section_texts.append(item["text"])
                                                elif element.get("type") == "rich_text_list":
                                                    for list_item in element.get("elements", []):
                                                        for item in list_item.get("elements", []):
                                                            if item.get("text"):
                                                                section_texts.append(item["text"])
                                            if section_texts:
                                                block_texts.append("".join(section_texts))
                                        elif block.get("text"):
                                            if isinstance(block["text"], dict):
                                                block_texts.append(block["text"].get("text", ""))
                                            else:
                                                block_texts.append(block["text"])
                                    
                                    if block_texts:
                                        full_text = "\n".join(block_texts)
                                        if len(full_text) > len(text):
                                            logger.info(f":arrows_counterclockwise: [{request_id}] Extracted fuller text from blocks in polling: {len(full_text)} chars (vs {len(text)})")
                                            text = full_text
                                except Exception as e:
                                    logger.warning(f":warning: [{request_id}] Failed to extract from blocks during polling: {e}")
                            
                            # Find if this message already exists
                            existing_idx = None
                            for idx, m in enumerate(messages):
                                if m["ts"] == ts:
                                    existing_idx = idx
                                    break
                            
                            is_working = self.is_working_message(text)
                            is_final = self.is_final_message(text)
                            
                            if existing_idx is not None:
                                # Update if content changed
                                if messages[existing_idx]["text"] != text:
                                    logger.info(f":arrows_counterclockwise: [{request_id}] Updated message via polling (ts: {ts}, is_final={is_final}, length={len(text)})")
                                    messages[existing_idx] = {
                                        "text": text,
                                        "ts": ts,
                                        "is_working": is_working,
                                        "is_final": is_final
                                    }
                                    self.pending_responses[thread_ts]["last_message_time"] = time.time()
                            else:
                                # New message found via polling
                                logger.info(f":arrows_counterclockwise: [{request_id}] Found new message via polling (ts: {ts}, is_final={is_final}, length={len(text)})")
                                messages.append({
                                    "text": text,
                                    "ts": ts,
                                    "is_working": is_working,
                                    "is_final": is_final
                                })
                                self.pending_responses[thread_ts]["last_message_time"] = time.time()
                    
                    last_fetch_time = time.time()
                except Exception as e:
                    logger.warning(f":warning: [{request_id}] Failed to poll thread: {e}")
            
            if not messages:
                await asyncio.sleep(1)
                continue
            
            last_message_time = self.pending_responses[thread_ts]["last_message_time"]
            time_since_last = time.time() - last_message_time
            has_final = any(msg["is_final"] for msg in messages)
            
            if has_final and time_since_last > self.completion_wait:
                logger.info(f":white_check_mark: [{request_id}] Bart done (no messages for {time_since_last:.1f}s)")
                return
            
            if time_since_last > self.fallback_wait:
                logger.warning(f":warning: [{request_id}] No messages for {time_since_last:.1f}s")
                return
            
            await asyncio.sleep(2)
    
    def concatenate_messages(self, thread_ts: str) -> str:
        """Concatenate all final messages from Bart"""
        request_id = self.pending_responses[thread_ts].get("request_id", "unknown")
        messages = self.pending_responses[thread_ts]["messages"]
        
        final_messages = [msg for msg in messages if msg["is_final"]]
        
        if final_messages:
            final_messages.sort(key=lambda m: m["ts"])
            texts = [msg["text"] for msg in final_messages]
            logger.info(f":clipboard: [{request_id}] Concatenated {len(final_messages)} FINAL messages")
            return "\n\n".join(texts)
        
        meaningful_messages = [msg for msg in messages if not msg["is_working"]]
        if not meaningful_messages:
            meaningful_messages = messages
        
        meaningful_messages.sort(key=lambda m: m["ts"])
        texts = [msg["text"] for msg in meaningful_messages]
        logger.info(f":clipboard: [{request_id}] Concatenated {len(meaningful_messages)} messages")
        return "\n\n".join(texts)
    
    async def _post_question(self, question: str, ticket_id: int, zendesk_subdomain: str, request_id: str) -> str:
        """Post question to Slack"""
        formatted_question = f"<@{self.BART_USER_ID}> {question}"
        MAX_MESSAGE_LENGTH = 3000
        
        # Build ticket URL
        ticket_url = f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        
        if len(formatted_question) <= MAX_MESSAGE_LENGTH:
            logger.info(f":outbox_tray: [{request_id}] Posting question ({len(formatted_question)} chars)")
            
            send_result = await self.client.chat_postMessage(
                channel=self.channel_id,
                text=formatted_question
            )
            
            logger.info(f":white_check_mark: [{request_id}] Posted (ts: {send_result['ts']})")
            return send_result["ts"]
        
        else:
            logger.info(f":outbox_tray: [{request_id}] Question too long ({len(formatted_question)} chars), using file")
            
            # Include Zendesk ticket URL in the summary message
            summary = f"<@{self.BART_USER_ID}> Please review the ticket details in the text file attached to this message and provide your response. Instructions are at the end of the file. This is in reference to Zendesk ticket: {ticket_url}"
            
            file_result = await self.client.files_upload_v2(
                channel=self.channel_id,
                content=question,
                filename=f"ticket_details_{request_id}.txt",
                title=f"Ticket Details (Request {request_id})",
                initial_comment=summary
            )
            
            logger.info(f":white_check_mark: [{request_id}] File uploaded with Zendesk URL: {ticket_url}")
            
            await asyncio.sleep(0.5)
            
            # Get the most recent message (should be our file upload comment)
            history_response = await self.client.conversations_history(
                channel=self.channel_id,
                limit=5  # Get last 5 to verify we find ours
            )
            
            if not history_response.get("messages"):
                raise ValueError("No messages in history")
            
            # Find OUR message by looking for our @mention and request_id
            our_message = None
            for msg in history_response["messages"]:
                msg_text = msg.get("text", "")
                msg_ts = msg.get("ts")
                
                # Check if this message has our @mention
                if f"<@{self.BART_USER_ID}>" in msg_text:
                    # Check if it mentions our ticket_id
                    if str(ticket_id) in msg_text:
                        # Check if it's very recent (within last 5 seconds)
                        msg_time = float(msg_ts)
                        current_time = time.time()
                        age = current_time - msg_time
                        
                        if age < 5:  # Message is less than 5 seconds old
                            logger.info(f":white_check_mark: [{request_id}] Found our message: ts={msg_ts}, age={age:.2f}s")
                            our_message = msg
                            break
            
            if not our_message:
                logger.error(f":x: [{request_id}] Could not find our file upload message in recent history!")
                logger.error(f"   Checked {len(history_response['messages'])} recent messages")
                raise ValueError("File upload message not found in recent history")
            
            message_ts = str(our_message["ts"])
            logger.info(f":white_check_mark: [{request_id}] Validated timestamp: {message_ts} for ticket #{ticket_id}")
            
            return message_ts
    
    async def ask(self, question: str, ticket_id: int, request_id: str, 
                  timeout: Optional[float] = None, custom_ttl: Optional[float] = None) -> Dict[str, Any]:
        """Ask Bart a question"""
        if self.was_recently_processed(ticket_id, custom_ttl=custom_ttl):
            time_since = time.time() - self.processed_tickets[ticket_id]
            minutes_ago = int(time_since / 60)
            raise ValueError(f"Ticket {ticket_id} already processed {minutes_ago} min ago")
        
        if ticket_id in self.active_tickets:
            raise ValueError(f"Ticket {ticket_id} already being processed")
        
        self.active_tickets.add(ticket_id)
        logger.info(f":lock: [{request_id}] Locked ticket {ticket_id}")
        
        try:
            if timeout is None:
                timeout = self.default_timeout
            
            message_ts = await self._post_question(question, ticket_id, self.zendesk_subdomain, request_id)
            logger.info(f":mag: [{request_id}] Tracking thread: {message_ts}")
            
            self.pending_responses[message_ts] = {
                "messages": [],
                "last_message_time": time.time(),
                "ticket_id": ticket_id,
                "request_id": request_id
            }
            
            # Proactively check for existing messages (in case Bart responded before tracking setup)
            logger.info(f":mag: [{request_id}] Checking for existing messages in thread...")
            try:
                thread_history = await self.client.conversations_replies(
                    channel=self.channel_id,
                    ts=message_ts,
                    limit=100
                )
                
                # CRITICAL: Verify this is actually OUR thread
                all_messages = thread_history.get("messages", [])
                if all_messages:
                    first_message = all_messages[0]
                    first_message_ts = first_message.get("ts")
                    first_message_text = first_message.get("text", "")
                    
                    # Verify parent timestamp matches
                    if first_message_ts != message_ts:
                        logger.error(f":x: [{request_id}] THREAD VALIDATION FAILED!")
                        logger.error(f"   Expected parent ts={message_ts}, got ts={first_message_ts}")
                        logger.error(f"   We are tracking the WRONG thread!")
                        raise ValueError(f"Thread validation failed - parent ts mismatch")
                    
                    # Verify our @mention is present
                    if f"<@{self.BART_USER_ID}>" not in first_message_text:
                        logger.error(f":x: [{request_id}] First message missing @Bart mention!")
                        logger.error(f"   This is NOT our thread!")
                        raise ValueError(f"Thread validation failed - no @Bart mention")
                    
                    # For file uploads, verify the request_id is in the filename or title
                    if "file" in first_message_text.lower() or "attached" in first_message_text.lower():
                        # This was a file upload - verify it's OUR file
                        if request_id not in str(first_message.get("files", [])):
                            logger.warning(f":warning: [{request_id}] File upload thread but request_id not found in files")
                            logger.warning(f"   This might be a different file upload!")
                    
                    # Verify ticket ID appears in the message
                    if str(ticket_id) not in first_message_text:
                        logger.warning(f":warning: [{request_id}] Ticket #{ticket_id} not mentioned in first message")
                        logger.warning(f"   First message text: {first_message_text[:200]}")
                        logger.warning(f"   This might be for a different ticket!")
                
                existing_messages = thread_history.get("messages", [])[1:]  # Skip our question
                if existing_messages:
                    logger.info(f":mag: [{request_id}] Found {len(existing_messages)} existing message(s) in thread {message_ts}")
                    
                    # CRITICAL: Verify these messages are actually responses to OUR question
                    # They should have timestamps AFTER our message_ts, not before
                    our_timestamp = float(message_ts)
                    
                    for msg in existing_messages:
                        if msg.get("user") == self.BART_USER_ID:
                            text = msg.get("text", "")
                            ts = msg.get("ts")
                            msg_timestamp = float(ts)
                            
                            # Check if this message is BEFORE our question (impossible - indicates wrong thread!)
                            if msg_timestamp < our_timestamp:
                                logger.error(f":x: [{request_id}] MESSAGE TIMESTAMP VIOLATION!")
                                logger.error(f"   Our question ts={our_timestamp}")
                                logger.error(f"   Found message ts={msg_timestamp} (BEFORE our question!)")
                                logger.error(f"   This message cannot be a response to our question!")
                                logger.error(f"   Likely tracking WRONG thread or timestamp extraction failed!")
                                raise ValueError(f"Found message with timestamp BEFORE our question - wrong thread")
                            
                            # Check if message is suspiciously close (< 2 seconds) which might indicate wrong thread
                            time_diff = msg_timestamp - our_timestamp
                            if time_diff < 2.0:
                                logger.warning(f":warning: [{request_id}] Message appeared very quickly ({time_diff:.2f}s after question)")
                                logger.warning(f"   This is unusually fast - verifying it's actually for us...")
                            
                            # Extract from blocks if needed
                            if msg.get("blocks") and len(text) < 1000:
                                try:
                                    block_texts = []
                                    for block in msg.get("blocks", []):
                                        if block.get("type") == "rich_text":
                                            section_texts = []
                                            for element in block.get("elements", []):
                                                if element.get("type") == "rich_text_section":
                                                    for item in element.get("elements", []):
                                                        if item.get("text"):
                                                            section_texts.append(item["text"])
                                                elif element.get("type") == "rich_text_list":
                                                    for list_item in element.get("elements", []):
                                                        for item in list_item.get("elements", []):
                                                            if item.get("text"):
                                                                section_texts.append(item["text"])
                                            if section_texts:
                                                block_texts.append("".join(section_texts))
                                        elif block.get("text"):
                                            if isinstance(block["text"], dict):
                                                block_texts.append(block["text"].get("text", ""))
                                            else:
                                                block_texts.append(block["text"])
                                    
                                    if block_texts:
                                        full_text = "\n".join(block_texts)
                                        if len(full_text) > len(text):
                                            logger.info(f":mag: [{request_id}] Extracted fuller text from blocks (proactive): {len(full_text)} chars")
                                            text = full_text
                                except Exception as e:
                                    logger.warning(f":warning: [{request_id}] Failed to extract from blocks: {e}")
                            
                            is_working = self.is_working_message(text)
                            is_final = self.is_final_message(text)
                            
                            logger.info(f":mag: [{request_id}] Proactive fetch: message ts={ts}, is_final={is_final}, length={len(text)}")
                            logger.info(f":mag: [{request_id}] Message content preview: {text[:300]}...")
                            
                            self.pending_responses[message_ts]["messages"].append({
                                "text": text,
                                "ts": ts,
                                "is_working": is_working,
                                "is_final": is_final
                            })
                            self.pending_responses[message_ts]["last_message_time"] = time.time()
                else:
                    logger.info(f":mag: [{request_id}] No existing messages found (thread is new)")
            except Exception as e:
                logger.warning(f":warning: [{request_id}] Failed to check for existing messages: {e}")
            
            try:
                await self._wait_for_completion(message_ts, timeout)
                
                # Get permalink
                try:
                    permalink_response = await self.client.chat_getPermalink(
                        channel=self.channel_id,
                        message_ts=message_ts
                    )
                    thread_url = permalink_response.get("permalink", "")
                except Exception as e:
                    thread_url = ""
                
                complete_response = self.concatenate_messages(message_ts)
                
                return {
                    "response": complete_response,
                    "ticket_id": self.pending_responses[message_ts]["ticket_id"],
                    "slack_thread_url": thread_url
                }
            finally:
                self.pending_responses.pop(message_ts, None)
        finally:
            self.active_tickets.discard(ticket_id)
            self.mark_as_processed(ticket_id)
            logger.info(f":unlock: [{request_id}] Unlocked ticket {ticket_id}")


class ZendeskClient:
    """Zendesk API client"""
    
    def __init__(self, subdomain: str, email: str, token: str):
        # IMPROVEMENT 5: Increased timeout to 120s
        self.zenpy_client = Zenpy(
            subdomain=subdomain,
            email=email,
            token=token,
            timeout=120
        )
    
    def add_comment(self, ticket_id: int, comment_text: str, public: bool = True):
        """Add comment to ticket"""
        try:
            ticket = self.zenpy_client.tickets(id=ticket_id)
            ticket.comment = Comment(body=comment_text, public=public)
            self.zenpy_client.tickets.update(ticket)
            logger.info(f":white_check_mark: Added comment to ticket #{ticket_id}")
        except Exception as e:
            logger.error(f":x: Failed to update ticket #{ticket_id}: {e}")
            raise
    
    def get_ticket(self, ticket_id: int) -> Ticket:
        """Get ticket by ID"""
        return self.zenpy_client.tickets(id=ticket_id)
    
    def get_organization(self, org_id: int):
        """Get organization by ID"""
        try:
            return self.zenpy_client.organizations(id=org_id)
        except Exception as e:
            logger.warning(f":warning: Failed to get org {org_id}: {e}")
            return None
    
    def has_recent_bart_comment(self, ticket_id: int, within_hours: int = 24) -> bool:
        """Check if Bart commented recently"""
        try:
            ticket = self.zenpy_client.tickets(id=ticket_id)
            comments = self.zenpy_client.tickets.comments(ticket=ticket)
            
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=within_hours)
            bart_signature = "This response was automatically generated by Bart"
            
            for comment in comments:
                if comment.created_at:
                    # Handle both timezone-aware and naive datetimes
                    comment_time = comment.created_at if comment.created_at.tzinfo else comment.created_at.replace(tzinfo=timezone.utc)
                    if comment_time > cutoff_time:
                        if comment.body and bart_signature in comment.body:
                            return True
            
            return False
        except Exception as e:
            logger.warning(f":warning: Failed to check comments: {e}")
            return False


class ZendeskWebhookHandler:
    """Handles Zendesk webhook events"""
    
    def __init__(self, bart_client: BartClient, zendesk_client: ZendeskClient, 
                 webhook_secret: Optional[str] = None, bart_comment_check_hours: int = 24):
        self.bart = bart_client
        self.zendesk = zendesk_client
        self.webhook_secret = webhook_secret
        self.bart_comment_check_hours = bart_comment_check_hours
    
    @staticmethod
    def format_for_zendesk(text: str) -> str:
        """
        EMOJI DECODING: Clean up HTML entities and convert emoji codes
        """
        import html
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Convert emoji codes
        emoji_map = {
            ':dart:': '🎯',
            ':books:': '📚',
            ':ticket:': '🎫',
            ':studio_microphone:': '🎙️',
            ':bar_chart:': '📊',
            ':paperclip:': '📎',
            ':link:': '🔗',
            ':wrench:': '🔧',
            ':warning:': '⚠️',
            ':bulb:': '💡',
            ':white_check_mark:': '✅',
            ':x:': '❌',
            ':fire:': '🔥',
            ':robot_face:': '🤖',
            ':speech_balloon:': '💬',
            ':football:': '🏈',
            ':soccer:': '⚽',
            ':basketball:': '🏀',
            ':hammer:': '🔨',
            ':gear:': '⚙️',
            ':computer:': '💻',
            ':chart_with_upwards_trend:': '📈',
            ':calendar:': '📅',
            ':hourglass_flowing_sand:': '⏳',
            ':lock:': '🔒',
            ':unlock:': '🔓',
            ':red_circle:': '🔴',
            ':clipboard:': '📋',
            ':date:': '📅',
            ':busts_in_silhouette:': '👥',
            ':bustsinsilhouette:': '👥',
            ':zap:': '⚡',
            ':shield:': '🛡️',
            ':repeat:': '🔁',
            ':large_yellow_circle:': '🟡',
            ':large_green_circle:': '🟢',
            ':largeyellowcircle:': '🟡',
            ':largegreencircle:': '🟢',
            ':rotating_light:': '🚨',
        }
        for code, emoji in emoji_map.items():
            text = text.replace(code, emoji)
        
        # Ensure proper section breaks before regression analysis
        regression_patterns = ['REGRESSION ANALYSIS', 'Regression Analysis']
        for pattern in regression_patterns:
            text = re.sub(
                rf'([^\n])\s*\n+\**{pattern}\**',
                rf'\1\n\n---\n\n**{pattern.upper()}**',
                text
            )
        
        return text
    
    def verify_signature(self, body: bytes, signature: str) -> bool:
        """Verify webhook signature"""
        if not self.webhook_secret:
            return True
        
        computed_signature = hmac.new(
            self.webhook_secret.encode(),
            body,
            hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(computed_signature, signature)
    
    def detect_deployment_type(self, tags: list) -> str:
        """Detect deployment from tags"""
        if "on_premise" in tags:
            return "on_premise"
        return "cloud"
    
    def build_question_from_ticket(self, ticket: Dict[str, Any], zendesk_subdomain: str) -> str:
        """
        Build question with prompt engineering
        IMPROVEMENT 2: Includes regression analysis instructions
        """
        ticket_id = ticket.get("id", "unknown")
        subject = ticket.get("subject", "").strip()
        description = ticket.get("description", "").strip()
        conversation = ticket.get("conversation", "").strip()
        tags = ticket.get("tags", [])
        organization_id = ticket.get("organization_id")
        
        # Fetch organization name
        organization_name = None
        if organization_id:
            try:
                org = self.zendesk.get_organization(organization_id)
                organization_name = org.name if org else None
            except Exception as e:
                logger.warning(f":warning: Failed to fetch org: {e}")
        
        deployment_type = self.detect_deployment_type(tags)
        ticket_url = f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        
        # Build question
        question_parts = []
        if subject:
            question_parts.append(f"Subject: {subject}")
        if description:
            question_parts.append(f"*Description:*\n{description}")
        if conversation:
            question_parts.append(f"*Additional Comments / Conversation:*\n{conversation}")
        if organization_name:
            question_parts.append(f"\n*Company/Customer:* {organization_name}")
        
        question_parts.append(f"Deployment Type: {deployment_type}")
        
        # IMPROVEMENT 2: Add regression analysis instructions
        additional_instructions = f"""
---
Instructions for your response:
- Can you please provide a response based on the previous conversation?
- Exclude any follow-up questions like 'Would you like more detail?' or 'Would you like me to create a GitHub issue to track this, or would you prefer to engage with your Acryl support team directly to get a fix prioritized?' from your response. This exchange will be terminated after your first response.
- Please note that my request originated from this ticket: <{ticket_url}>. DO NOT use Zendesk ticket {ticket_id} as a reference or source in your response, because you will be self-referencing.
- The customer is using DataHub {deployment_type.replace('_', ' ').title()}. Please tailor your response accordingly.
- Finally, please add a separate and brief SECOND section to your response. In this second section, please give your analysis as to whether or not the reported issue is a product regression compared to earlier versions (If you believe the reported issue to be a product bug or failure). Provide a "Percentage likelihood that this is a product regression," along with citations of documents or artifacts that led you to this conclusion. This can include references in our documentation to the feature previously working and/or being supported, as well as previous related tickets in Zendesk and Linear indicating it was operational. Please make sure this section is clearly delineated from the first 'Main' section.
"""
        question_parts.append(additional_instructions)
        
        return "\n\n".join(question_parts)
    
    def should_process_ticket(self, payload: Dict[str, Any]) -> bool:
        """Determine if ticket should be processed"""
        ticket = payload.get("ticket", {})
        ticket_id = ticket.get("id")
        status = ticket.get("status", "").lower()
        
        if status in ["closed", "solved"]:
            logger.info(f":black_right_pointing_double_triangle_with_vertical_bar: Skipping {ticket_id} - status is {status}")
            return False
        
        tags = ticket.get("tags", [])
        if "bart-ignore" in tags:
            logger.info(f":black_right_pointing_double_triangle_with_vertical_bar: Skipping {ticket_id} - has bart-ignore")
            return False
        
        subject = ticket.get("subject", "").strip()
        description = ticket.get("description", "").strip()
        if not subject and not description:
            logger.info(f":black_right_pointing_double_triangle_with_vertical_bar: Skipping {ticket_id} - no content")
            return False
        
        if self.zendesk.has_recent_bart_comment(ticket_id, within_hours=self.bart_comment_check_hours):
            logger.info(f":black_right_pointing_double_triangle_with_vertical_bar: Skipping {ticket_id} - recent comment")
            return False
        
        return True
    
    async def process_ticket_event(self, payload: Dict[str, Any], zendesk_subdomain: str, 
                                   add_comment: bool = True, is_test_request: bool = False) -> Dict[str, Any]:
        """Process ticket event"""
        request_id = str(uuid.uuid4())[:8]
        
        ticket = payload.get("ticket", {})
        ticket_id = ticket.get("id")
        event_type = payload.get("event_type", "unknown")
        
        logger.info(f"")
        logger.info(f"{'='*80}")
        logger.info(f":inbox_tray: [{request_id}] PROCESSING TICKET #{ticket_id}")
        logger.info(f":inbox_tray: [{request_id}] Event: {event_type}")
        
        ttl_to_use = self.bart.test_processing_ttl if is_test_request else None
        
        if ticket_id in self.bart.active_tickets:
            logger.warning(f":warning: Ticket #{ticket_id} already being processed")
            return {"status": "skipped", "ticket_id": ticket_id}
        
        if not self.should_process_ticket(payload):
            return {"status": "skipped", "ticket_id": ticket_id}
        
        question = self.build_question_from_ticket(ticket, zendesk_subdomain)
        deployment_type = self.detect_deployment_type(ticket.get("tags", []))
        
        logger.info(f":thinking_face: [{request_id}] Built question for ticket #{ticket_id}")
        logger.info(f"   Ticket data used: ID={ticket.get('id')}, Subject={ticket.get('subject', '')[:60]}...")
        logger.info(f"   Question preview: {question[:200]}...")
        
        try:
            logger.info(f":robot_face: [{request_id}] Asking Bart...")
            start_time = time.time()
            
            result = await self.bart.ask(question, ticket_id=ticket_id, request_id=request_id, custom_ttl=ttl_to_use)
            bart_response = result["response"]
            response_ticket_id = result["ticket_id"]  # Get ticket_id from response
            slack_thread_url = result.get("slack_thread_url", "")
            
            # CRITICAL: Verify we got the right ticket_id back (race condition detection)
            if response_ticket_id != ticket_id:
                logger.error(f":x: [{request_id}] RACE CONDITION DETECTED!")
                logger.error(f"   Expected ticket #{ticket_id}, but response is for ticket #{response_ticket_id}")
                logger.error(f"   This indicates a thread tracking mixup - using response ticket_id to prevent misposting")
                ticket_id = response_ticket_id  # Use the correct ticket_id from response
            
            elapsed = time.time() - start_time
            logger.info(f":white_check_mark: [{request_id}] Bart responded in {elapsed:.1f}s")
            logger.info(f":white_check_mark: [{request_id}] Response is for ticket #{response_ticket_id}")
            logger.info(f":mag: [{request_id}] Response content preview: {bart_response[:300]}...")
            
            # EMOJI DECODING: Apply formatting cleanup
            cleaned_response = self.format_for_zendesk(bart_response)
            
            formatted_response = (
                f"🤖 **Bart's Response:**\n\n"
                f"{cleaned_response}\n\n"
                f"---\n"
                f"_This response was automatically generated by Bart, DataHub's code archaeology expert. "
                f"Response generated at {datetime.now(timezone.utc).isoformat()}Z_"
            )
            
            if slack_thread_url:
                formatted_response += f"\n\n💬 [View conversation in Slack]({slack_thread_url})"
            
            if add_comment:
                # Use response_ticket_id to ensure we post to the correct ticket
                logger.info(f":memo: [{request_id}] Adding comment to ticket #{response_ticket_id}")
                self.zendesk.add_comment(ticket_id=response_ticket_id, comment_text=formatted_response, public=False)
            
            logger.info(f":white_check_mark: [{request_id}] Successfully processed #{response_ticket_id}")
            logger.info(f"{'='*80}")
            
            return {
                "status": "success",
                "ticket_id": response_ticket_id,  # Return the actual ticket_id from response
                "deployment_type": deployment_type,
                "response_length": len(bart_response),
                "processing_time_seconds": elapsed,
                "response": bart_response,
                "formatted_response": formatted_response,
                "slack_thread_url": slack_thread_url,
                "comment_added": add_comment
            }
        
        except ValueError as e:
            logger.warning(f":warning: [{request_id}] {str(e)}")
            return {"status": "skipped", "ticket_id": ticket_id, "reason": str(e)}
        
        except TimeoutError as e:
            logger.error(f":alarm_clock: [{request_id}] Timeout: {e}")
            if add_comment:
                self.zendesk.add_comment(
                    ticket_id=ticket_id,
                    comment_text=":alarm_clock: Bart took too long. Please try again.",
                    public=False
                )
            return {"status": "timeout", "ticket_id": ticket_id, "error": str(e)}
        
        except Exception as e:
            logger.error(f":x: [{request_id}] Error: {e}")
            if add_comment:
                self.zendesk.add_comment(
                    ticket_id=ticket_id,
                    comment_text=f":x: Bart error: {str(e)}",
                    public=False
                )
            return {"status": "error", "ticket_id": ticket_id, "error": str(e)}


# FastAPI app
app = FastAPI(title="Bart Zendesk Webhook Handler")

# Add CORS middleware to allow requests from Zendesk apps and browsers
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (or restrict to Zendesk domains)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

# Global clients
bart_client: Optional[BartClient] = None
zendesk_client: Optional[ZendeskClient] = None
webhook_handler: Optional[ZendeskWebhookHandler] = None
zendesk_subdomain: Optional[str] = None

# JOB TRACKING for async processing
jobs: Dict[str, Dict[str, Any]] = {}
JOB_TTL = 3600


def cleanup_old_jobs():
    """Remove completed jobs older than JOB_TTL"""
    current_time = time.time()
    expired = [
        jid for jid, job in jobs.items()
        if job.get("status") in ["complete", "error", "skipped"] 
        and current_time - job.get("completed_at", current_time) > JOB_TTL
    ]
    for jid in expired:
        jobs.pop(jid, None)


def create_job(ticket_id: int, payload: Dict[str, Any]) -> str:
    """Create job and return job_id"""
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "job_id": job_id,
        "status": "processing",
        "ticket_id": ticket_id,
        "created_at": time.time(),
        "payload": payload
    }
    logger.info(f":id: Created job {job_id} for ticket #{ticket_id}")
    cleanup_old_jobs()
    return job_id


def update_job(job_id: str, status: str, result: Dict[str, Any]):
    """Update job with result"""
    if job_id in jobs:
        jobs[job_id].update({
            "status": status,
            "result": result,
            "completed_at": time.time()
        })
        logger.info(f":white_check_mark: Job {job_id} - status: {status}")


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get job"""
    return jobs.get(job_id)


async def resolve_channel_id(client: AsyncWebClient, channel_name_or_id: str) -> str:
    """Resolve channel name to ID"""
    if channel_name_or_id.startswith("C"):
        return channel_name_or_id
    
    channel_name = channel_name_or_id.lstrip("#")
    
    try:
        response = await client.conversations_list(types="public_channel,private_channel")
        for channel in response["channels"]:
            if channel["name"] == channel_name:
                return channel["id"]
        raise ValueError(f"Channel '{channel_name}' not found")
    except Exception as e:
        logger.error(f":x: Failed to resolve channel: {e}")
        raise


@app.on_event("startup")
async def startup_event():
    """Initialize clients"""
    global bart_client, zendesk_client, webhook_handler, zendesk_subdomain
    
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
    processing_history_ttl = float(os.getenv("PROCESSING_HISTORY_TTL", "3600"))
    test_processing_ttl = float(os.getenv("TEST_PROCESSING_TTL", "300"))
    bart_comment_check_hours = int(os.getenv("BART_COMMENT_CHECK_HOURS", "24"))
    
    if not all([bot_token, app_token, zendesk_subdomain, zendesk_email, zendesk_token]):
        raise ValueError("Missing required environment variables")
    
    if not channel_id and not channel_name:
        raise ValueError("Missing channel configuration")
    
    if not channel_id:
        client = AsyncWebClient(token=bot_token)
        channel_id = await resolve_channel_id(client, channel_name)
    
    bart_client = BartClient(
        bot_token=bot_token,
        app_token=app_token,
        channel_id=channel_id,
        zendesk_subdomain=zendesk_subdomain,
        timeout=bart_timeout,
        completion_wait=bart_completion_wait,
        fallback_wait=bart_fallback_wait
    )
    
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
    
    await bart_client.connect()
    
    port = int(os.getenv("PORT", "8000"))
    base_url = public_url if public_url else f"http://localhost:{port}"
    
    logger.info(":rocket: Bart Zendesk Webhook Handler - MERGED VERSION")
    logger.info("=" * 80)
    logger.info(f"   Health:     GET  {base_url}/")
    logger.info(f"   Webhook:    POST {base_url}/zendesk/webhook")
    logger.info(f"   Get Ticket: GET  {base_url}/zendesk/ticket/{{ticket_id}}")
    logger.info(f"   Test:       POST {base_url}/zendesk/test")
    logger.info(f"   Job Status: GET  {base_url}/zendesk/job/{{job_id}}")
    logger.info("")
    logger.info(f"   Features: Job tracking, Block extraction, Regression analysis, Emoji decoding")
    logger.info("=" * 80)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup"""
    if bart_client:
        await bart_client.disconnect()


@app.get("/")
async def health_check():
    """Health check"""
    return {
        "status": "healthy",
        "service": "Bart Zendesk Webhook Handler - Merged Version",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "features": ["job_tracking", "block_extraction", "regression_analysis", "emoji_decoding"]
    }


@app.get("/zendesk/job/{job_id}")
async def get_job_status(job_id: str):
    """
    Poll job status - keeps connection alive with progress updates
    
    Returns "Bart is still processing..." messages
    """
    cleanup_old_jobs()
    
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    job_status = job.get("status")
    ticket_id = job.get("ticket_id")
    created_at = job.get("created_at")
    elapsed = int(time.time() - created_at)
    
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
    
    return {"job_id": job_id, "status": "unknown"}


@app.post("/zendesk/webhook")
async def zendesk_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Zendesk webhook endpoint
    
    Supports async_mode for job tracking:
    - async_mode=false (default): Background processing
    - async_mode=true: Returns job_id, poll /zendesk/job/{job_id}
    
    Parameters:
    - add_comment: Whether to post comment to Zendesk (default: true)
    - force_reprocess: Bypass deduplication (default: false)
    - async_mode: Enable job tracking (default: false)
    """
    body = await request.body()
    signature = request.headers.get("X-Zendesk-Webhook-Signature", "")
    
    if webhook_handler.webhook_secret and not webhook_handler.verify_signature(body, signature):
        raise HTTPException(status_code=403, detail="Invalid signature")
    
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    
    add_comment = payload.get("add_comment", True)
    force_reprocess = payload.get("force_reprocess", False)
    async_mode = payload.get("async_mode", False)
    
    # Extract from Zendesk format
    ticket_detail = payload.get("detail", {})
    if not ticket_detail:
        raise HTTPException(status_code=400, detail="Missing 'detail' field")
    
    ticket_id_raw = ticket_detail.get("id")
    ticket_id = int(ticket_id_raw) if isinstance(ticket_id_raw, str) else ticket_id_raw
    
    # Check deduplication
    if not force_reprocess and bart_client.was_recently_processed(ticket_id):
        time_since = time.time() - bart_client.processed_tickets[ticket_id]
        minutes_ago = int(time_since / 60)
        return JSONResponse(status_code=200, content={
            "status": "rejected",
            "reason": f"Already processed {minutes_ago} min ago",
            "ticket_id": ticket_id
        })
    
    # Fetch fresh data
    try:
        fresh_ticket = zendesk_client.get_ticket(ticket_id)
        conversation = ticket_detail.get("conversation", "")
        
        normalized_payload = {
            "event_type": payload.get("type", "unknown").replace("zen:event-type:", ""),
            "ticket": {
                "id": fresh_ticket.id,
                "status": fresh_ticket.status,
                "subject": fresh_ticket.subject,
                "description": fresh_ticket.description,
                "conversation": conversation,
                "tags": fresh_ticket.tags,
                "organization_id": fresh_ticket.organization_id
            }
        }
    except Exception as e:
        logger.error(f":x: Failed to fetch ticket: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    # ASYNC MODE: Job tracking
    if async_mode:
        job_id = create_job(ticket_id, normalized_payload)
        
        async def process_with_job():
            try:
                result = await webhook_handler.process_ticket_event(
                    normalized_payload, zendesk_subdomain, add_comment=add_comment
                )
                status = "complete" if result["status"] == "success" else result["status"]
                update_job(job_id, status, result)
            except Exception as e:
                update_job(job_id, "error", {"error": str(e), "ticket_id": ticket_id})
        
        background_tasks.add_task(process_with_job)
        
        return JSONResponse(status_code=202, content={
            "status": "accepted",
            "job_id": job_id,
            "ticket_id": ticket_id,
            "poll_url": f"/zendesk/job/{job_id}",
            "message": f"Poll /zendesk/job/{job_id} for status"
        })
    
    # SYNC MODE: Background processing
    async def process_ticket():
        try:
            await webhook_handler.process_ticket_event(normalized_payload, zendesk_subdomain, add_comment=add_comment)
        except Exception as e:
            logger.error(f":x: Background error: {e}")
    
    background_tasks.add_task(process_ticket)
    
    return JSONResponse(status_code=200, content={"status": "accepted", "ticket_id": ticket_id})


@app.get("/zendesk/ticket/{ticket_id}")
async def get_zendesk_ticket(ticket_id: int, background_tasks: BackgroundTasks, 
                             add_comment: bool = True, force_reprocess: bool = False):
    """
    Process ticket by ID
    
    Parameters:
    - add_comment: Post comment to Zendesk (default: true)
    - force_reprocess: Bypass deduplication (default: false)
    """
    try:
        if not force_reprocess and bart_client.was_recently_processed(ticket_id):
            time_since = time.time() - bart_client.processed_tickets[ticket_id]
            minutes_ago = int(time_since / 60)
            return JSONResponse(status_code=200, content={
                "status": "skipped",
                "reason": f"Already processed {minutes_ago} min ago",
                "ticket_id": ticket_id
            })
        
        ticket = zendesk_client.get_ticket(ticket_id)
        
        logger.info(f":inbox_tray: Fetched ticket from Zendesk:")
        logger.info(f"   Requested ID: {ticket_id}")
        logger.info(f"   Fetched ID: {ticket.id}")
        logger.info(f"   Subject: {ticket.subject[:80]}")
        logger.info(f"   Status: {ticket.status}")
        
        # Verify we got the right ticket
        if ticket.id != ticket_id:
            logger.error(f":x: ZENDESK API RETURNED WRONG TICKET!")
            logger.error(f"   Requested #{ticket_id}, got #{ticket.id}")
            logger.error(f"   Using fetched ticket ID #{ticket.id} to prevent mixup")
        
        payload = {
            "event_type": "ticket.get",
            "ticket": {
                "id": ticket.id,
                "subject": ticket.subject,
                "description": ticket.description,
                "conversation": "",
                "tags": ticket.tags,
                "status": ticket.status,
                "organization_id": ticket.organization_id
            }
        }
        
        background_tasks.add_task(
            webhook_handler.process_ticket_event,
            payload,
            zendesk_subdomain,
            add_comment
        )
        
        return JSONResponse(status_code=200, content={"status": "accepted", "ticket_id": ticket_id})
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/zendesk/test")
async def test_bart_question(request: Request, background_tasks: BackgroundTasks):
    """
    Test endpoint
    
    Supports async_mode for job tracking:
    - async_mode=false (default): Synchronous, waits for response
    - async_mode=true: Returns job_id, poll /zendesk/job/{job_id}
    
    Parameters:
    - add_comment: Post to Zendesk (default: true)
    - force_reprocess: Bypass deduplication (default: false)
    - async_mode: Enable job tracking (default: false)
    """
    try:
        data = await request.json()
        
        # Handle webhook format
        if "detail" in data:
            detail = data["detail"]
            ticket_id = int(detail.get("id")) if detail.get("id") else None
            subject = detail.get("subject", "")
            description = detail.get("description", "")
            conversation = detail.get("conversation", "")
            tags = detail.get("tags", [])
        else:
            # Simple format
            subject = data.get("subject", "")
            description = data.get("description", "")
            conversation = data.get("conversation", "")
            ticket_id = data.get("ticket_id")
            tags = data.get("tags", [])
        
        add_comment = data.get("add_comment", True)
        force_reprocess = data.get("force_reprocess", False)
        async_mode = data.get("async_mode", False)
        
        if not subject and not description:
            raise HTTPException(status_code=400, detail="Missing subject or description")
        
        logger.info(f":test_tube: Test - Ticket #{ticket_id}, Subject: {subject[:50]}")
        logger.info(f":gear: Mode: {'async (polling)' if async_mode else 'sync'}")
        
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
        
        # ASYNC MODE: Job tracking
        if async_mode:
            job_id = create_job(ticket_id or 99999, data)
            
            async def process_test_with_job():
                request_id = str(uuid.uuid4())[:8]
                start_time = time.time()
                
                try:
                    custom_ttl = 0 if force_reprocess else bart_client.test_processing_ttl
                    result = await bart_client.ask(
                        question, 
                        ticket_id=ticket_id or 99999, 
                        request_id=request_id,
                        custom_ttl=custom_ttl
                    )
                    response = result["response"]
                    response_ticket_id = result["ticket_id"]  # Get ticket_id from response
                    slack_thread_url = result.get("slack_thread_url", "")
                    elapsed = time.time() - start_time
                    
                    # Verify ticket ID matches
                    requested_ticket_id = ticket_id or 99999
                    if response_ticket_id != requested_ticket_id:
                        logger.error(f":x: RACE CONDITION in test endpoint!")
                        logger.error(f"   Requested ticket #{requested_ticket_id}, got response for #{response_ticket_id}")
                    
                    # Add comment if requested (use response_ticket_id for safety)
                    comment_added = False
                    if response_ticket_id and response_ticket_id != 99999 and add_comment:
                        try:
                            cleaned = webhook_handler.format_for_zendesk(response)
                            formatted = f"🤖 **Bart's Response (Test):**\n\n{cleaned}\n\n---\n_Test response_"
                            if slack_thread_url:
                                formatted += f"\n\n💬 [View in Slack]({slack_thread_url})"
                            logger.info(f":memo: Posting test comment to ticket #{response_ticket_id}")
                            zendesk_client.add_comment(response_ticket_id, formatted, public=False)
                            comment_added = True
                        except Exception as e:
                            logger.warning(f":warning: Failed to post comment: {e}")
                    
                    update_job(job_id, "complete", {
                        "status": "success",
                        "response": response,
                        "ticket_id": response_ticket_id,  # Use actual ticket_id from response
                        "slack_thread_url": slack_thread_url,
                        "processing_time_seconds": elapsed,
                        "comment_added": comment_added
                    })
                except Exception as e:
                    logger.error(f":x: Test error: {e}")
                    update_job(job_id, "error", {"error": str(e)})
            
            background_tasks.add_task(process_test_with_job)
            
            return JSONResponse(status_code=202, content={
                "status": "accepted",
                "job_id": job_id,
                "ticket_id": ticket_id or 99999,
                "poll_url": f"/zendesk/job/{job_id}",
                "message": f"Poll /zendesk/job/{job_id} every 10-30 seconds for status"
            })
        
        # SYNC MODE: Wait for response (may timeout)
        logger.info(f":test_tube: Synchronous mode - waiting for Bart...")
        
        request_id = str(uuid.uuid4())[:8]
        start_time = time.time()
        
        custom_ttl = 0 if force_reprocess else bart_client.test_processing_ttl
        
        result = await bart_client.ask(
            question,
            ticket_id=ticket_id or 99999,
            request_id=request_id,
            custom_ttl=custom_ttl
        )
        response = result["response"]
        response_ticket_id = result["ticket_id"]  # Get ticket_id from response
        slack_thread_url = result.get("slack_thread_url", "")
        elapsed = time.time() - start_time
        
        # Verify ticket ID matches
        requested_ticket_id = ticket_id or 99999
        if response_ticket_id != requested_ticket_id:
            logger.error(f":x: RACE CONDITION in sync test endpoint!")
            logger.error(f"   Requested ticket #{requested_ticket_id}, got response for #{response_ticket_id}")
        
        # Add comment if requested (use response_ticket_id for safety)
        comment_added = False
        zendesk_error = None
        if response_ticket_id and response_ticket_id != 99999 and add_comment:
            try:
                cleaned = webhook_handler.format_for_zendesk(response)
                formatted = f"🤖 **Bart's Response (Test):**\n\n{cleaned}\n\n---\n_Test response_"
                if slack_thread_url:
                    formatted += f"\n\n💬 [View in Slack]({slack_thread_url})"
                logger.info(f":memo: Posting test comment to ticket #{response_ticket_id}")
                zendesk_client.add_comment(response_ticket_id, formatted, public=False)
                comment_added = True
            except Exception as e:
                zendesk_error = str(e)
                logger.warning(f":warning: Zendesk error: {e}")
        
        return {
            "status": "success",
            "response": response,
            "ticket_id": response_ticket_id,  # Return actual ticket_id from response
            "slack_thread_url": slack_thread_url,
            "processing_time_seconds": elapsed,
            "ticket_updated": comment_added,
            "comment_added": comment_added,
            "zendesk_error": zendesk_error,
            "note": "Use async_mode=true to avoid timeouts and enable polling"
        }
    
    except Exception as e:
        logger.error(f":x: Test error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level="info"
    )
