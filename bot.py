"""
================================================================================
  PIPECAT AI — Real-Time Outbound Voice Sales Agent (Twilio Direct Dial)
  + Call Logging (transcripts & outcomes saved to calls.db)
  Framework: pipecat-ai

  ARCHITECTURE OVERVIEW:
  ─────────────────────────────────────────────────────────────────────────────

  dial.py / POST /call
       │  (compliance pre-checks: E.164, opt-out, calling window)
       ▼
  [Twilio REST API]  ──►  Dials the prospect's phone number
       │
       ▼  (prospect picks up)
  [Twilio Media Stream WebSocket]  ──►  Streams μ-law audio to /stream
       │
       ▼
  [TwilioTransport]  ──►  Decodes to PCM AudioRawFrames
       │
       ▼
  [Deepgram STT]  ──►  AudioRawFrame → TranscriptionFrame
       │
       ▼
  [TranscriptLogger(user)]  ──►  logs user speech to DB + records turn timestamp
       │
       ▼
  [OptOutGuardrail]  ──►  detects opt-out phrases → records immediately, schedules end
       │
       ▼
  [ContextCompactor]  ──►  trims LLM context to last N turn pairs (keeps system prompt)
       │
       ▼
  [OpenAI LLM]  ──►  TranscriptionFrame → TextFrame / FunctionCallFrame
       │
       ▼
  [TranscriptLogger(assistant)]  ──►  logs assistant speech + emits turn latency metric
       │
       ▼
  [Cartesia TTS]  ──►  TextFrame → AudioRawFrame (synthesized speech)
       │
       ▼
  [TwilioTransport]  ──►  PCM → μ-law → Twilio WS → prospect's phone

================================================================================
"""

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo

import requests as _requests
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, Request, Depends, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from twilio.rest import Client as TwilioClient
from twilio.request_validator import RequestValidator
import phonenumbers

# ── Pipecat core ──────────────────────────────────────────────────────────────
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineTask, PipelineParams
from pipecat.pipeline.runner import PipelineRunner

# ── Frames ────────────────────────────────────────────────────────────────────
from pipecat.frames.frames import (
    LLMMessagesFrame,
    EndFrame,
    TranscriptionFrame,
    TextFrame,
    InputAudioRawFrame,
    OutputAudioRawFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
)

# ── Twilio Transport ──────────────────────────────────────────────────────────
from pipecat.transports.websocket.fastapi import FastAPIWebsocketTransport, FastAPIWebsocketParams
from pipecat.serializers.twilio import TwilioFrameSerializer
from browser_serializer import BrowserFrameSerializer

# ── STT ───────────────────────────────────────────────────────────────────────
from pipecat.services.deepgram.stt import DeepgramSTTService

# ── LLM ───────────────────────────────────────────────────────────────────────
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair

# ── TTS ───────────────────────────────────────────────────────────────────────
from pipecat.services.cartesia.tts import CartesiaTTSService, GenerationConfig

# ── VAD ───────────────────────────────────────────────────────────────────────
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams

# ── Processors ────────────────────────────────────────────────────────────────
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.services.cartesia.tts import CartesiaTTSSettings
from pipecat.transcriptions.language import Language

# ── Call Logger (local SQLite) ────────────────────────────────────────────────
from logger_db import (
    init_db, start_call, log_turn, end_call,
    get_call_summary, list_calls,
    is_opted_out, record_opt_out, get_call_number,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("VoiceSalesAgent")

init_db()


# ==============================================================================
#  COMPLIANCE CONSTANTS
#  ACMA telemarketing rules — https://www.acma.gov.au/say-no-to-telemarketers
# ==============================================================================

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

# National public holidays 2026 — update annually or integrate an API for state holidays.
# Current list covers nation-wide holidays only; add your state's additional dates.
_AU_PUBLIC_HOLIDAYS = {
    date(2026, 1, 1),    # New Year's Day
    date(2026, 1, 26),   # Australia Day
    date(2026, 4, 3),    # Good Friday
    date(2026, 4, 4),    # Easter Saturday
    date(2026, 4, 5),    # Easter Sunday
    date(2026, 4, 6),    # Easter Monday
    date(2026, 4, 25),   # Anzac Day
    date(2026, 6, 8),    # King's Birthday (most states — check your state)
    date(2026, 12, 25),  # Christmas Day
    date(2026, 12, 26),  # Boxing Day
}

# Phrases that constitute an opt-out request (used by OptOutGuardrail).
# Deliberately specific to avoid false positives from phrases like "not interested in that plan".
OPT_OUT_PHRASES = [
    "stop calling",
    "don't call me",
    "do not call",
    "remove me",
    "take me off",
    "opt me out",
    "this is harassment",
    "add me to your do not call",
    "never call again",
    "not interested and don't call",
    "not interested and do not call",
    "never contact me",
]


# ==============================================================================
#  HELPERS
# ==============================================================================

def normalize_e164(number: str, default_region: str = "AU") -> str | None:
    """Return E.164-formatted number or None if invalid."""
    try:
        parsed = phonenumbers.parse(number, default_region)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    return None


def _is_calling_allowed() -> tuple[bool, str]:
    """
    ACMA telemarketing calling windows (Australia/Sydney timezone):
      Mon–Fri  09:00–20:00
      Saturday 09:00–17:00
      Sunday   — never
      Public holidays — never

    Returns (allowed: bool, reason: str).
    Reason is 'ok', 'sunday', 'public_holiday', or 'outside_hours'.
    """
    now = datetime.now(SYDNEY_TZ)
    today = now.date()
    weekday = now.weekday()   # 0=Mon … 6=Sun
    hour = now.hour + now.minute / 60.0

    if today in _AU_PUBLIC_HOLIDAYS:
        return False, "public_holiday"
    if weekday == 6:
        return False, "sunday"
    if weekday == 5:   # Saturday
        return (True, "ok") if 9.0 <= hour < 17.0 else (False, "outside_hours")
    # Monday – Friday
    return (True, "ok") if 9.0 <= hour < 20.0 else (False, "outside_hours")


# ==============================================================================
#  GREETING — pre-cached at startup for instant browser playback
# ==============================================================================
GREETING_TEXT = (
    "Hey, this is Alex from Tuvi Solutions — I'm an AI assistant. "
    "Super quick one: is now an okay moment?"
)

# Filled by _prewarm_services() at startup; None = fall back to LLM-generated greeting
_cached_greeting_audio: bytes | None = None


# ==============================================================================
#  SYSTEM PROMPT — AI disclosure required by ACMA (§15.4 of architecture guide)
# ==============================================================================
SYSTEM_PROMPT = f"""
You are Alex, an AI sales assistant calling on behalf of Tuvi Solutions.
Tuvi Solutions is a tech agency specialising in Web Design, AI/ML Development, and Custom App Development.

IDENTITY — MANDATORY:
You must disclose you are an AI at the start of every call and whenever the prospect asks.
Never claim to be human. If asked "are you a robot?", confirm you are an AI.

YOUR GOAL:
Qualify the prospect's interest, generate curiosity about Tuvi Solutions, and book a short 10-minute discovery call.

VOICE RULES (follow strictly):
- Keep every response under 2 sentences. Never monologue.
- Ask ONE question per turn. Wait for the answer before continuing.
- Use a warm, natural, conversational tone — like a friendly colleague, not a script-reader.
- Do not use bullet points, numbered lists, markdown, or long URLs — this is spoken audio.
- Use natural filler transitions: "So,", "Right,", "Yeah,", "Look,", "Honestly," — sparingly.
- Use contractions always: "I'm" not "I am", "we've" not "we have", "don't" not "do not".
- Vary sentence length — mix short punchy sentences with slightly longer ones.
- If interrupted, acknowledge briefly ("Got it." / "Yep, no worries." / "Sure, sure.") then adapt.
- Pause naturally before asking a question — end statements before questions with a comma pause signal, e.g. "...so I wanted to ask — are you currently happy with your website?"
- Never sound like you're reading. Sound like you're thinking as you speak.

CALL FLOW:
1. Identify yourself as an AI assistant from Tuvi Solutions. State briefly why you are calling.
2. Ask one relevance question to check if the topic is worth their time.
3. If relevant — qualify their pain in one or two short questions.
4. Offer a one-sentence value proposition matched to their situation.
5. Propose a 10-minute discovery call. Offer 2–3 time slots.
6. Confirm name, email, and chosen slot. Book via book_appointment tool.
7. End politely.

OBJECTION HANDLING:
- "Who is this?" → "I'm Alex, an AI assistant from Tuvi Solutions. [one-sentence reason for call]."
- "Are you a robot?" → "Yes, I'm an AI. Happy to help or take you off the list — your call."
- "I'm busy." → "No problem. Want me to send the details by text instead?"
- "Not interested." → Acknowledge and end the call. Do not push further.
- "How much?" → "It depends on your scope — I can send a quick summary by text, or we can cover it in 10 minutes."
- "Remove me." → Use mark_do_not_call tool immediately, then say one polite goodbye line, then use end_call.

HARD-STOP — use mark_do_not_call immediately if the prospect says any of:
"stop calling", "remove me", "do not call", "this is harassment", or similar opt-out language.
After mark_do_not_call, say: "Of course, I'll make sure you're not called again. Sorry for the interruption."
Then use end_call. Do not attempt any more sales conversation after an opt-out.

Start the call with (say it naturally, not robotically):
"{GREETING_TEXT}"

Wait for their response before continuing. If they say yes, then explain why you're calling in one sentence.
"""


# ==============================================================================
#  TOOLS  (architecture guide §9.5)
# ==============================================================================
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "check_calendar_availability",
            "description": "Check available 10-minute discovery call slots for the next 3 business days.",
            "parameters": {
                "type": "object",
                "properties": {
                    "preferred_time": {
                        "type": "string",
                        "description": "Prospect's preferred time window, e.g. 'tomorrow afternoon'. Optional.",
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "book_appointment",
            "description": (
                "Book a confirmed 10-minute discovery call after the prospect explicitly agrees to a slot. "
                "Only call this tool after the prospect has said yes to a specific time."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "slot":           {"type": "string", "description": "ISO-8601 datetime, e.g. '2026-06-25T14:00:00'"},
                    "prospect_name":  {"type": "string"},
                    "prospect_email": {"type": "string"},
                },
                "required": ["slot", "prospect_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "mark_do_not_call",
            "description": (
                "Record an opt-out / do-not-call request immediately and durably. "
                "Call this as soon as the prospect says stop calling, remove me, or similar. "
                "This must be called before end_call when an opt-out is detected."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "send_followup_sms",
            "description": "Send a short follow-up SMS to the prospect (max 160 chars) after they give consent or ask for details.",
            "parameters": {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "The SMS text. Keep it under 160 characters. Plain text only.",
                    }
                },
                "required": ["message"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "transfer_to_human",
            "description": "Warm-transfer the call to a human team member if the prospect explicitly asks for a person or is clearly a high-value lead.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "end_call",
            "description": "End the call gracefully. Only call this after saying a polite goodbye.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]


# ==============================================================================
#  TRANSCRIPT LOGGER
# ==============================================================================
class TranscriptLogger(FrameProcessor):
    """
    Transparent processor that logs user/assistant turns to the DB and optionally
    forwards transcript events as JSON over a browser WebSocket.
    """

    def __init__(
        self,
        call_id: int,
        role_filter: str,
        timing_state: dict | None = None,
        event_ws: WebSocket | None = None,
    ):
        super().__init__()
        self._call_id = call_id
        self._role = role_filter
        self._timing = timing_state
        self._event_ws = event_ws

    async def _send_event(self, data: dict):
        if self._event_ws is None:
            return
        try:
            await self._event_ws.send_text(json.dumps(data))
        except Exception:
            pass

    async def process_frame(self, frame: object, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if self._role == "user" and isinstance(frame, TranscriptionFrame):
            text = frame.text.strip()
            if text:
                log_turn(self._call_id, "user", text)
                logger.info(f"[USER]      {text}")
                if self._timing is not None:
                    self._timing["user_turn_ts"] = time.time()
                    self._timing["first_text_pending"] = True
                await self._send_event({"event": "transcript", "role": "user", "text": text})

        elif self._role == "assistant" and isinstance(frame, TextFrame):
            text = frame.text.strip()
            if text:
                log_turn(self._call_id, "assistant", text)
                logger.info(f"[ASSISTANT] {text}")
                if (
                    self._timing is not None
                    and self._timing.get("first_text_pending")
                    and self._timing.get("user_turn_ts")
                ):
                    latency_ms = round((time.time() - self._timing["user_turn_ts"]) * 1000)
                    logger.info(f"[LATENCY] turn_end→first_text: {latency_ms}ms")
                    self._timing["first_text_pending"] = False
                await self._send_event({"event": "transcript", "role": "assistant", "text": text})

        await self.push_frame(frame, direction)


# ==============================================================================
#  STATUS EVENT SENDER  (browser sessions only)
#  Observes VAD / TTS speaking frames and forwards status events as JSON to
#  the browser WebSocket. Place one instance before STT and one after TTS so
#  both user- and bot-speaking events are captured.
# ==============================================================================
class StatusEventSender(FrameProcessor):
    """Forwards user/bot speaking state as JSON text frames over a WebSocket."""

    def __init__(self, ws: WebSocket):
        super().__init__()
        self._ws = ws

    async def _send(self, data: dict):
        try:
            await self._ws.send_text(json.dumps(data))
        except Exception:
            pass

    async def process_frame(self, frame: object, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, UserStartedSpeakingFrame):
            await self._send({"event": "status", "state": "user_speaking"})
        elif isinstance(frame, UserStoppedSpeakingFrame):
            await self._send({"event": "status", "state": "thinking"})
        elif isinstance(frame, BotStartedSpeakingFrame):
            await self._send({"event": "status", "state": "bot_speaking"})
        elif isinstance(frame, BotStoppedSpeakingFrame):
            await self._send({"event": "status", "state": "listening"})

        await self.push_frame(frame, direction)


# ==============================================================================
#  OPT-OUT GUARDRAIL
#  Safety net: records opt-out in DB immediately even if LLM misses it.
#  Does NOT block the frame — lets LLM generate the polite goodbye.
#  Schedules a forced call end (12 s) so the call ends even if end_call tool
#  is never called.
# ==============================================================================
class OptOutGuardrail(FrameProcessor):

    def __init__(
        self,
        call_db_id: int,
        to_number_ref: list[str],
        outcome_ref: list[str],
        task_ref: list,
    ):
        super().__init__()
        self._call_db_id = call_db_id
        self._to_number_ref = to_number_ref
        self._outcome_ref = outcome_ref
        self._task_ref = task_ref
        self._triggered = False

    async def process_frame(self, frame: object, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if not self._triggered and isinstance(frame, TranscriptionFrame):
            text = frame.text.lower()
            if any(phrase in text for phrase in OPT_OUT_PHRASES):
                self._triggered = True
                self._outcome_ref[0] = "opted_out"
                phone = self._to_number_ref[0]
                record_opt_out(phone, self._call_db_id)
                logger.warning(f"Opt-out detected: '{frame.text}' — recorded for {phone}")
                task = self._task_ref[0]
                if task:
                    # Allow LLM/TTS to say goodbye, then force-end
                    asyncio.create_task(_delayed_end(task, delay=12.0))

        await self.push_frame(frame, direction)


# ==============================================================================
#  CONTEXT COMPACTOR
#  Sits after OptOutGuardrail, before user_aggregator.
#  At the start of each user turn, trims the LLM context to the last max_pairs
#  user/assistant turn pairs. System prompt is always preserved.
#  This keeps the hot-path prompt small and prevents cost/latency growth on long calls.
#  (Architecture guide §7.4)
# ==============================================================================
class ContextCompactor(FrameProcessor):

    def __init__(self, context: LLMContext, max_pairs: int = 5):
        super().__init__()
        self._context = context
        self._max_pairs = max_pairs

    async def process_frame(self, frame: object, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, TranscriptionFrame) and frame.text.strip():
            self._trim()

        await self.push_frame(frame, direction)

    def _trim(self):
        messages = self._context.messages
        system_msgs = [m for m in messages if m["role"] == "system"]
        other_msgs  = [m for m in messages if m["role"] != "system"]
        max_keep = self._max_pairs * 2
        if len(other_msgs) > max_keep:
            dropped = len(other_msgs) - max_keep
            self._context.messages = system_msgs + other_msgs[-max_keep:]
            logger.info(f"[CONTEXT] Compacted: dropped {dropped} msgs, kept last {self._max_pairs} turn pairs")


# ==============================================================================
#  TWILIO SIGNATURE VALIDATION  (architecture guide §16.1)
#  Skip in development (ENVIRONMENT=development) since ngrok/tunnels break HMAC.
#  Set ENVIRONMENT=production to enable.
# ==============================================================================
async def _require_twilio_signature(request: Request):
    env = os.environ.get("ENVIRONMENT", "development").lower()
    if env == "development":
        return

    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    validator = RequestValidator(auth_token)

    public_base = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
    path = request.url.path
    if request.url.query:
        path += f"?{request.url.query}"
    url = f"{public_base}{path}"

    signature = request.headers.get("X-Twilio-Signature", "")
    form_data = await request.form()
    params = dict(form_data)

    if not validator.validate(url, params, signature):
        logger.warning(f"Invalid Twilio signature rejected: {url}")
        raise HTTPException(status_code=403, detail="Invalid Twilio signature")


# ==============================================================================
#  STARTUP PRE-WARMING
#  Eliminates the two biggest sources of connect latency:
#    1. SileroVADAnalyzer ONNX model load  →  2–4 s on cold start
#    2. LLM + TTS greeting generation      →  1–3 s per call
# ==============================================================================
async def _prewarm_services():
    global _cached_greeting_audio

    # 1. Pre-load Silero ONNX model so first-call instantiation is fast
    try:
        _warmup = SileroVADAnalyzer(params=VADParams(stop_secs=0.4))
        del _warmup
        logger.info("[PREWARM] Silero VAD model loaded")
    except Exception as e:
        logger.warning(f"[PREWARM] Silero pre-warm failed: {e}")

    # 2. Pre-generate the opening greeting via Cartesia REST so browser sessions
    #    can play it instantly without waiting for LLM + TTS on connect.
    cartesia_key = os.environ.get("CARTESIA_API_KEY", "")
    voice_id = os.environ.get("CARTESIA_VOICE_ID", "")
    if not (cartesia_key and voice_id):
        logger.warning("[PREWARM] Skipping greeting cache — CARTESIA_API_KEY or CARTESIA_VOICE_ID not set")
        return

    def _fetch():
        resp = _requests.post(
            "https://api.cartesia.ai/tts/bytes",
            headers={
                "Cartesia-Version": "2025-04-16",
                "X-API-Key": cartesia_key,
                "Content-Type": "application/json",
            },
            json={
                "model_id": "sonic-3.5",
                "transcript": GREETING_TEXT,
                "voice": {"mode": "id", "id": voice_id},
                "output_format": {"container": "raw", "encoding": "pcm_s16le", "sample_rate": 16000},
                "language": "en",
                "generation_config": {"speed": 1.05, "emotion": "positivity:medium"},
            },
            timeout=20,
        )
        resp.raise_for_status()
        return resp.content

    try:
        _cached_greeting_audio = await asyncio.to_thread(_fetch)
        duration_s = len(_cached_greeting_audio) / 32000  # PCM16 @ 16kHz mono = 32000 bytes/s
        logger.info(f"[PREWARM] Greeting cached: {len(_cached_greeting_audio):,} bytes ({duration_s:.1f}s)")
    except Exception as e:
        logger.warning(f"[PREWARM] Greeting cache failed — browser sessions will use live LLM+TTS: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_prewarm_services())
    yield


# ==============================================================================
#  SHARED TOOL DISPATCHER
#  Called by both the Twilio and browser pipeline tool handlers.
# ==============================================================================
async def _dispatch_tool(
    *,
    function_name: str,
    arguments: dict,
    call_db_id: int,
    to_number_ref: list[str],
    outcome_state: list[str],
    task_ref: list,
    booking_state: dict,
    call_sid: str = "unknown",
    is_browser: bool = False,
) -> dict:
    logger.info(f"Tool: {function_name}  args={arguments}  browser={is_browser}")

    if function_name == "check_calendar_availability":
        return {"status": "success", "available_slots": _mock_available_slots()}

    if function_name == "book_appointment":
        slot  = arguments.get("slot", "TBD")
        name  = arguments.get("prospect_name", "Prospect")
        email = arguments.get("prospect_email", "")
        booking_state.update({"slot": slot, "prospect_name": name, "prospect_email": email})
        outcome_state[0] = "booked"
        result = {
            "status": "booked",
            "confirmed_slot": slot,
            "prospect_name": name,
            "calendar_link": f"https://cal.tuvisolutions.com/demo/{slot.replace(':', '-')}",
            "message": f"Discovery call booked for {name} at {slot}.",
        }
        logger.info(f"Booking confirmed: {result}")
        return result

    if function_name == "mark_do_not_call":
        phone = to_number_ref[0]
        record_opt_out(phone, call_db_id)
        outcome_state[0] = "opted_out"
        logger.warning(f"mark_do_not_call tool called for {phone}")
        return {"status": "recorded", "message": "Number added to internal do-not-call list."}

    if function_name == "send_followup_sms":
        if is_browser:
            return {"status": "unavailable", "message": "SMS is not available in browser sessions."}
        message = arguments.get("message", "Thank you for your time.")
        phone = to_number_ref[0]
        if phone and phone != "unknown":
            try:
                tc = TwilioClient(os.environ["TWILIO_ACCOUNT_SID"], os.environ["TWILIO_AUTH_TOKEN"])
                tc.messages.create(
                    to=phone,
                    from_=os.environ["TWILIO_PHONE_NUMBER"],
                    body=message,
                )
                logger.info(f"SMS sent to {phone}")
                return {"status": "sent"}
            except Exception as e:
                logger.error(f"SMS failed: {e}")
                return {"status": "error", "message": str(e)}
        return {"status": "error", "message": "Prospect phone number not available."}

    if function_name == "transfer_to_human":
        if is_browser:
            outcome_state[0] = "transferred"
            if task_ref[0]:
                asyncio.create_task(_delayed_end(task_ref[0], delay=3.0))
            return {
                "status": "info",
                "message": "A team member will follow up with you shortly. Ending this session.",
            }
        human_number = os.environ.get("HUMAN_AGENT_NUMBER", "")
        if human_number and call_sid != "unknown":
            try:
                public_url = os.environ["PUBLIC_BASE_URL"].rstrip("/")
                tc = TwilioClient(os.environ["TWILIO_ACCOUNT_SID"], os.environ["TWILIO_AUTH_TOKEN"])
                tc.calls(call_sid).update(
                    url=f"{public_url}/transfer-twiml?to={human_number}",
                    method="POST",
                )
                outcome_state[0] = "transferred"
                if task_ref[0]:
                    asyncio.create_task(_delayed_end(task_ref[0], delay=2.0))
                logger.info(f"Warm transfer initiated to {human_number}")
                return {"status": "transferring"}
            except Exception as e:
                logger.error(f"Transfer failed: {e}")
                return {"status": "error", "message": str(e)}
        return {"status": "error", "message": "Human transfer not configured. Set HUMAN_AGENT_NUMBER in .env."}

    if function_name == "end_call":
        if outcome_state[0] == "unknown":
            outcome_state[0] = "not_interested"
        if task_ref[0]:
            asyncio.create_task(_delayed_end(task_ref[0]))
        return {"status": "ending"}

    return {"status": "error", "message": f"Unknown tool: {function_name}"}


# ==============================================================================
#  FASTAPI APP
# ==============================================================================
app = FastAPI(title="Voice Sales Agent", lifespan=lifespan)

_STATIC_DIR = Path(__file__).parent / "static"
if _STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

_active_tasks: dict[str, PipelineTask] = {}
_call_db_ids: dict[str, int] = {}
_paused_campaigns: set[str] = set()   # in-memory; use Redis for multi-worker


# ── Health / readiness ─────────────────────────────────────────────────────────

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    """Readiness check — verifies required env vars are present."""
    required = ("TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_PHONE_NUMBER",
                "DEEPGRAM_API_KEY", "OPENAI_API_KEY", "CARTESIA_API_KEY",
                "CARTESIA_VOICE_ID", "PUBLIC_BASE_URL")
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        return JSONResponse({"status": "not_ready", "missing": missing}, status_code=503)
    return {"status": "ready"}


# ── Call log REST endpoints ────────────────────────────────────────────────────

@app.get("/calls")
async def get_calls(limit: int = 20):
    return JSONResponse(list_calls(limit))


@app.get("/calls/{call_id}")
async def get_call(call_id: int):
    summary = get_call_summary(call_id)
    if not summary:
        return JSONResponse({"error": "not found"}, status_code=404)
    return JSONResponse(summary)


# ── Admin endpoints ────────────────────────────────────────────────────────────

@app.post("/admin/call/{call_sid}/hangup")
async def admin_hangup(call_sid: str):
    """Manually terminate an active call pipeline."""
    task = _active_tasks.get(call_sid)
    if not task:
        return JSONResponse({"error": "call not found or already ended"}, status_code=404)
    await task.queue_frames([EndFrame()])
    logger.warning(f"Admin hangup triggered for {call_sid}")
    return {"status": "hangup_queued", "call_sid": call_sid}


@app.post("/admin/campaign/{campaign_id}/pause")
async def admin_pause_campaign(campaign_id: str):
    _paused_campaigns.add(campaign_id)
    logger.warning(f"Campaign {campaign_id} paused")
    return {"status": "paused", "campaign_id": campaign_id}


@app.post("/admin/campaign/{campaign_id}/resume")
async def admin_resume_campaign(campaign_id: str):
    _paused_campaigns.discard(campaign_id)
    return {"status": "resumed", "campaign_id": campaign_id}


# ── Outbound call trigger ──────────────────────────────────────────────────────

@app.post("/call")
async def initiate_call(request: Request):
    """
    Trigger an outbound call with compliance pre-checks.

    Body: {"to": "+61412345678", "campaign_id": "camp_01"}

    Pre-checks (in order):
      1. Campaign pause guard
      2. E.164 normalisation
      3. Internal opt-out list
      4. ACMA calling time window
    """
    body = await request.json()
    to_raw = body.get("to", "")
    campaign_id = body.get("campaign_id", "default")

    # 1. Campaign pause guard
    if campaign_id in _paused_campaigns:
        return JSONResponse({"status": "blocked", "reason": "campaign_paused"}, status_code=409)

    # 2. E.164 normalisation (defaults to AU region)
    to_number = normalize_e164(to_raw)
    if not to_number:
        return JSONResponse(
            {"status": "blocked", "reason": "invalid_number", "raw": to_raw},
            status_code=400,
        )

    # 3. Internal opt-out check
    if is_opted_out(to_number):
        logger.info(f"Blocked (opt-out): {to_number}")
        return JSONResponse({"status": "blocked", "reason": "internal_opt_out"})

    # 4. ACMA calling time window (bypassed in development via skip_compliance flag)
    skip_compliance = (
        body.get("skip_compliance", False)
        and os.environ.get("ENVIRONMENT", "development").lower() == "development"
    )
    allowed, reason = _is_calling_allowed()
    if not allowed and not skip_compliance:
        logger.info(f"Blocked (calling window): {reason} for {to_number}")
        return JSONResponse({
            "status": "queued",
            "reason": reason,
            "message": f"Outside ACMA calling window ({reason}). Schedule for next valid window.",
        })

    public_url = os.environ["PUBLIC_BASE_URL"].rstrip("/")
    ws_host = public_url.replace("https://", "").replace("http://", "")

    twilio_client = TwilioClient(
        os.environ["TWILIO_ACCOUNT_SID"],
        os.environ["TWILIO_AUTH_TOKEN"],
    )

    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://{ws_host}/stream">
      <Parameter name="campaign_id" value="{campaign_id}"/>
    </Stream>
  </Connect>
</Response>"""

    call = twilio_client.calls.create(
        to=to_number,
        from_=os.environ["TWILIO_PHONE_NUMBER"],
        twiml=twiml,
        status_callback=f"{public_url}/twilio/status",
        status_callback_event=["initiated", "ringing", "answered", "completed"],
        status_callback_method="POST",
    )

    call_db_id = start_call(call.sid, to_number)
    _call_db_ids[call.sid] = call_db_id

    logger.info(f"Outbound call → {to_number}  SID={call.sid}  DB id={call_db_id}")
    return {
        "status": "calling",
        "to": to_number,
        "call_sid": call.sid,
        "log_id": call_db_id,
    }


# ── TwiML webhook (inbound / fallback) ────────────────────────────────────────

@app.post("/twiml")
async def twiml_webhook(
    request: Request,
    _: None = Depends(_require_twilio_signature),
):
    public_url = os.environ["PUBLIC_BASE_URL"].rstrip("/")
    ws_host = public_url.replace("https://", "").replace("http://", "")
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://{ws_host}/stream"/>
  </Connect>
</Response>"""
    return PlainTextResponse(content=twiml, media_type="application/xml")


# ── Twilio call status callbacks ───────────────────────────────────────────────

@app.post("/twilio/status")
async def twilio_status(
    request: Request,
    _: None = Depends(_require_twilio_signature),
):
    """
    Receives Twilio call status callbacks.
    Only records unanswered-call outcomes (no-answer, busy, failed).
    Connected-call outcomes are handled by the WebSocket pipeline.
    """
    form = await request.form()
    call_sid = form.get("CallSid", "")
    call_status = form.get("CallStatus", "")
    logger.info(f"Twilio status callback: SID={call_sid}  status={call_status}")

    if call_status in ("no-answer", "busy", "failed", "canceled"):
        db_id = _call_db_ids.get(call_sid)
        if db_id is not None:
            end_call(db_id, call_status, duration_s=None)
            _call_db_ids.pop(call_sid, None)

    return PlainTextResponse("", media_type="text/plain")


# ── Warm transfer TwiML ────────────────────────────────────────────────────────

@app.post("/transfer-twiml")
async def transfer_twiml(request: Request):
    """Returns TwiML that dials the human agent after a warm transfer."""
    to = request.query_params.get("to", os.environ.get("HUMAN_AGENT_NUMBER", ""))
    if not to:
        return PlainTextResponse(
            '<?xml version="1.0" encoding="UTF-8"?><Response><Say>Transfer unavailable.</Say><Hangup/></Response>',
            media_type="application/xml",
        )
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say>Please hold while I connect you with a member of our team.</Say>
  <Dial>{to}</Dial>
</Response>"""
    return PlainTextResponse(content=twiml, media_type="application/xml")


# ── WebSocket: Twilio Media Stream ─────────────────────────────────────────────

@app.websocket("/stream")
async def stream_websocket(websocket: WebSocket):
    """
    Twilio connects here when the prospect picks up.
    Full Pipecat pipeline boots per call.

    Pipeline:
      TwilioTransport.input()
        → Deepgram STT
        → TranscriptLogger(user)
        → OptOutGuardrail
        → LLMContextAggregator (user side)
        → OpenAI LLM
        → TranscriptLogger(assistant)
        → Cartesia TTS
        → TwilioTransport.output()
        → LLMContextAggregator (assistant side)
    """
    await websocket.accept()
    call_sid = "unknown"
    stream_sid = "unknown"
    call_db_id = -1

    logger.info("Twilio WebSocket connected — reading call SID...")

    # Peek at the first two Twilio envelope messages (connected + start events)
    # to extract call SID before handing the socket to Pipecat.
    try:
        raw = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
        envelope = json.loads(raw)
        if envelope.get("event") != "connected":
            logger.warning(f"Unexpected first event: {envelope.get('event')}")

        raw2 = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
        envelope2 = json.loads(raw2)
        if envelope2.get("event") == "start":
            call_sid = envelope2.get("start", {}).get("callSid", "unknown")
            stream_sid = envelope2.get("start", {}).get("streamSid", "unknown")
            logger.info(f"Linked to call SID: {call_sid}")
            if call_sid in _call_db_ids:
                call_db_id = _call_db_ids[call_sid]
            else:
                # Inbound / unregistered call
                to_num = envelope2.get("start", {}).get("to", "unknown")
                call_db_id = start_call(call_sid, to_num)
                _call_db_ids[call_sid] = call_db_id
    except Exception as e:
        logger.warning(f"Could not read call SID from Twilio envelope: {e}")
        call_db_id = start_call("unknown", "unknown")

    # Resolve prospect's phone number (needed for SMS and opt-out tools)
    to_number_ref: list[str] = [get_call_number(call_db_id) or "unknown"]

    # ── Transport ─────────────────────────────────────────────────────────────
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_enabled=True,
            vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.4)),
            serializer=TwilioFrameSerializer(
                stream_sid,
                call_sid=call_sid,
                account_sid=os.environ.get("TWILIO_ACCOUNT_SID"),
                auth_token=os.environ.get("TWILIO_AUTH_TOKEN"),
            ),
        ),
    )

    # ── STT ───────────────────────────────────────────────────────────────────
    stt = DeepgramSTTService(api_key=os.environ["DEEPGRAM_API_KEY"])

    # ── LLM ───────────────────────────────────────────────────────────────────
    llm = OpenAILLMService(
        api_key=os.environ["OPENAI_API_KEY"],
        model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        tools=TOOLS,
    )

    # Mutable closure state
    booking_state: dict = {}
    outcome_state: list[str] = ["unknown"]
    task_ref: list = [None]   # populated after Pipeline/PipelineTask are created

    async def handle_tool_call(function_name, tool_call_id, arguments, llm, context, result_callback):
        result = await _dispatch_tool(
            function_name=function_name,
            arguments=arguments,
            call_db_id=call_db_id,
            to_number_ref=to_number_ref,
            outcome_state=outcome_state,
            task_ref=task_ref,
            booking_state=booking_state,
            call_sid=call_sid,
            is_browser=False,
        )
        await result_callback(json.dumps(result))

    llm.register_function(None, handle_tool_call)

    # ── LLM Context ───────────────────────────────────────────────────────────
    context = LLMContext(messages=[{"role": "system", "content": SYSTEM_PROMPT}])
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(context)

    # ── TTS ───────────────────────────────────────────────────────────────────
    # Language set via CartesiaTTSSettings. Default EN_AU; override via CARTESIA_LANGUAGE env var.
    # Env var format: "en_au" or "en-AU" → Language.EN_AU
    _lang_key = os.environ.get("CARTESIA_LANGUAGE", "en_au").upper().replace("-", "_")
    _lang = getattr(Language, _lang_key, Language.EN_AU)
    tts = CartesiaTTSService(
        api_key=os.environ["CARTESIA_API_KEY"],
        voice_id=os.environ["CARTESIA_VOICE_ID"],
        settings=CartesiaTTSSettings(
            language=_lang,
            generation_config=GenerationConfig(
                speed=1.05,         # Cartesia range: 0.6–1.5, 1.0=default; 1.05=slightly snappier
                emotion="positivity:medium",  # warm, upbeat but not fake
            ),
        ),
    )

    # ── Processors ────────────────────────────────────────────────────────────
    # Shared timing state: records user-turn-end timestamp, cleared on first assistant text
    turn_timing: dict = {"user_turn_ts": 0.0, "first_text_pending": False}

    user_logger      = TranscriptLogger(call_db_id, "user", timing_state=turn_timing)
    assistant_logger = TranscriptLogger(call_db_id, "assistant", timing_state=turn_timing)
    opt_out_guardrail = OptOutGuardrail(call_db_id, to_number_ref, outcome_state, task_ref)
    context_compactor = ContextCompactor(context, max_pairs=int(os.environ.get("LLM_CONTEXT_PAIRS", "5")))

    # ── Pipeline ──────────────────────────────────────────────────────────────
    #  user_logger → records turn timestamp
    #  opt_out_guardrail → hard opt-out detection
    #  context_compactor → trims context to last N pairs before LLM receives new turn
    #  assistant_logger → measures first-text latency
    pipeline = Pipeline([
        transport.input(),
        stt,
        user_logger,
        opt_out_guardrail,
        context_compactor,
        user_aggregator,
        llm,
        assistant_logger,
        tts,
        transport.output(),
        assistant_aggregator,
    ])

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=True,
            enable_metrics=True,
        ),
    )
    task_ref[0] = task   # make available to tool handlers and guardrail
    _active_tasks[call_sid] = task

    # Track answer time for duration_s calculation
    call_start_time: list[float] = [0.0]
    _flushed: list[bool] = [False]

    def _do_flush():
        if _flushed[0]:
            return
        _flushed[0] = True
        duration = round(time.time() - call_start_time[0], 1) if call_start_time[0] else None
        _flush_call_log(call_sid, call_db_id, outcome_state[0], booking_state or None, duration)

    # ── Events ────────────────────────────────────────────────────────────────

    @transport.event_handler("on_client_connected")
    async def on_connected(transport, client):
        call_start_time[0] = time.time()
        logger.info("Prospect answered — AI greeting started")
        await task.queue_frames([LLMMessagesFrame(context.messages)])

    @transport.event_handler("on_client_disconnected")
    async def on_disconnected(transport, client):
        logger.info("Call ended")
        if outcome_state[0] == "unknown":
            outcome_state[0] = "completed"
        _do_flush()
        await task.queue_frames([EndFrame()])

    # ── Run ───────────────────────────────────────────────────────────────────
    runner = PipelineRunner()
    await runner.run(task)

    _do_flush()  # no-op if already flushed in on_disconnected

    _active_tasks.pop(call_sid, None)
    _call_db_ids.pop(call_sid, None)
    logger.info(f"Pipeline done. DB id={call_db_id}  outcome={outcome_state[0]}")


# ── Static UI ─────────────────────────────────────────────────────────────────

@app.get("/")
async def serve_index():
    index = _STATIC_DIR / "index.html"
    if not index.is_file():
        return JSONResponse({"error": "UI not found. static/index.html is missing."}, status_code=404)
    return FileResponse(str(index))


# ── WebSocket: Browser Direct Voice ───────────────────────────────────────────

@app.websocket("/browser-stream")
async def browser_stream(websocket: WebSocket):
    """
    Browser clients connect here for a direct voice session without a phone.

    Audio protocol:
      Browser → Server: binary PCM16 @ 16kHz mono
      Server → Browser: binary PCM16 @ 16kHz mono  (TTS output)
      Server → Browser: text JSON  {"event": ..., ...}  (transcripts, status)

    Pipeline mirrors the Twilio stream but uses BrowserFrameSerializer.
    """
    await websocket.accept()

    session_id = f"browser-{uuid.uuid4().hex[:8]}"
    call_db_id = start_call(session_id, "browser")

    logger.info(f"Browser session started: {session_id}  DB id={call_db_id}")

    to_number_ref: list[str] = ["browser"]
    booking_state: dict = {}
    outcome_state: list[str] = ["unknown"]
    task_ref: list = [None]

    # ── Transport ─────────────────────────────────────────────────────────────
    _lang_key = os.environ.get("CARTESIA_LANGUAGE", "en_au").upper().replace("-", "_")
    _lang = getattr(Language, _lang_key, Language.EN_AU)

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=16000,
            audio_out_sample_rate=16000,
            vad_enabled=True,
            vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.4)),
            serializer=BrowserFrameSerializer(),
        ),
    )

    # ── Services ──────────────────────────────────────────────────────────────
    stt = DeepgramSTTService(api_key=os.environ["DEEPGRAM_API_KEY"])
    llm = OpenAILLMService(
        api_key=os.environ["OPENAI_API_KEY"],
        model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        tools=TOOLS,
    )
    tts = CartesiaTTSService(
        api_key=os.environ["CARTESIA_API_KEY"],
        voice_id=os.environ["CARTESIA_VOICE_ID"],
        sample_rate=16000,
        settings=CartesiaTTSSettings(
            language=_lang,
            generation_config=GenerationConfig(
                speed=1.05,
                emotion="positivity:medium",
            ),
        ),
    )

    # ── Tool handler ──────────────────────────────────────────────────────────
    async def handle_browser_tool(function_name, tool_call_id, arguments, llm, context, result_callback):
        result = await _dispatch_tool(
            function_name=function_name,
            arguments=arguments,
            call_db_id=call_db_id,
            to_number_ref=to_number_ref,
            outcome_state=outcome_state,
            task_ref=task_ref,
            booking_state=booking_state,
            call_sid=session_id,
            is_browser=True,
        )
        await result_callback(json.dumps(result))

    llm.register_function(None, handle_browser_tool)

    # ── LLM Context ───────────────────────────────────────────────────────────
    context = LLMContext(messages=[{"role": "system", "content": SYSTEM_PROMPT}])
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(context)

    # ── Processors ────────────────────────────────────────────────────────────
    turn_timing: dict = {"user_turn_ts": 0.0, "first_text_pending": False}
    max_pairs = int(os.environ.get("LLM_CONTEXT_PAIRS", "5"))

    user_logger      = TranscriptLogger(call_db_id, "user",      timing_state=turn_timing, event_ws=websocket)
    assistant_logger = TranscriptLogger(call_db_id, "assistant", timing_state=turn_timing, event_ws=websocket)
    opt_out_guardrail = OptOutGuardrail(call_db_id, to_number_ref, outcome_state, task_ref)
    context_compactor = ContextCompactor(context, max_pairs=max_pairs)
    status_user = StatusEventSender(websocket)   # catches UserStarted/StoppedSpeakingFrame
    status_bot  = StatusEventSender(websocket)   # catches BotStarted/StoppedSpeakingFrame

    # ── Pipeline ──────────────────────────────────────────────────────────────
    pipeline = Pipeline([
        transport.input(),
        status_user,
        stt,
        user_logger,
        opt_out_guardrail,
        context_compactor,
        user_aggregator,
        llm,
        assistant_logger,
        tts,
        status_bot,
        transport.output(),
        assistant_aggregator,
    ])

    task = PipelineTask(
        pipeline,
        params=PipelineParams(allow_interruptions=True, enable_metrics=True),
    )
    task_ref[0] = task

    call_start_time: list[float] = [0.0]
    _flushed: list[bool] = [False]

    def _do_flush():
        if _flushed[0]:
            return
        _flushed[0] = True
        duration = round(time.time() - call_start_time[0], 1) if call_start_time[0] else None
        _flush_call_log(session_id, call_db_id, outcome_state[0], booking_state or None, duration)

    # ── Events ────────────────────────────────────────────────────────────────
    @transport.event_handler("on_client_connected")
    async def on_browser_connected(transport, client):
        call_start_time[0] = time.time()
        logger.info(f"Browser session live: {session_id}")
        # Greeting already streamed before pipeline start (see below).
        # Only fall back to live LLM if no cached audio was available.
        if not _cached_greeting_audio:
            await task.queue_frames([LLMMessagesFrame(context.messages)])

    @transport.event_handler("on_client_disconnected")
    async def on_browser_disconnected(transport, client):
        logger.info(f"Browser session ended: {session_id}")
        if outcome_state[0] == "unknown":
            outcome_state[0] = "completed"
        _do_flush()
        await task.queue_frames([EndFrame()])

    # ── Stream greeting BEFORE pipeline starts ────────────────────────────────
    # Sending all audio into the WebSocket send-buffer now means the browser
    # will receive it the moment its AudioContext is ready, with no pipeline
    # latency involved. "ready" is sent LAST so the browser starts the mic only
    # after all greeting frames are already queued for delivery.
    if _cached_greeting_audio:
        try:
            log_turn(call_db_id, "assistant", GREETING_TEXT)
            context.messages.append({"role": "assistant", "content": GREETING_TEXT})
            await websocket.send_text(json.dumps({"event": "status", "state": "bot_speaking"}))
            chunk_size = 3200  # 100 ms of PCM16 @ 16 kHz mono
            for i in range(0, len(_cached_greeting_audio), chunk_size):
                await websocket.send_bytes(_cached_greeting_audio[i:i + chunk_size])
            await websocket.send_text(json.dumps({"event": "transcript", "role": "assistant", "text": GREETING_TEXT}))
            await websocket.send_text(json.dumps({"event": "status", "state": "listening"}))
            logger.info("[PREWARM] Cached greeting queued into WS send-buffer")
        except Exception as e:
            logger.warning(f"Cached greeting send failed: {e}")

    # "ready" sent last — browser starts mic only after greeting is buffered
    try:
        await websocket.send_text(json.dumps({"event": "ready", "session_id": session_id}))
    except Exception:
        pass

    runner = PipelineRunner()
    await runner.run(task)

    _do_flush()
    logger.info(f"Browser pipeline done. DB id={call_db_id}  outcome={outcome_state[0]}")


# ==============================================================================
#  HELPERS
# ==============================================================================

def _flush_call_log(
    call_sid: str,
    call_db_id: int,
    outcome: str,
    booking: dict | None,
    duration_s: float | None = None,
):
    if call_db_id < 0:
        return
    try:
        end_call(call_db_id, outcome, booking, duration_s)
        logger.info(f"Call log saved → DB id={call_db_id}  outcome={outcome}  duration={duration_s}s")
    except Exception as e:
        logger.error(f"Failed to flush call log: {e}")


async def _delayed_end(task: PipelineTask, delay: float = 3.0):
    """Let TTS finish saying goodbye, then end the pipeline."""
    await asyncio.sleep(delay)
    await task.queue_frames([EndFrame()])


def _mock_available_slots() -> list[str]:
    """Return 3 business-day slots starting from tomorrow (Australia/Sydney)."""
    slots = []
    base = datetime.now(SYDNEY_TZ) + timedelta(days=1)
    for _ in range(3):
        while base.weekday() >= 5:   # skip weekends
            base += timedelta(days=1)
        slots.append(base.replace(hour=14, minute=0, second=0, microsecond=0).isoformat())
        base += timedelta(days=1)
    return slots


# ==============================================================================
#  ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    logger.info(
        f"\n{'='*60}\n"
        f"  Voice Sales Agent\n"
        f"\n"
        f"  NEXT STEPS:\n"
        f"  1. Expose:     ngrok http {port}\n"
        f"  2. Set PUBLIC_BASE_URL in .env to your ngrok URL\n"
        f"  3. Dial:       python dial.py +61412345678\n"
        f"  4. View logs:  GET http://localhost:{port}/calls\n"
        f"  5. Terminal:   python show_calls.py\n"
        f"  6. Readiness:  GET http://localhost:{port}/readyz\n"
        f"{'='*60}\n"
    )
    uvicorn.run(app, host="0.0.0.0", port=port)
