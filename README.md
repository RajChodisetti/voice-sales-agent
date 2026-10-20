# Streaming Voice Agent

A Python voice-interaction prototype with two transports—browser audio and Twilio calls—sharing a Pipecat speech-to-text → language-model → text-to-speech pipeline.

The engineering focus is real-time audio orchestration: interruptible responses, bounded conversation context, tool dispatch, transcript persistence, and session cleanup. The included **Example Studio** persona is fictional demo content.

## Implemented capabilities

- **Browser conversation:** microphone input, streamed audio output, transcript events, and listening/speaking status in the included UI.
- **Telephony adapter:** Twilio Media Streams, call-status callbacks, outbound call initiation, and optional SMS/human-transfer tools.
- **Shared pipeline:** Deepgram STT, OpenAI responses, Cartesia TTS, voice-activity detection, and interruption support.
- **Conversation controls:** retained recent context pairs, opt-out phrase handling, explicit end-call tooling, and campaign pause state.
- **Local evidence:** SQLite call records, ordered transcript turns, outcomes, durations, and an internal opt-out list.
- **Operational hooks:** liveness/readiness endpoints and Twilio signature checking outside development mode.

## Design choices and tradeoffs

| Choice | Purpose | Current boundary |
| --- | --- | --- |
| Shared tool dispatcher | Keep browser and phone behavior in one place | SMS and transfer behavior differ by transport |
| Audio frame pipeline | Process speech incrementally and allow interruptions | Live quality and latency depend on providers and network |
| Context compaction | Bound conversation history sent to the model | Retains recent turns rather than an unlimited transcript |
| SQLite logging | Make sessions inspectable without a separate database service | Distributed persistence is not implemented |
| Cached greeting | Prepare initial audio before a browser conversation | Provider calls can occur during startup |
| Local session maps | Track active tasks and campaign controls simply | State is process-local, not a durable multi-worker queue |

## Code tour

- [`bot.py`](bot.py): transports, processors, tool dispatch, lifecycle, and HTTP endpoints.
- [`browser_serializer.py`](browser_serializer.py): browser audio/frame conversion.
- [`static/`](static): browser UI and AudioWorklet microphone processor.
- [`logger_db.py`](logger_db.py): SQLite transcript and outcome storage.
- [`show_calls.py`](show_calls.py): local log inspection.
- [`voice_sales_agent_architecture_guide.md`](voice_sales_agent_architecture_guide.md): original design exploration; recommendations there are not a checklist of implemented features.

## Try it locally

Use Python 3.12 and an isolated environment:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Configure local environment variables for `DEEPGRAM_API_KEY`, `OPENAI_API_KEY`, `CARTESIA_API_KEY`, and `CARTESIA_VOICE_ID`. `OPENAI_MODEL` is optional. The phone adapter also requires `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_PHONE_NUMBER`, and `PUBLIC_BASE_URL`.

```sh
uvicorn bot:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000/` for the browser UI. Provider-backed startup/audio uses external services and may incur charges. `/readyz` checks environment-variable presence, including telephony settings; it does not prove provider connectivity.

## Validation and limitations

Python syntax can be checked without calling a provider:

```sh
python3 -m py_compile bot.py browser_serializer.py logger_db.py dial.py show_calls.py
```

On 2026-10-19 (America/Phoenix), Python syntax checks and an isolated temporary-SQLite smoke check passed for transcript persistence, opt-out lookup, and repeated opt-out recording. No provider calls or real conversations were used.

There is currently no committed automated end-to-end audio test suite or latency benchmark. Syntax validation is not an integration test.

Calendar availability and appointment booking are **mock implementations**; the sample calendar URL uses `example.com`. The code includes a regional calling-window example, not a guarantee of legal compliance. Redis is present in the Compose scaffold, but active task and campaign state remain in memory. Call, log, and admin endpoints need access controls before any internet deployment. Use synthetic conversations in local development.
