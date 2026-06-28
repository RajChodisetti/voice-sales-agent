"""
BrowserFrameSerializer — PCM16 serializer for browser WebSocket sessions.

Protocol:
  Browser → Server:
    binary  = raw PCM16 audio @ 16kHz mono  →  InputAudioRawFrame
    text    = {"event": "stop"}             →  EndFrame

  Server → Browser:
    binary  = raw PCM16 audio @ 16kHz mono  ←  OutputAudioRawFrame
    text    = {"event": ..., ...}           ←  sent directly via ws.send_text() in StatusEventSender / TranscriptLogger
"""

import json

from pipecat.frames.frames import AudioRawFrame, EndFrame, Frame, InputAudioRawFrame, OutputAudioRawFrame
from pipecat.serializers.base_serializer import FrameSerializer


class BrowserFrameSerializer(FrameSerializer):
    """
    Converts between raw PCM16 WebSocket binary frames and pipecat audio frames.
    JSON text frames (transcripts, status) are sent directly from processors — not via this serializer.
    """

    async def serialize(self, frame: Frame) -> str | bytes | None:
        if isinstance(frame, (OutputAudioRawFrame, AudioRawFrame)):
            return frame.audio
        return None

    async def deserialize(self, data: str | bytes) -> Frame | None:
        if isinstance(data, bytes):
            if not data:
                return None
            return InputAudioRawFrame(audio=data, sample_rate=16000, num_channels=1)

        try:
            msg = json.loads(data)
            if msg.get("event") == "stop":
                return EndFrame()
        except Exception:
            pass

        return None
