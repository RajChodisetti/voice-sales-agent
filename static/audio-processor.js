/**
 * AudioWorklet processor: captures mic audio as PCM16 and posts it to the main thread.
 * Runs at the input sample rate (16kHz), converts float32 → int16, sends 20ms chunks.
 */
class MicProcessor extends AudioWorkletProcessor {
    constructor() {
        super();
        this._buffer = [];
        // 16kHz × 20ms = 320 samples per chunk
        this._chunkSize = 320;
    }

    process(inputs) {
        const input = inputs[0];
        if (!input || !input[0]) return true;

        const samples = input[0];

        for (let i = 0; i < samples.length; i++) {
            const clamped = Math.max(-1, Math.min(1, samples[i]));
            this._buffer.push(Math.round(clamped * 32767));
        }

        while (this._buffer.length >= this._chunkSize) {
            const chunk = this._buffer.splice(0, this._chunkSize);
            const int16 = new Int16Array(chunk);
            this.port.postMessage(int16.buffer, [int16.buffer]);
        }

        return true;
    }
}

registerProcessor("mic-processor", MicProcessor);
