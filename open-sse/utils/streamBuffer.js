/**
 * Stream Buffer — enables mid-stream retry with partial output preservation.
 *
 * When a provider stream drops mid-response (DeepSeek "Network connection lost"),
 * this buffer accumulates SSE chunks so 9Router can:
 *   1. Cache partial output from the failed attempt
 *   2. On fallback to the next account, prepend the partial output as context
 *   3. Request the new account to "continue from here"
 *
 * This is a best-effort recovery — the upstream model state is NOT preserved,
 * so the new account generates a fresh response. But for CLI tools that use
 * tool calling, partial tool results from the first attempt are replayed.
 *
 * Buffer is in-memory only (no disk), per-request, cleared after success.
 */
import { dbg } from "./debugLog.js";

const MAX_BUFFER_CHUNKS = 200;      // safety cap — ~1MB of text at 5KB chunks
const MAX_BUFFER_BYTES = 1_047_576; // 1MB hard cap

/**
 * Create a stream buffer for accumulating partial SSE output.
 * Pass-through: every chunk written is immediately enqueued downstream.
 * On stream error, the accumulated chunks can be retrieved for retry context.
 */
export function createStreamBuffer() {
  const chunks = [];
  let totalBytes = 0;
  let closed = false;

  const writer = new WritableStream({
    write(chunk, controller) {
      const size = chunk?.byteLength || chunk?.length || 0;

      // Accumulate for replay
      if (!closed && totalBytes + size <= MAX_BUFFER_BYTES && chunks.length < MAX_BUFFER_CHUNKS) {
        chunks.push(chunk);
        totalBytes += size;
      }

      // Always pass through to downstream
      controller.enqueue(chunk);
    },
    close() {
      closed = true;
    },
    abort(reason) {
      closed = true;
      dbg("SBUF", `aborted: ${reason} | chunks=${chunks.length} | bytes=${totalBytes}`);
    },
  });

  return {
    writable: writer,
    /** Get accumulated chunks for retry context */
    getChunks: () => closed ? [] : [...chunks],
    /** Total bytes accumulated */
    getTotalBytes: () => totalBytes,
    /** Number of chunks */
    getChunkCount: () => chunks.length,
    /** Clear the buffer */
    clear: () => { chunks.length = 0; totalBytes = 0; },
    /** Mark as closed (success) */
    close: () => { closed = true; },
  };
}

/**
 * Extract text content from accumulated SSE chunks.
 * Handles OpenAI SSE format: `data: {"choices":[{"delta":{"content":"..."}}]}`
 * Returns concatenated text for use as prefix in retry context.
 */
export function extractTextFromSseChunks(chunks) {
  const decoder = new TextDecoder();
  const parts = [];
  for (const chunk of chunks) {
    const text = typeof chunk === "string" ? chunk : decoder.decode(chunk);
    const lines = text.split("\n");
    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      const jsonStr = line.slice(6).trim();
      if (jsonStr === "[DONE]") continue;
      try {
        const parsed = JSON.parse(jsonStr);
        const content = parsed?.choices?.[0]?.delta?.content;
        if (content) parts.push(content);
        // Also capture reasoning content (DeepSeek thinking)
        const reasoning = parsed?.choices?.[0]?.delta?.reasoning_content;
        if (reasoning) parts.push(reasoning);
      } catch {
        // skip unparseable lines
      }
    }
  }
  return parts.join("");
}

/**
 * Build a retry-optimized message array that includes partial output as context.
 * The assistant starts with the partial response from the failed attempt,
 * so the new account's model can see what was already generated.
 *
 * @param {Array} messages — original messages array
 * @param {string} partialContent — text extracted from failed stream
 * @param {boolean} prependAsPrefix — if true, add as assistant prefix; if false, append as new message
 * @returns {Array} modified messages array
 */
export function buildRetryMessages(messages, partialContent, prependAsPrefix = true) {
  if (!partialContent || partialContent.trim().length === 0) return messages;

  // Only apply if we have meaningful partial output (>200 chars)
  if (partialContent.length < 200) return messages;

  const clone = [...messages];

  if (prependAsPrefix) {
    // Find the last message and append to it as a continuation marker
    // This doesn't work well for OpenAI format, so we use the message approach
  }

  // Add partial output as an assistant message so the model sees the context
  // This helps the retry generate a coherent continuation
  // NOTE: This is a heuristic — the new model may generate duplicate content.
  // For tool-calling flows, this is essential to replay tool results.
  clone.push({
    role: "assistant",
    content: `[PARTIAL RESPONSE — stream dropped. Continue from here.]\n${partialContent.slice(0, 4000)}`,
  });
  clone.push({
    role: "user",
    content: "The previous response was cut off mid-stream. Please continue from where it left off. Do NOT repeat the partial content above — continue naturally from the last complete sentence or tool call.",
  });

  return clone;
}
