/**
 * Stream Buffer — enables mid-stream retry with partial output preservation.
 *
 * When a provider stream drops mid-response (DeepSeek "Network connection lost"),
 * this buffer accumulates SSE chunks as a TransformStream so 9Router can:
 *   1. Capture partial output from the failed attempt
 *   2. On fallback, prepend the partial output as context
 *   3. Request a new account to "continue from here"
 *
 * Buffer is in-memory only (no disk), per-request, cleared after success.
 */
import { dbg } from "./debugLog.js";

const MAX_BUFFER_CHUNKS = 200;
const MAX_BUFFER_BYTES = 1_047_576;

export function createStreamBuffer() {
  const chunks = [];
  let totalBytes = 0;
  let closed = false;

  const transform = new TransformStream({
    transform(chunk, controller) {
      const size = chunk?.byteLength || chunk?.length || 0;
      if (!closed && totalBytes + size <= MAX_BUFFER_BYTES && chunks.length < MAX_BUFFER_CHUNKS) {
        chunks.push(chunk);
        totalBytes += size;
      }
      controller.enqueue(chunk);
    },
    flush() { closed = true; },
  });

  return {
    stream: transform,
    getChunks: () => [...chunks],
    getTotalBytes: () => totalBytes,
    getChunkCount: () => chunks.length,
    clear: () => { chunks.length = 0; totalBytes = 0; },
    close: () => { closed = true; },
  };
}

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
        const reasoning = parsed?.choices?.[0]?.delta?.reasoning_content;
        if (reasoning) parts.push(reasoning);
      } catch { /* skip */ }
    }
  }
  return parts.join("");
}

export function buildRetryMessages(messages, partialContent) {
  if (!partialContent || partialContent.trim().length === 0) return messages;
  if (partialContent.length < 100) return messages;
  const clone = [...messages];
  clone.push({
    role: "assistant",
    content: `[PARTIAL RESPONSE — stream dropped mid-way. Continue from here.]\n${partialContent.slice(0, 4000)}`,
  });
  clone.push({
    role: "user",
    content: "The previous response was cut off mid-stream. Please continue from where it left off. Do NOT repeat the partial content above — continue naturally from the last complete sentence or tool call.",
  });
  return clone;
}

export function isMidStreamDrop(error) {
  if (!error) return false;
  const msg = error?.message || "";
  const code = error?.code || error?.cause?.code || "";
  return (
    error.name === "TypeError" && (msg.includes("network") || msg.includes("fetch")) ||
    msg.includes("socket hang up") ||
    msg.includes("ECONNRESET") ||
    msg.includes("ETIMEDOUT") ||
    msg.includes("EPIPE") ||
    msg.includes("UND_ERR_SOCKET") ||
    msg.includes("Network connection lost") ||
    msg.includes("stream closed before") ||
    code === "ECONNRESET" ||
    code === "ETIMEDOUT" ||
    code === "EPIPE" ||
    code === "UND_ERR_SOCKET"
  );
}
