/**
 * CommandCode → OpenAI response translator
 *
 * CommandCode upstream emits NDJSON-style AI SDK v5 stream events.
 * Each upstream "event" arrives as one JSON object per line.
 */
import { register } from "../index.js";
import { FORMATS } from "../formats.js";

function ensureState(state, model) {
  if (!state.responseId) {
    state.responseId = `chatcmpl-${Date.now()}`;
    state.created = Math.floor(Date.now() / 1000);
    state.model = state.model || model || "commandcode";
    state.chunkIndex = 0;
    state.toolIndex = 0;
    state.toolIndexById = new Map();
    state.openTools = new Set();
    state.openText = false;
    state.finishReason = null;
    state.usage = null;
  }
}

function makeChunk(state, delta, finishReason = null) {
  return {
    id: state.responseId,
    object: "chat.completion.chunk",
    created: state.created,
    model: state.model,
    choices: [{ index: 0, delta, finish_reason: finishReason }],
  };
}

function mapFinishReason(reason) {
  switch (reason) {
    case "stop": return "stop";
    case "length": return "length";
    case "tool-calls":
    case "tool_use": return "tool_calls";
    case "content-filter": return "content_filter";
    case "error": return "stop";
    default: return reason || "stop";
  }
}

export function convertCommandCodeToOpenAI(chunk, state) {
  if (!chunk) return null;

  if (chunk && typeof chunk === "object" && chunk.object === "chat.completion.chunk") {
    return chunk;
  }

  let event = chunk;
  if (typeof chunk === "string") {
    const line = chunk.trim();
    if (!line) return null;
    const json = line.startsWith("data:") ? line.slice(5).trim() : line;
    if (!json || json === "[DONE]") return null;
    try {
      event = JSON.parse(json);
    } catch {
      return null;
    }
  }

  if (!event || typeof event !== "object" || !event.type) return null;

  ensureState(state, event.model);
  const out = [];

  switch (event.type) {
    case "text-delta": {
      const text = event.text || event.delta || "";
      if (!text) break;
      const delta = state.chunkIndex === 0 ? { role: "assistant", content: text } : { content: text };
      state.chunkIndex++;
      state.openText = true;
      out.push(makeChunk(state, delta));
      break;
    }
    case "reasoning-delta": {
      const text = event.text || "";
      if (!text) break;
      const delta = state.chunkIndex === 0
        ? { role: "assistant", reasoning_content: text }
        : { reasoning_content: text };
      state.chunkIndex++;
      out.push(makeChunk(state, delta));
      break;
    }
    case "tool-input-start": {
      const id = event.id || event.toolCallId || `call_${Date.now()}_${state.toolIndex}`;
      let idx = state.toolIndexById.get(id);
      if (idx == null) {
        idx = state.toolIndex++;
        state.toolIndexById.set(id, idx);
      }
      state.openTools.add(id);
      const delta = {
        ...(state.chunkIndex === 0 ? { role: "assistant" } : {}),
        tool_calls: [{
          index: idx,
          id,
          type: "function",
          function: { name: event.toolName || "", arguments: "" },
        }],
      };
      state.chunkIndex++;
      out.push(makeChunk(state, delta));
      break;
    }
    case "tool-input-delta": {
      const id = event.id || event.toolCallId;
      const idx = state.toolIndexById.get(id);
      if (idx == null) break;
      const delta = {
        tool_calls: [{
          index: idx,
          function: { arguments: event.delta || event.inputTextDelta || "" },
        }],
      };
      out.push(makeChunk(state, delta));
      break;
    }
    case "tool-call": {
      const id = event.toolCallId;
      if (state.toolIndexById.has(id)) break;
      const idx = state.toolIndex++;
      state.toolIndexById.set(id, idx);
      const argsStr = typeof event.input === "string" ? event.input : JSON.stringify(event.input ?? {});
      const delta = {
        ...(state.chunkIndex === 0 ? { role: "assistant" } : {}),
        tool_calls: [{
          index: idx,
          id,
          type: "function",
          function: { name: event.toolName || "", arguments: argsStr },
        }],
      };
      state.chunkIndex++;
      out.push(makeChunk(state, delta));
      break;
    }
    case "finish-step": {
      state.finishReason = mapFinishReason(event.finishReason);
      if (event.usage) state.usage = event.usage;
      break;
    }
    case "finish": {
      const finishReason = state.finishReason || mapFinishReason(event.finishReason || "stop");
      const finalChunk = makeChunk(state, {}, finishReason);
      const totalUsage = event.totalUsage || state.usage;
      if (totalUsage) {
        finalChunk.usage = {
          prompt_tokens: totalUsage.inputTokens ?? 0,
          completion_tokens: totalUsage.outputTokens ?? 0,
          total_tokens: totalUsage.totalTokens ?? ((totalUsage.inputTokens ?? 0) + (totalUsage.outputTokens ?? 0)),
        };
      }
      out.push(finalChunk);
      break;
    }
    // Mid-stream server error from CommandCode (network drops, internal failures)
    // Comes as {"type":"server_error","message":"Network connection lost."}
    case "server_error": {
      state.finishReason = "stop";
      const msg = event.message || event.error || JSON.stringify(event);
      const msgStr = typeof msg === "string" ? msg : JSON.stringify(msg);
      out.push(makeChunk(state, { content: `\n\n[CommandCode server error: ${msgStr}]` }));
      out.push(makeChunk(state, {}, "stop"));
      break;
    }
    // Client error from CommandCode
    case "error": {
      state.finishReason = "stop";
      const errVal = event.error ?? event.message ?? "unknown";
      const errStr = typeof errVal === "string" ? errVal : JSON.stringify(errVal);
      out.push(makeChunk(state, { content: `\n\n[CommandCode error: ${errStr}]` }));
      out.push(makeChunk(state, {}, "stop"));
      break;
    }
    default:
      break;
  }

  return out.length ? out : null;
}

register(FORMATS.COMMANDCODE, FORMATS.OPENAI, null, convertCommandCodeToOpenAI);
