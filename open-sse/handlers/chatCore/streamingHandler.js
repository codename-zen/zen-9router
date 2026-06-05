import { FORMATS } from "../../translator/formats.js";
import { needsTranslation } from "../../translator/index.js";
import { createSSETransformStreamWithLogger, createPassthroughStreamWithLogger } from "../../utils/stream.js";
import { buildRequestDetail, extractRequestConfig, saveUsageStats } from "./requestDetail.js";
import { saveRequestDetail } from "@/lib/usageDb.js";
import { createStreamBuffer, extractTextFromSseChunks, buildRetryMessages, isMidStreamDrop } from "../../utils/streamBuffer.js";
import { COLORS } from "../../utils/stream.js";

const SSE_HEADERS = {
  "Content-Type": "text/event-stream",
  "Cache-Control": "no-cache",
  "Connection": "keep-alive",
  "Access-Control-Allow-Origin": "*"
};

const STREAM_RETRY_MAX = 1;

function buildTransformStream({ provider, sourceFormat, targetFormat, userAgent, reqLogger, toolNameMap, model, connectionId, body, onStreamComplete, apiKey }) {
  const isDroidCLI = userAgent?.toLowerCase().includes("droid") || userAgent?.toLowerCase().includes("codex-cli");
  const needsCodexTranslation = provider === "codex" && targetFormat === FORMATS.OPENAI_RESPONSES && !isDroidCLI;

  if (needsCodexTranslation) {
    let codexTarget;
    if (sourceFormat === FORMATS.OPENAI_RESPONSES) codexTarget = FORMATS.OPENAI_RESPONSES;
    else if (sourceFormat === FORMATS.CLAUDE) codexTarget = FORMATS.CLAUDE;
    else if (sourceFormat === FORMATS.ANTIGRAVITY || sourceFormat === FORMATS.GEMINI || sourceFormat === FORMATS.GEMINI_CLI) codexTarget = FORMATS.ANTIGRAVITY;
    else codexTarget = FORMATS.OPENAI;
    return createSSETransformStreamWithLogger(FORMATS.OPENAI_RESPONSES, codexTarget, provider, reqLogger, toolNameMap, model, connectionId, body, onStreamComplete, apiKey);
  }

  if (needsTranslation(targetFormat, sourceFormat)) {
    return createSSETransformStreamWithLogger(targetFormat, sourceFormat, provider, reqLogger, toolNameMap, model, connectionId, body, onStreamComplete, apiKey);
  }

  return createPassthroughStreamWithLogger(provider, reqLogger, model, connectionId, body, onStreamComplete, apiKey);
}

function buildPipeline(upstreamBody, streamBuf, transformStream) {
  return upstreamBody
    .pipeThrough(streamBuf.stream)
    .pipeThrough(transformStream);
}

function getTimestamp() {
  return new Date().toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export async function handleStreamingResponse({ providerResponse, provider, model, sourceFormat, targetFormat, userAgent, body, stream, translatedBody, finalBody, requestStartTime, connectionId, apiKey, clientRawRequest, onRequestSuccess, reqLogger, toolNameMap, streamController, onStreamComplete, retryContext }) {
  if (onRequestSuccess) onRequestSuccess();

  const streamDetailId = `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;

  const streamBuf = createStreamBuffer();
  const transformStream = buildTransformStream({ provider, sourceFormat, targetFormat, userAgent, reqLogger, toolNameMap, model, connectionId, body, onStreamComplete, apiKey });
  let currentPipe = buildPipeline(providerResponse.body, streamBuf, transformStream);
  let pipeReader = currentPipe.getReader();
  let retryCount = 0;
  let streamEnded = false;

  const retryableStream = new ReadableStream({
    async pull(controller) {
      if (streamEnded) return;
      try {
        const { done, value } = await pipeReader.read();
        if (done) { streamEnded = true; controller.close(); return; }
        controller.enqueue(value);
      } catch (err) {
        if (retryCount < STREAM_RETRY_MAX && isMidStreamDrop(err) && retryContext?.executor) {
          retryCount++;
          const chunks = streamBuf.getChunks();
          const partialText = extractTextFromSseChunks(chunks);
          console.log(`[${getTimestamp()}] ${COLORS.yellow}[STREAM RETRY] ${provider}/${model} | retry=${retryCount} | chunks=${chunks.length} | text_len=${partialText?.length || 0} | err=${err.message}${COLORS.reset}`);
          if (partialText && partialText.length > 100) {
            const origMessages = body.messages || translatedBody?.messages || [];
            const retryBody = { ...(translatedBody || body), messages: buildRetryMessages(origMessages, partialText), model, stream: true };
            try {
              const retryResult = await retryContext.executor.execute({ model, body: retryBody, stream: true, credentials: retryContext.credentials, signal: streamController.signal, log: retryContext.log, proxyOptions: retryContext.proxyOptions });
              if (retryResult.response?.ok && retryResult.response.body) {
                const retryBuf = createStreamBuffer();
                const retryTransform = createPassthroughStreamWithLogger(provider, reqLogger, model, connectionId, retryBody, onStreamComplete, apiKey);
                const newPipe = buildPipeline(retryResult.response.body, retryBuf, retryTransform);
                try { pipeReader.releaseLock(); } catch (_) {}
                pipeReader = newPipe.getReader();
                console.log(`[${getTimestamp()}] ${COLORS.green}[STREAM RETRY] ${provider}/${model} | continuing from new pipe${COLORS.reset}`);
                return this.pull(controller);
              }
            } catch (execErr) {
              console.log(`[${getTimestamp()}] [STREAM RETRY] ${provider}/${model} | retry execute failed: ${execErr.message}`);
            }
          }
        }
        controller.error(err);
      }
    },
    cancel(reason) { streamEnded = true; try { pipeReader.cancel(reason).catch(() => {}); } catch (_) {} },
  });

  saveRequestDetail(buildRequestDetail({
    provider, model, connectionId,
    latency: { ttft: 0, total: Date.now() - requestStartTime },
    tokens: { prompt_tokens: 0, completion_tokens: 0 },
    request: extractRequestConfig(body, stream),
    providerRequest: finalBody || translatedBody || null,
    providerResponse: "[Streaming - raw response not captured]",
    response: { content: "[Streaming in progress...]", thinking: null, type: "streaming" },
    status: "success"
  }, { id: streamDetailId })).catch(() => {});

  return { success: true, response: new Response(retryableStream, { headers: SSE_HEADERS }) };
}

export function buildOnStreamComplete({ provider, model, connectionId, apiKey, requestStartTime, body, stream, finalBody, translatedBody, clientRawRequest }) {
  const streamDetailId = `${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;
  const onStreamComplete = (contentObj, usage, ttftAt) => {
    const latency = { ttft: ttftAt ? ttftAt - requestStartTime : Date.now() - requestStartTime, total: Date.now() - requestStartTime };
    const safeContent = contentObj?.content || "[Empty streaming response]";
    const safeThinking = contentObj?.thinking || null;
    saveRequestDetail(buildRequestDetail({ provider, model, connectionId, latency, tokens: usage || { prompt_tokens: 0, completion_tokens: 0 }, request: extractRequestConfig(body, stream), providerRequest: finalBody || translatedBody || null, providerResponse: safeContent, response: { content: safeContent, thinking: safeThinking, type: "streaming" }, status: "success" }, { id: streamDetailId })).catch(() => {});
    saveUsageStats({ provider, model, tokens: usage, connectionId, apiKey, endpoint: clientRawRequest?.endpoint, label: "STREAM USAGE" });
  };
  return { onStreamComplete, streamDetailId };
}
