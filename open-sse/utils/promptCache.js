/**
 * Prompt Cache — translation-level request caching.
 * 
 * When 9Router retries across accounts for transient errors, the same request body
 * gets translated multiple times (source → target format). This module caches the
 * translation output keyed by a hash of the source request body, so retries to
 * different accounts skip the expensive translation step entirely.
 *
 * Cache entries expire after CACHE_TTL_MS (30s by default) — long enough for
 * account-level retry loops, short enough to avoid stale entries across turns.
 *
 * Additionally provides a lightweight "request fingerprint" for dedup logging.
 *
 * Inspired by Anthropic Claude Code's prompt caching (which caches at the provider
 * level via cache_control breakpoints). This is a layer above — we cache the
 * 9Router internal translation to avoid re-doing work during retries.
 */
import { createHash } from "crypto";

const CACHE_TTL_MS = 30_000;              // how long translated requests stay cached
const SWEEP_INTERVAL_MS = 60_000;         // how often we prune expired entries
const MAX_CACHE_ENTRIES = 200;            // safety cap — old entries evicted FIFO

/** @type {Map<string, {result: any, expiresAt: number}>} */
const _cache = new Map();

let _sweepTimer = null;

function ensureSweeper() {
  if (_sweepTimer) return;
  _sweepTimer = setInterval(sweep, SWEEP_INTERVAL_MS);
  if (_sweepTimer.unref) _sweepTimer.unref();
}

function sweep() {
  const now = Date.now();
  for (const [key, entry] of _cache) {
    if (entry.expiresAt <= now) _cache.delete(key);
  }
  if (_cache.size === 0 && _sweepTimer) {
    clearInterval(_sweepTimer);
    _sweepTimer = null;
  }
}

/**
 * Generate a deterministic hash of the request body.
 * Only keys that affect translation are included (model, messages, tools, etc.).
 * @param {object} body — the source request body
 * @returns {string} hex hash
 */
export function hashRequest(body) {
  const canonical = JSON.stringify({
    model: body.model,
    messages: body.messages,
    input: body.input,
    tools: body.tools,
    stream: body.stream,
    tool_choice: body.tool_choice,
    response_format: body.response_format,
  });
  return createHash("sha256").update(canonical).digest("hex").slice(0, 16);
}

/**
 * Try to get a cached translation result.
 * @param {string} key — hash from hashRequest()
 * @returns {any|null} cached translation result, or null
 */
export function getCachedTranslation(key) {
  ensureSweeper();
  const entry = _cache.get(key);
  if (!entry) return null;
  if (entry.expiresAt <= Date.now()) {
    _cache.delete(key);
    return null;
  }
  return entry.result;
}

/**
 * Store a translation result in the cache.
 * @param {string} key — hash from hashRequest()
 * @param {any} result — translated request body
 */
export function setCachedTranslation(key, result) {
  ensureSweeper();
  if (_cache.size >= MAX_CACHE_ENTRIES) {
    // FIFO eviction: delete oldest entry
    const oldest = _cache.keys().next().value;
    _cache.delete(oldest);
  }
  _cache.set(key, { result, expiresAt: Date.now() + CACHE_TTL_MS });
}

/**
 * Get cache stats for debugging/monitoring.
 * @returns {{size: number, maxSize: number, ttl: number}}
 */
export function getCacheStats() {
  return {
    size: _cache.size,
    maxSize: MAX_CACHE_ENTRIES,
    ttl: CACHE_TTL_MS,
  };
}

/**
 * Clear the entire cache.
 */
export function clearCache() {
  _cache.clear();
}
