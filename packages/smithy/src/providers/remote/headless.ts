/**
 * Remote Headless Provider
 *
 * Implements HeadlessProvider by delegating to a remote agent node over
 * HTTP + Server-Sent Events (SSE).
 *
 * Session lifecycle:
 *   1. POST /sessions/headless → receive sessionId
 *   2. GET  /sessions/headless/:id/events → SSE stream of AgentMessage events
 *   3. POST /sessions/headless/:id/message → send follow-up messages
 *   4. DELETE /sessions/headless/:id → close/interrupt
 *
 * @module
 */

import type {
  HeadlessProvider,
  HeadlessSession,
  HeadlessSpawnOptions,
  AgentMessage,
} from '../types.js';
import { ProviderError } from '../types.js';
import type { RemoteNodeConnectionConfig, RemoteHeadlessSpawnBody } from './types.js';
// AsyncQueue is a push-to-pull bridge shared from the opencode provider utilities
import { AsyncQueue } from '../opencode/async-queue.js';

// ============================================================================
// Helpers
// ============================================================================

/** Parse Server-Sent Events from a stream of UTF-8 text chunks */
async function* parseSseStream(body: ReadableStream<Uint8Array>): AsyncGenerator<{ event: string; data: string }> {
  const decoder = new TextDecoder();
  let buffer = '';
  const reader = body.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      // SSE messages are separated by blank lines (\n\n)
      const parts = buffer.split('\n\n');
      buffer = parts.pop() ?? '';
      for (const raw of parts) {
        let eventName = 'message';
        let dataLine = '';
        for (const line of raw.trim().split('\n')) {
          if (line.startsWith('event: ')) {
            eventName = line.slice(7).trim();
          } else if (line.startsWith('data: ')) {
            dataLine = line.slice(6);
          }
        }
        if (dataLine) yield { event: eventName, data: dataLine };
      }
    }
  } finally {
    reader.releaseLock();
  }
}

// ============================================================================
// RemoteHeadlessSession
// ============================================================================

class RemoteHeadlessSession implements HeadlessSession {
  private readonly baseUrl: string;
  private readonly sessionId: string;
  private readonly headers: Record<string, string>;
  private readonly queue: AsyncQueue<AgentMessage>;
  private sseAbort: AbortController;
  private closed = false;

  constructor(
    baseUrl: string,
    sessionId: string,
    headers: Record<string, string>,
    queue: AsyncQueue<AgentMessage>,
    sseAbort: AbortController
  ) {
    this.baseUrl = baseUrl;
    this.sessionId = sessionId;
    this.headers = headers;
    this.queue = queue;
    this.sseAbort = sseAbort;
  }

  sendMessage(content: string): void {
    if (this.closed) return;

    fetch(`${this.baseUrl}/sessions/headless/${this.sessionId}/message`, {
      method: 'POST',
      headers: { ...this.headers, 'content-type': 'application/json' },
      body: JSON.stringify({ content }),
    }).catch(() => {
      // ignore fire-and-forget errors
    });
  }

  async interrupt(): Promise<void> {
    if (this.closed) return;
    await fetch(`${this.baseUrl}/sessions/headless/${this.sessionId}/interrupt`, {
      method: 'POST',
      headers: this.headers,
    }).catch(() => {});
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.sseAbort.abort();
    this.queue.close();

    fetch(`${this.baseUrl}/sessions/headless/${this.sessionId}`, {
      method: 'DELETE',
      headers: this.headers,
    }).catch(() => {});
  }

  [Symbol.asyncIterator](): AsyncIterator<AgentMessage> {
    return this.queue[Symbol.asyncIterator]();
  }
}

// ============================================================================
// RemoteHeadlessProvider
// ============================================================================

/**
 * Spawns headless agent sessions on a remote node via HTTP + SSE.
 */
export class RemoteHeadlessProvider implements HeadlessProvider {
  readonly name = 'remote-headless';
  private readonly config: RemoteNodeConnectionConfig;

  constructor(config: RemoteNodeConnectionConfig) {
    this.config = config;
  }

  async isAvailable(): Promise<boolean> {
    try {
      const res = await fetch(`${this.config.url}/health`, {
        headers: this.buildHeaders(),
        signal: AbortSignal.timeout(5000),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  async spawn(options: HeadlessSpawnOptions): Promise<HeadlessSession> {
    const headers = this.buildHeaders();
    const body: RemoteHeadlessSpawnBody = {
      workingDirectory: options.workingDirectory,
      initialPrompt: options.initialPrompt,
      resumeSessionId: options.resumeSessionId,
      environmentVariables: options.environmentVariables,
      stoneforgeRoot: options.stoneforgeRoot,
      timeout: options.timeout,
      model: options.model,
      provider: this.config.provider,
    };

    const spawnRes = await fetch(`${this.config.url}/sessions/headless`, {
      method: 'POST',
      headers: { ...headers, 'content-type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(this.config.timeout ?? 30_000),
    });

    if (!spawnRes.ok) {
      const text = await spawnRes.text().catch(() => '');
      throw new ProviderError(
        `Remote node returned ${spawnRes.status} spawning headless session: ${text}`,
        'remote'
      );
    }

    const { sessionId } = (await spawnRes.json()) as { sessionId: string };

    // Set up the SSE stream
    const queue = new AsyncQueue<AgentMessage>();
    const sseAbort = new AbortController();

    // Fire-and-forget: stream events from the remote node into the queue
    this.streamEvents(sessionId, headers, queue, sseAbort).catch(() => {
      queue.close();
    });

    return new RemoteHeadlessSession(`${this.config.url}`, sessionId, headers, queue, sseAbort);
  }

  private async streamEvents(
    sessionId: string,
    headers: Record<string, string>,
    queue: AsyncQueue<AgentMessage>,
    abort: AbortController
  ): Promise<void> {
    const res = await fetch(`${this.config.url}/sessions/headless/${sessionId}/events`, {
      headers: { ...headers, accept: 'text/event-stream' },
      signal: abort.signal,
    });

    if (!res.ok || !res.body) {
      queue.close();
      return;
    }

    try {
      for await (const { event, data } of parseSseStream(res.body)) {
        if (event === 'done') {
          queue.close();
          return;
        }
        if (event === 'error') {
          const errData = JSON.parse(data) as { message: string };
          queue.push({
            type: 'error',
            content: errData.message,
            raw: errData,
          });
          queue.close();
          return;
        }
        // event === 'message'
        const parsed = JSON.parse(data) as AgentMessage;
        queue.push(parsed);
      }
    } finally {
      queue.close();
    }
  }

  private buildHeaders(): Record<string, string> {
    return {
      authorization: `Bearer ${this.config.apiKey}`,
    };
  }
}
