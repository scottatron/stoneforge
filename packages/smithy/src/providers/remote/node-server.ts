/**
 * Remote Agent Node Server
 *
 * A lightweight Hono HTTP server that runs on any network-accessible machine
 * and executes agent sessions using the locally-installed AI provider (Claude,
 * OpenCode, Codex, etc.).  The orchestrator control plane connects to this
 * server via the `RemoteAgentProvider`.
 *
 * ## API surface
 *
 * All routes require `Authorization: Bearer <apiKey>`.
 *
 * ### Headless sessions
 *   POST   /sessions/headless             Spawn a new headless session
 *   GET    /sessions/headless/:id/events  SSE stream of AgentMessage events
 *   POST   /sessions/headless/:id/message Send a follow-up user message
 *   POST   /sessions/headless/:id/interrupt Interrupt the current operation
 *   DELETE /sessions/headless/:id         Close/abort the session
 *
 * ### Interactive sessions
 *   POST   /sessions/interactive          Spawn a new interactive (PTY) session
 *   WS     /sessions/interactive/:id/pty  WebSocket for PTY I/O
 *   POST   /sessions/interactive/:id/resize Resize the terminal
 *   DELETE /sessions/interactive/:id      Kill the session
 *
 * ### Utilities
 *   GET    /health                        Health check
 *
 * ## Usage
 *
 * ```typescript
 * import { createRemoteAgentNode } from '@stoneforge/smithy/providers';
 *
 * const node = createRemoteAgentNode({
 *   apiKey: process.env.REMOTE_NODE_API_KEY!,
 *   provider: 'claude-code',   // default provider on this machine
 * });
 *
 * // Start listening on port 4000 (returns the Bun server handle)
 * const server = node.start(4000);
 * ```
 *
 * @module
 */

import { Hono } from 'hono';
import type { AgentMessage } from '../types.js';
import { getProviderRegistry } from '../registry.js';
import { REMOTE_NODE_PROTOCOL_VERSION } from './types.js';
import type { RemotePtyClientMessage, RemotePtyServerMessage } from './types.js';

// ============================================================================
// Node Configuration
// ============================================================================

/**
 * Configuration for the remote agent node server.
 */
export interface RemoteAgentNodeConfig {
  /**
   * API key used to authenticate incoming requests.
   * Clients must send `Authorization: Bearer <apiKey>`.
   */
  readonly apiKey: string;

  /**
   * Default provider to use when spawning sessions (e.g. `'claude-code'`).
   * Can be overridden per-request.
   */
  readonly provider?: string;
}

// ============================================================================
// In-memory session stores
// ============================================================================

interface HeadlessSessionEntry {
  iterator: AsyncIterator<AgentMessage>;
  sendMessage: (content: string) => void;
  interrupt: () => Promise<void>;
  close: () => void;
  subscribers: Array<ReadableStreamDefaultController<Uint8Array>>;
  done: boolean;
}

interface InteractiveSessionEntry {
  write: (data: string) => void;
  kill: () => void;
  resize: (cols: number, rows: number) => void;
  /** Active PTY WebSocket (Bun ServerWebSocket) */
  ws: { send(data: string): void; close(code?: number, reason?: string): void } | null;
}

const headlessSessions = new Map<string, HeadlessSessionEntry>();
const interactiveSessions = new Map<string, InteractiveSessionEntry>();

function generateSessionId(): string {
  // 'rs' = remote session
  return `rs-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

// ============================================================================
// Helper: broadcast an SSE event to all SSE subscribers
// ============================================================================

function broadcastSse(entry: HeadlessSessionEntry, event: string, data: unknown): void {
  const payload = new TextEncoder().encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  for (const controller of entry.subscribers) {
    try {
      controller.enqueue(payload);
    } catch {
      // subscriber disconnected
    }
  }
}

// ============================================================================
// createRemoteAgentNode
// ============================================================================

/**
 * Creates and configures the remote agent node Hono application.
 * Call `.start(port)` to begin listening.
 */
export function createRemoteAgentNode(nodeConfig: RemoteAgentNodeConfig): {
  app: Hono;
  start: (port?: number) => ReturnType<typeof Bun.serve>;
} {
  const app = new Hono();
  const registry = getProviderRegistry();

  // ── Auth middleware ────────────────────────────────────────────────────────

  app.use('*', async (c, next) => {
    // WebSocket upgrades are authenticated in the websocket.open handler
    const upgrade = c.req.header('upgrade');
    if (upgrade?.toLowerCase() === 'websocket') {
      return next();
    }

    const authHeader = c.req.header('authorization') ?? '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
    if (token !== nodeConfig.apiKey) {
      return c.json({ error: 'Unauthorized' }, 401);
    }
    return next();
  });

  // ── GET /health ────────────────────────────────────────────────────────────

  app.get('/health', (c) => {
    return c.json({
      status: 'ok',
      version: REMOTE_NODE_PROTOCOL_VERSION,
      providers: registry.list(),
    });
  });

  // ── POST /sessions/headless ────────────────────────────────────────────────

  app.post('/sessions/headless', async (c) => {
    const body = await c.req.json();
    const providerName = (body.provider as string | undefined) ?? nodeConfig.provider;
    const provider = providerName ? registry.get(providerName) : registry.getDefault();
    if (!provider) {
      return c.json({ error: `Provider '${providerName}' not found` }, 404);
    }

    const session = await provider.headless.spawn({
      workingDirectory: body.workingDirectory,
      initialPrompt: body.initialPrompt,
      resumeSessionId: body.resumeSessionId,
      environmentVariables: body.environmentVariables,
      stoneforgeRoot: body.stoneforgeRoot,
      timeout: body.timeout,
      model: body.model,
    });

    const sessionId = generateSessionId();
    const entry: HeadlessSessionEntry = {
      iterator: session[Symbol.asyncIterator](),
      sendMessage: (content) => session.sendMessage(content),
      interrupt: () => session.interrupt(),
      close: () => session.close(),
      subscribers: [],
      done: false,
    };
    headlessSessions.set(sessionId, entry);

    // Drain the session iterator in the background, pushing events to SSE subscribers
    void drainHeadlessSession(sessionId, entry);

    return c.json({ sessionId }, 201);
  });

  async function drainHeadlessSession(sessionId: string, entry: HeadlessSessionEntry): Promise<void> {
    try {
      while (true) {
        const { value, done } = await entry.iterator.next();
        if (done) break;
        broadcastSse(entry, 'message', value);
      }
    } catch (err) {
      broadcastSse(entry, 'error', { message: String(err) });
    } finally {
      entry.done = true;
      broadcastSse(entry, 'done', {});
      for (const controller of entry.subscribers) {
        try { controller.close(); } catch { /* already closed */ }
      }
      entry.subscribers.splice(0);
      headlessSessions.delete(sessionId);
    }
  }

  // ── GET /sessions/headless/:id/events ──────────────────────────────────────

  app.get('/sessions/headless/:id/events', (c) => {
    const entry = headlessSessions.get(c.req.param('id'));
    if (!entry) {
      return c.json({ error: 'Session not found' }, 404);
    }

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        if (entry.done) {
          controller.enqueue(new TextEncoder().encode('event: done\ndata: {}\n\n'));
          controller.close();
          return;
        }
        entry.subscribers.push(controller);
      },
      cancel(controller) {
        const idx = entry.subscribers.indexOf(controller as ReadableStreamDefaultController<Uint8Array>);
        if (idx !== -1) entry.subscribers.splice(idx, 1);
      },
    });

    return new Response(stream, {
      headers: {
        'content-type': 'text/event-stream',
        'cache-control': 'no-cache',
        connection: 'keep-alive',
      },
    });
  });

  // ── POST /sessions/headless/:id/message ───────────────────────────────────

  app.post('/sessions/headless/:id/message', async (c) => {
    const entry = headlessSessions.get(c.req.param('id'));
    if (!entry) return c.json({ error: 'Session not found' }, 404);
    const body = await c.req.json();
    entry.sendMessage(body.content);
    return c.json({ success: true });
  });

  // ── POST /sessions/headless/:id/interrupt ─────────────────────────────────

  app.post('/sessions/headless/:id/interrupt', async (c) => {
    const entry = headlessSessions.get(c.req.param('id'));
    if (!entry) return c.json({ error: 'Session not found' }, 404);
    await entry.interrupt();
    return c.json({ success: true });
  });

  // ── DELETE /sessions/headless/:id ─────────────────────────────────────────

  app.delete('/sessions/headless/:id', (c) => {
    const id = c.req.param('id');
    const entry = headlessSessions.get(id);
    if (!entry) return c.json({ error: 'Session not found' }, 404);
    entry.close();
    headlessSessions.delete(id);
    return c.json({ success: true });
  });

  // ── POST /sessions/interactive ─────────────────────────────────────────────

  app.post('/sessions/interactive', async (c) => {
    const body = await c.req.json();
    const providerName = (body.provider as string | undefined) ?? nodeConfig.provider;
    const provider = providerName ? registry.get(providerName) : registry.getDefault();
    if (!provider) {
      return c.json({ error: `Provider '${providerName}' not found` }, 404);
    }

    const session = await provider.interactive.spawn({
      workingDirectory: body.workingDirectory,
      initialPrompt: body.initialPrompt,
      resumeSessionId: body.resumeSessionId,
      environmentVariables: body.environmentVariables,
      stoneforgeRoot: body.stoneforgeRoot,
      cols: body.cols,
      rows: body.rows,
      model: body.model,
    });

    const sessionId = generateSessionId();
    const entry: InteractiveSessionEntry = {
      write: (data) => session.write(data),
      kill: () => session.kill(),
      resize: (cols, rows) => session.resize(cols, rows),
      ws: null,
    };

    session.onData((data) => {
      if (entry.ws) {
        const msg: RemotePtyServerMessage = { type: 'data', data };
        entry.ws.send(JSON.stringify(msg));
      }
    });

    session.onExit((code, signal) => {
      if (entry.ws) {
        const msg: RemotePtyServerMessage = { type: 'exit', code, signal };
        entry.ws.send(JSON.stringify(msg));
        entry.ws.close();
      }
      interactiveSessions.delete(sessionId);
    });

    interactiveSessions.set(sessionId, entry);
    return c.json({ sessionId }, 201);
  });

  // ── GET /sessions/interactive/:id/pty (WebSocket upgrade) ─────────────────
  // The actual upgrade is handled in Bun.serve.fetch below.
  // This route is here only for documentation purposes.
  app.get('/sessions/interactive/:id/pty', (c) => {
    return c.text('Use WebSocket connection', 426);
  });

  // ── POST /sessions/interactive/:id/resize ─────────────────────────────────

  app.post('/sessions/interactive/:id/resize', async (c) => {
    const entry = interactiveSessions.get(c.req.param('id'));
    if (!entry) return c.json({ error: 'Session not found' }, 404);
    const body = await c.req.json();
    entry.resize(body.cols, body.rows);
    return c.json({ success: true });
  });

  // ── DELETE /sessions/interactive/:id ──────────────────────────────────────

  app.delete('/sessions/interactive/:id', (c) => {
    const id = c.req.param('id');
    const entry = interactiveSessions.get(id);
    if (!entry) return c.json({ error: 'Session not found' }, 404);
    entry.kill();
    interactiveSessions.delete(id);
    return c.json({ success: true });
  });

  // ── start() ───────────────────────────────────────────────────────────────

  /**
   * Start the remote agent node server on the given port.
   * Returns the Bun server instance so callers can call `.stop()`.
   */
  function start(port = 4000): ReturnType<typeof Bun.serve> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const BunRuntime = (globalThis as any).Bun as typeof import('bun') | undefined;
    if (!BunRuntime) {
      throw new Error(
        'RemoteAgentNode.start() requires the Bun runtime. ' +
        'Run this server with `bun run` instead of `node`.'
      );
    }

    type BunServerWebSocket = { data: { sessionId: string }; send(msg: string): void; close(code?: number, reason?: string): void };

    return BunRuntime.serve({
      port,
      async fetch(req: Request, server: { upgrade(req: Request, opts: { data: unknown }): boolean }) {
        const url = new URL(req.url);

        // Handle WebSocket upgrades for PTY paths
        const ptyMatch = url.pathname.match(/^\/sessions\/interactive\/([^/]+)\/pty$/);
        if (ptyMatch && req.headers.get('upgrade')?.toLowerCase() === 'websocket') {
          const sessionId = ptyMatch[1];
          const token = url.searchParams.get('token') ?? '';
          if (token !== nodeConfig.apiKey) {
            return new Response('Unauthorized', { status: 401 });
          }
          const entry = interactiveSessions.get(sessionId);
          if (!entry) {
            return new Response('Session not found', { status: 404 });
          }
          const upgraded = server.upgrade(req, { data: { sessionId } });
          if (upgraded) return undefined as unknown as Response;
          return new Response('WebSocket upgrade failed', { status: 500 });
        }

        // Delegate all other requests to Hono
        return app.fetch(req);
      },
      websocket: {
        open(ws: BunServerWebSocket) {
          const { sessionId } = ws.data;
          const entry = interactiveSessions.get(sessionId);
          if (!entry) {
            ws.close(4004, 'Session not found');
            return;
          }
          entry.ws = ws;
        },
        message(ws: BunServerWebSocket, message: string) {
          const { sessionId } = ws.data;
          const entry = interactiveSessions.get(sessionId);
          if (!entry) return;
          try {
            const msg = JSON.parse(message) as RemotePtyClientMessage;
            if (msg.type === 'input') {
              entry.write(msg.data);
            } else if (msg.type === 'resize') {
              entry.resize(msg.cols, msg.rows);
            } else if (msg.type === 'kill') {
              entry.kill();
            }
          } catch {
            // ignore malformed messages
          }
        },
        close(ws: BunServerWebSocket) {
          const { sessionId } = ws.data;
          const entry = interactiveSessions.get(sessionId);
          if (entry) entry.ws = null;
        },
      },
    });
  }

  return { app, start };
}
