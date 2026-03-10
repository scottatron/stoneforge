/**
 * Remote Interactive Provider
 *
 * Implements InteractiveProvider by delegating to a remote agent node over
 * HTTP + WebSocket (for PTY data).
 *
 * Session lifecycle:
 *   1. POST /sessions/interactive → receive sessionId
 *   2. WS   /sessions/interactive/:id/pty → bidirectional PTY stream
 *   3. POST /sessions/interactive/:id/resize → resize terminal
 *   4. DELETE /sessions/interactive/:id → kill session
 *
 * @module
 */

import type {
  InteractiveProvider,
  InteractiveSession,
  InteractiveSpawnOptions,
  ProviderSessionId,
} from '../types.js';
import { ProviderError } from '../types.js';
import type { RemoteNodeConnectionConfig, RemoteInteractiveSpawnBody, RemotePtyClientMessage, RemotePtyServerMessage } from './types.js';

// ============================================================================
// RemoteInteractiveSession
// ============================================================================

/** WebSocket close code for a normal (clean) close */
const WS_CLOSE_NORMAL = 1000;

class RemoteInteractiveSession implements InteractiveSession {
  readonly pid?: number;  // PIDs are local to the remote host and not surfaced to the client

  private readonly baseUrl: string;
  private readonly sessionId: string;
  private readonly headers: Record<string, string>;
  private readonly ws: WebSocket;
  private closed = false;

  constructor(
    baseUrl: string,
    sessionId: string,
    headers: Record<string, string>,
    ws: WebSocket
  ) {
    this.baseUrl = baseUrl;
    this.sessionId = sessionId;
    this.headers = headers;
    this.ws = ws;
  }

  write(data: string): void {
    if (this.closed || this.ws.readyState !== WebSocket.OPEN) return;
    const msg: RemotePtyClientMessage = { type: 'input', data };
    this.ws.send(JSON.stringify(msg));
  }

  resize(cols: number, rows: number): void {
    if (this.closed || this.ws.readyState !== WebSocket.OPEN) return;
    const msg: RemotePtyClientMessage = { type: 'resize', cols, rows };
    this.ws.send(JSON.stringify(msg));
  }

  kill(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.ws.readyState === WebSocket.OPEN) {
      const msg: RemotePtyClientMessage = { type: 'kill' };
      this.ws.send(JSON.stringify(msg));
      this.ws.close();
    }
    fetch(`${this.baseUrl}/sessions/interactive/${this.sessionId}`, {
      method: 'DELETE',
      headers: this.headers,
    }).catch(() => {});
  }

  onData(callback: (data: string) => void): void {
    this.ws.addEventListener('message', (event) => {
      try {
        const msg = JSON.parse(event.data as string) as RemotePtyServerMessage;
        if (msg.type === 'data') {
          callback(msg.data);
        }
      } catch {
        // ignore malformed frames
      }
    });
  }

  onExit(callback: (code: number, signal?: number) => void): void {
    this.ws.addEventListener('message', (event) => {
      try {
        const msg = JSON.parse(event.data as string) as RemotePtyServerMessage;
        if (msg.type === 'exit') {
          callback(msg.code, msg.signal);
        }
      } catch {
        // ignore malformed frames
      }
    });
    // Treat unexpected WS close as exit code -1
    this.ws.addEventListener('close', (event) => {
      if (!this.closed) {
        callback(event.code === WS_CLOSE_NORMAL ? 0 : -1);
      }
    });
  }

  getSessionId(): ProviderSessionId | undefined {
    return this.sessionId;
  }
}

// ============================================================================
// RemoteInteractiveProvider
// ============================================================================

/**
 * Spawns interactive PTY sessions on a remote agent node via HTTP + WebSocket.
 */
export class RemoteInteractiveProvider implements InteractiveProvider {
  readonly name = 'remote-interactive';
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

  async spawn(options: InteractiveSpawnOptions): Promise<InteractiveSession> {
    const headers = this.buildHeaders();
    const body: RemoteInteractiveSpawnBody = {
      workingDirectory: options.workingDirectory,
      initialPrompt: options.initialPrompt,
      resumeSessionId: options.resumeSessionId,
      environmentVariables: options.environmentVariables,
      stoneforgeRoot: options.stoneforgeRoot,
      cols: options.cols,
      rows: options.rows,
      model: options.model,
      provider: this.config.provider,
    };

    const spawnRes = await fetch(`${this.config.url}/sessions/interactive`, {
      method: 'POST',
      headers: { ...headers, 'content-type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(this.config.timeout ?? 30_000),
    });

    if (!spawnRes.ok) {
      const text = await spawnRes.text().catch(() => '');
      throw new ProviderError(
        `Remote node returned ${spawnRes.status} spawning interactive session: ${text}`,
        'remote'
      );
    }

    const { sessionId } = (await spawnRes.json()) as { sessionId: string };

    // Open WebSocket for PTY data (auth token passed as ?token= query param)
    const wsUrl = this.buildWsUrl(sessionId);
    const ws = new WebSocket(wsUrl);

    // Wait for the WebSocket to open before returning the session
    await new Promise<void>((resolve, reject) => {
      ws.addEventListener('open', () => resolve(), { once: true });
      ws.addEventListener('error', () => reject(new ProviderError('WebSocket connection to remote node failed', 'remote')), { once: true });
    });

    return new RemoteInteractiveSession(this.config.url, sessionId, headers, ws);
  }

  private buildWsUrl(sessionId: string): string {
    // Convert http(s) to ws(s) and append auth token as query param
    const wsBase = this.config.url
      .replace(/^https:\/\//, 'wss://')
      .replace(/^http:\/\//, 'ws://');
    const token = encodeURIComponent(this.config.apiKey);
    return `${wsBase}/sessions/interactive/${sessionId}/pty?token=${token}`;
  }

  private buildHeaders(): Record<string, string> {
    return {
      authorization: `Bearer ${this.config.apiKey}`,
    };
  }
}
