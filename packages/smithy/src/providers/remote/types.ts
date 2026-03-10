/**
 * Remote Agent Provider Types
 *
 * Configuration and protocol types for connecting to a remote agent node.
 * A remote agent node is a lightweight Hono server (see node-server.ts) that
 * runs on any network-accessible machine and executes agent sessions locally
 * on that machine.
 *
 * Authentication uses a shared API key passed in the `Authorization: Bearer`
 * header on every request.
 *
 * @module
 */

// ============================================================================
// Remote Node Configuration
// ============================================================================

/**
 * Configuration for connecting to a remote agent node from the control plane.
 * This is the client-side config used by `RemoteAgentProvider`.
 */
export interface RemoteNodeConnectionConfig {
  /**
   * Base URL of the remote agent node, e.g. `https://agent-node.example.com`
   * or `http://10.0.0.5:4000`.  No trailing slash.
   */
  readonly url: string;

  /**
   * API key for authenticating with the remote node.
   * Sent as `Authorization: Bearer <apiKey>` on every request.
   */
  readonly apiKey: string;

  /**
   * Agent provider to use on the remote machine.
   * Defaults to the remote node's default provider.
   * e.g. `'claude-code'`, `'opencode'`
   */
  readonly provider?: string;

  /**
   * HTTP request timeout in milliseconds for spawn and control operations.
   * Note: does NOT apply to the long-lived SSE event stream or WebSocket connection.
   * Default: 30 000 ms.
   */
  readonly timeout?: number;
}

// ============================================================================
// Remote Node HTTP API — Request/Response bodies
// ============================================================================

/** Body for POST /sessions/headless */
export interface RemoteHeadlessSpawnBody {
  readonly workingDirectory: string;
  readonly initialPrompt?: string;
  readonly resumeSessionId?: string;
  readonly environmentVariables?: Record<string, string>;
  readonly stoneforgeRoot?: string;
  readonly timeout?: number;
  readonly model?: string;
  /** Override the provider on the remote node */
  readonly provider?: string;
}

/** Body for POST /sessions/interactive */
export interface RemoteInteractiveSpawnBody {
  readonly workingDirectory: string;
  readonly initialPrompt?: string;
  readonly resumeSessionId?: string;
  readonly environmentVariables?: Record<string, string>;
  readonly stoneforgeRoot?: string;
  readonly cols?: number;
  readonly rows?: number;
  readonly model?: string;
  /** Override the provider on the remote node */
  readonly provider?: string;
}

/** Successful spawn response from the remote node */
export interface RemoteSpawnResponse {
  readonly sessionId: string;
}

/** Body for POST /sessions/headless/:id/message */
export interface RemoteMessageBody {
  readonly content: string;
}

/** Body for POST /sessions/interactive/:id/resize */
export interface RemoteResizeBody {
  readonly cols: number;
  readonly rows: number;
}

/** Health check response from the remote node */
export interface RemoteHealthResponse {
  readonly status: 'ok';
  readonly version: string;
  readonly providers: string[];
}

// ============================================================================
// Remote Node WebSocket Protocol (PTY)
// ============================================================================

/** Messages sent from the control-plane client to the remote node over WS */
export type RemotePtyClientMessage =
  | { readonly type: 'input'; readonly data: string }
  | { readonly type: 'resize'; readonly cols: number; readonly rows: number }
  | { readonly type: 'kill' };

/** Messages sent from the remote node to the control-plane client over WS */
export type RemotePtyServerMessage =
  | { readonly type: 'data'; readonly data: string }
  | { readonly type: 'exit'; readonly code: number; readonly signal?: number };

// ============================================================================
// Remote Node SSE Protocol (Headless Events)
// ============================================================================

/**
 * Server-Sent Events sent on GET /sessions/headless/:id/events
 *
 * Each SSE line looks like:
 *   event: message\ndata: <JSON of RemoteHeadlessEvent>\n\n
 */
export type RemoteHeadlessEvent =
  | { readonly type: 'message'; readonly data: unknown }
  | { readonly type: 'done' }
  | { readonly type: 'error'; readonly data: { readonly message: string } };

// ============================================================================
// Remote Node Constants
// ============================================================================

/** Current protocol version surfaced in health responses */
export const REMOTE_NODE_PROTOCOL_VERSION = '1.0';
