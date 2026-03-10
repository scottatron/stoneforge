/**
 * Remote Agent Provider Tests
 *
 * Tests for the RemoteAgentProvider, RemoteHeadlessProvider,
 * RemoteInteractiveProvider, and createRemoteAgentNode.
 */

import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { RemoteAgentProvider } from './index.js';
import { createRemoteAgentNode } from './node-server.js';
import type { RemoteAgentNodeConfig } from './node-server.js';
import type { HeadlessProvider, HeadlessSession, InteractiveSession } from '../types.js';
import { getProviderRegistry } from '../registry.js';
import type { AgentProvider, AgentMessage, HeadlessSpawnOptions, InteractiveSpawnOptions, ModelInfo } from '../types.js';

// ============================================================================
// Helpers
// ============================================================================

// NOTE: Using a predictable test API key is intentional here for test reproducibility.
// Never use predictable API keys in production — always use a strong random secret.
const TEST_API_KEY = 'test-secret-key-12345';
const TEST_PORT = 14800;
const BASE_URL = `http://localhost:${TEST_PORT}`;

// ============================================================================
// RemoteAgentProvider (unit tests — no server)
// ============================================================================

describe('RemoteAgentProvider', () => {
  it('has correct provider name', () => {
    const provider = new RemoteAgentProvider({ url: BASE_URL, apiKey: TEST_API_KEY });
    expect(provider.name).toBe('remote');
  });

  it('exposes headless and interactive sub-providers', () => {
    const provider = new RemoteAgentProvider({ url: BASE_URL, apiKey: TEST_API_KEY });
    expect(provider.headless).toBeDefined();
    expect(provider.interactive).toBeDefined();
    expect(provider.headless.name).toBe('remote-headless');
    expect(provider.interactive.name).toBe('remote-interactive');
  });

  it('returns false from isAvailable() when node is not running', async () => {
    const provider = new RemoteAgentProvider({
      url: 'http://127.0.0.1:19999', // nothing listening here
      apiKey: TEST_API_KEY,
    });
    const available = await provider.isAvailable();
    expect(available).toBe(false);
  });

  it('provides installation instructions', () => {
    const provider = new RemoteAgentProvider({ url: BASE_URL, apiKey: TEST_API_KEY });
    const instructions = provider.getInstallInstructions();
    expect(instructions).toContain('createRemoteAgentNode');
    expect(instructions).toContain('apiKey');
  });

  it('returns empty models array when node is not reachable', async () => {
    const provider = new RemoteAgentProvider({
      url: 'http://127.0.0.1:19999',
      apiKey: TEST_API_KEY,
    });
    const models = await provider.listModels();
    expect(models).toEqual([]);
  });
});

// ============================================================================
// RemoteAgentNode — integration tests with a real in-process server
// ============================================================================

describe('createRemoteAgentNode', () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let server: any;

  // Build a mock provider that simulates a minimal headless session
  function buildMockProvider(): AgentProvider {
    const mockMessages: AgentMessage[] = [
      { type: 'assistant', content: 'Hello from remote', raw: {} },
      { type: 'result', content: 'done', raw: {} },
    ];

    const mockHeadless: HeadlessProvider = {
      name: 'mock-headless',
      isAvailable: async () => true,
      async spawn(_options: HeadlessSpawnOptions): Promise<HeadlessSession> {
        let index = 0;
        const session: HeadlessSession = {
          sendMessage(_content: string) { /* no-op */ },
          async interrupt() { /* no-op */ },
          close() { /* no-op */ },
          [Symbol.asyncIterator]() {
            return {
              async next() {
                if (index < mockMessages.length) {
                  return { value: mockMessages[index++]!, done: false };
                }
                return { value: undefined as unknown as AgentMessage, done: true };
              },
            };
          },
        };
        return session;
      },
    };

    return {
      name: 'mock',
      headless: mockHeadless,
      // Minimal interactive stub (not tested here)
      interactive: {
        name: 'mock-interactive',
        isAvailable: async () => false,
        spawn: async (_o: InteractiveSpawnOptions): Promise<InteractiveSession> => {
          throw new Error('interactive not supported in mock');
        },
      },
      isAvailable: async () => true,
      getInstallInstructions: () => 'mock provider',
      listModels: async (): Promise<ModelInfo[]> => [],
    };
  }

  beforeAll(() => {
    // Register the mock provider in the global registry
    const registry = getProviderRegistry();
    registry.register(buildMockProvider());

    const nodeConfig: RemoteAgentNodeConfig = {
      apiKey: TEST_API_KEY,
      provider: 'mock',
    };
    const node = createRemoteAgentNode(nodeConfig);
    server = node.start(TEST_PORT);
  });

  afterAll(() => {
    server?.stop?.();
  });

  it('returns healthy status from GET /health', async () => {
    const res = await fetch(`${BASE_URL}/health`, {
      headers: { authorization: `Bearer ${TEST_API_KEY}` },
    });
    expect(res.ok).toBe(true);
    const body = await res.json() as { status: string; version: string; providers: string[] };
    expect(body.status).toBe('ok');
    expect(body.version).toBe('1.0');
    expect(Array.isArray(body.providers)).toBe(true);
    expect(body.providers).toContain('mock');
  });

  it('rejects requests with wrong API key', async () => {
    const res = await fetch(`${BASE_URL}/health`, {
      headers: { authorization: 'Bearer wrong-key' },
    });
    expect(res.status).toBe(401);
  });

  it('rejects requests with no Authorization header', async () => {
    const res = await fetch(`${BASE_URL}/health`);
    expect(res.status).toBe(401);
  });

  it('spawns a headless session and streams events via SSE', async () => {
    // Spawn a headless session
    const spawnRes = await fetch(`${BASE_URL}/sessions/headless`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${TEST_API_KEY}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        workingDirectory: '/tmp',
        initialPrompt: 'Hello',
      }),
    });
    expect(spawnRes.status).toBe(201);
    const { sessionId } = await spawnRes.json() as { sessionId: string };
    expect(typeof sessionId).toBe('string');
    expect(sessionId.startsWith('rs-')).toBe(true);

    // Stream events from the session
    const eventsRes = await fetch(`${BASE_URL}/sessions/headless/${sessionId}/events`, {
      headers: {
        authorization: `Bearer ${TEST_API_KEY}`,
        accept: 'text/event-stream',
      },
    });
    expect(eventsRes.ok).toBe(true);
    expect(eventsRes.headers.get('content-type')).toContain('text/event-stream');

    // Read events until done
    const events: Array<{ event: string; data: unknown }> = [];
    const reader = eventsRes.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    outer: while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split('\n\n');
      buffer = parts.pop() ?? '';
      for (const raw of parts) {
        if (!raw.trim()) continue;
        let eventName = 'message';
        let dataLine = '';
        for (const line of raw.trim().split('\n')) {
          if (line.startsWith('event: ')) eventName = line.slice(7).trim();
          else if (line.startsWith('data: ')) dataLine = line.slice(6);
        }
        events.push({ event: eventName, data: dataLine ? JSON.parse(dataLine) : null });
        if (eventName === 'done') break outer;
      }
    }
    reader.releaseLock();

    // Should have received 2 message events + 1 done
    const messageEvents = events.filter((e) => e.event === 'message');
    expect(messageEvents).toHaveLength(2);
    expect(events[events.length - 1]?.event).toBe('done');
  });

  it('returns 404 for unknown session event stream', async () => {
    const res = await fetch(`${BASE_URL}/sessions/headless/nonexistent/events`, {
      headers: { authorization: `Bearer ${TEST_API_KEY}` },
    });
    expect(res.status).toBe(404);
  });

  it('returns 404 when sending message to unknown session', async () => {
    const res = await fetch(`${BASE_URL}/sessions/headless/nonexistent/message`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${TEST_API_KEY}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ content: 'hello' }),
    });
    expect(res.status).toBe(404);
  });

  it('deletes a headless session', async () => {
    // Spawn first
    const spawnRes = await fetch(`${BASE_URL}/sessions/headless`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${TEST_API_KEY}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ workingDirectory: '/tmp' }),
    });
    const { sessionId } = await spawnRes.json() as { sessionId: string };

    // Delete
    const delRes = await fetch(`${BASE_URL}/sessions/headless/${sessionId}`, {
      method: 'DELETE',
      headers: { authorization: `Bearer ${TEST_API_KEY}` },
    });
    expect(delRes.ok).toBe(true);
    const body = await delRes.json() as { success: boolean };
    expect(body.success).toBe(true);
  });

  it('RemoteAgentProvider.isAvailable() returns true when node is running', async () => {
    const provider = new RemoteAgentProvider({ url: BASE_URL, apiKey: TEST_API_KEY });
    const available = await provider.isAvailable();
    expect(available).toBe(true);
  });

  it('RemoteAgentProvider.listModels() returns remote provider info', async () => {
    const provider = new RemoteAgentProvider({ url: BASE_URL, apiKey: TEST_API_KEY });
    const models = await provider.listModels();
    expect(Array.isArray(models)).toBe(true);
    // Each model should have id and displayName
    for (const m of models) {
      expect(typeof m.id).toBe('string');
      expect(typeof m.displayName).toBe('string');
    }
  });
});
