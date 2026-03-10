/**
 * Remote Agent Provider
 *
 * Allows the orchestrator to spawn agent sessions on remote machines.
 * A remote machine must be running the `RemoteAgentNode` server
 * (see `node-server.ts`) which exposes the Remote Agent Node HTTP API.
 *
 * Usage:
 * ```typescript
 * import { RemoteAgentProvider } from '@stoneforge/smithy/providers';
 * import { getProviderRegistry } from '@stoneforge/smithy';
 *
 * const remote = new RemoteAgentProvider({
 *   url: 'https://agent-node.example.com',
 *   apiKey: process.env.REMOTE_NODE_API_KEY!,
 *   provider: 'claude-code',   // provider used on the remote machine
 * });
 *
 * // Register and set as default provider
 * const registry = getProviderRegistry();
 * registry.register(remote);
 * registry.setDefault('remote');
 * ```
 *
 * @module
 */

import type { AgentProvider, HeadlessProvider, InteractiveProvider, ModelInfo } from '../types.js';
import { RemoteHeadlessProvider } from './headless.js';
import { RemoteInteractiveProvider } from './interactive.js';
import type { RemoteNodeConnectionConfig, RemoteHealthResponse } from './types.js';

export { RemoteHeadlessProvider } from './headless.js';
export { RemoteInteractiveProvider } from './interactive.js';
export type { RemoteNodeConnectionConfig } from './types.js';

/**
 * Agent provider that delegates all session work to a remote agent node.
 *
 * The provider name is always `'remote'`.  Register multiple remote providers
 * under custom names if you need to target different remote nodes:
 *
 * ```typescript
 * class RemoteNodeEU extends RemoteAgentProvider {
 *   override readonly name = 'remote-eu';
 * }
 * ```
 */
export class RemoteAgentProvider implements AgentProvider {
  readonly name = 'remote';
  readonly headless: HeadlessProvider;
  readonly interactive: InteractiveProvider;
  private readonly config: RemoteNodeConnectionConfig;

  constructor(config: RemoteNodeConnectionConfig) {
    this.config = config;
    this.headless = new RemoteHeadlessProvider(config);
    this.interactive = new RemoteInteractiveProvider(config);
  }

  async isAvailable(): Promise<boolean> {
    try {
      const res = await fetch(`${this.config.url}/health`, {
        headers: { authorization: `Bearer ${this.config.apiKey}` },
        signal: AbortSignal.timeout(5000),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  getInstallInstructions(): string {
    return (
      'Start a RemoteAgentNode on the target machine:\n' +
      '  import { createRemoteAgentNode } from \'@stoneforge/smithy/providers\';\n' +
      '  const node = createRemoteAgentNode({ apiKey: \'<secret>\' });\n' +
      '  node.start(4000);\n\n' +
      'Then configure the RemoteAgentProvider with the node URL and API key.'
    );
  }

  async listModels(): Promise<ModelInfo[]> {
    try {
      const res = await fetch(`${this.config.url}/health`, {
        headers: { authorization: `Bearer ${this.config.apiKey}` },
        signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) return [];
      const health = (await res.json()) as RemoteHealthResponse;
      // Models are provider-specific; we surface the available providers instead
      return health.providers.map((p) => ({
        id: `remote/${p}`,
        displayName: `Remote: ${p}`,
        description: `Provider '${p}' available on ${this.config.url}`,
        providerName: 'remote',
      }));
    } catch {
      return [];
    }
  }
}
