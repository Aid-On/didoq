/**
 * Cloudflare Workers mock for testing
 */

export class DurableObject {
  protected ctx: DurableObjectState;
  protected env: Record<string, unknown>;

  constructor(state: DurableObjectState, env: Record<string, unknown>) {
    this.ctx = state;
    this.env = env;
  }

  fetch(_request: Request): Response | Promise<Response> {
    return new Response('Not implemented');
  }

  alarm?(): void | Promise<void>;
}
