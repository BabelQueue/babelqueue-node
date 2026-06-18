/**
 * Optional idempotency helper (ADR-0022): dedupe a consume handler on `meta.id`.
 *
 * The Node mirror of the PHP `BabelQueue\Idempotency` and Go `idempotency` helpers.
 * The core is codec-only (no dispatcher), so this wraps a user-provided handler that
 * an adapter (NestJS, BullMQ, ...) drives:
 *
 * ```ts
 * import { Wrap, InMemoryStore, type Handler } from "@babelqueue/core";
 *
 * const store = new InMemoryStore();
 * const handler = Wrap(store, async (env) => {  ...  });
 * ```
 *
 * A previously-seen id returns early (the adapter acks it); a throwing/rejecting
 * handler leaves the id unmarked so a redelivery runs it again; a message with no
 * usable `meta.id` runs unchanged. "Seen-set" post-success dedupe — not exactly-once,
 * not in-flight concurrency locking (a transactional mode is a future direction).
 */
import type { Envelope } from "./codec.js";

/** A consume handler: receives a decoded envelope, may be sync or async. */
export type Handler = (env: Envelope) => void | Promise<void>;

/**
 * A pluggable record of message ids already processed, keyed on `meta.id`. Methods may
 * be sync or async so a production store can be Redis- or DB-backed; the reference
 * {@link InMemoryStore} is synchronous.
 */
export interface Store {
  seen(messageId: string): boolean | Promise<boolean>;
  remember(messageId: string): void | Promise<void>;
  forget(messageId: string): void | Promise<void>;
}

/**
 * Process-local {@link Store} backed by a Set. For tests / single-process consumers;
 * not shared across workers and not persistent — use a Redis- or DB-backed store for
 * production fleets.
 */
export class InMemoryStore implements Store {
  private readonly entries = new Set<string>();

  seen(messageId: string): boolean {
    return this.entries.has(messageId);
  }

  remember(messageId: string): void {
    this.entries.add(messageId);
  }

  forget(messageId: string): void {
    this.entries.delete(messageId);
  }
}

/**
 * Wraps `handler` so a message whose `meta.id` was already processed successfully is
 * skipped. A thrown/rejected handler leaves the id unmarked, so a redelivery runs it
 * again (retry / dead-letter still apply); a message with no usable id runs unchanged.
 */
export function Wrap(store: Store, handler: Handler): Handler {
  return async (env: Envelope): Promise<void> => {
    const id = env.meta.id;

    // No usable id → cannot dedupe; run the handler unchanged.
    if (!id) {
      await handler(env);
      return;
    }

    // Already processed on an earlier delivery: return so the adapter acks it.
    if (await store.seen(id)) {
      return;
    }

    // First success wins; a throw here leaves the id unmarked → retry/DLQ apply.
    await handler(env);
    await store.remember(id);
  };
}
