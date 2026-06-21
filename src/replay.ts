/**
 * Replay-Bypass — a side-effect guard for DLQ replay (ADR-0027).
 *
 * A deliberate replay off the dead-letter queue ({@link redrive}) re-runs the handler, and the
 * handler's external side-effects re-fire: a second charge, a duplicate email. {@link Wrap}
 * (idempotency, ADR-0022) stops an *accidental* duplicate; it does not stop the *intended*
 * reprocess from re-firing effects that already happened. This closes that gap.
 *
 * The marker that says "this is a replay, skip the external effects" rides **out of band** as the
 * {@link HEADER_REPLAY_BYPASS} transport header on the {@link HeaderCarrier} — never in the frozen
 * envelope (GR-1, `schema_version` stays 1). {@link redrive} (with `bypass`) stamps it when the
 * transport can carry headers; a consume adapter, having reserved the message with its headers,
 * passes them to the guard so the handler can skip effects that already fired:
 *
 * ```ts
 * import { isReplay, bypassExternalEffects } from "@babelqueue/core";
 *
 * // CONSUMER: the adapter surfaces the delivered message's out-of-band headers.
 * async function onMessage(env: Envelope, headers: HeaderCarrier): Promise<void> {
 *   saveOrder(env);                                    // idempotent core — always runs
 *   await bypassExternalEffects(headers, async () => { // external effect — skipped on replay
 *     await sendConfirmationEmail(env);
 *   });
 * }
 * ```
 *
 * The Node core is codec-only (no runtime / no transport), mirroring the Go reference
 * `babelqueue-go/replay.go` and the Java `com.babelqueue.Replay`. Go/Java thread the flag through
 * an ambient scope (a `context.Context` / a `ThreadLocal`); Node has no ambient request scope, so
 * the delivered {@link HeaderCarrier} is passed explicitly — the same seam `otel.wrapHandler`
 * already uses for delivered headers. A concrete broker transport carries the header once it
 * implements the {@link RedriveHeaderIO} capability (the per-adapter rollout, like ADR-0028).
 */

import type { HeaderCarrier } from "./contracts.js";

/**
 * The out-of-band transport header {@link redrive} (with `bypass`) stamps on a replayed message,
 * and that a consume adapter surfaces to the handler via {@link isReplay} /
 * {@link bypassExternalEffects}. It rides on the {@link HeaderCarrier} beside the frozen envelope,
 * never inside it (GR-1) — the same seam as the `traceparent` header (ADR-0028).
 */
export const HEADER_REPLAY_BYPASS = "bq-replay-bypass";

/**
 * Reports whether a delivered message was redriven with the replay-bypass marker — i.e. a
 * deliberate replay whose external side-effects should be skipped. Reads the presence of
 * {@link HEADER_REPLAY_BYPASS} on the message's out-of-band `headers` (nil/empty-safe: a
 * header-less delivery is never a replay).
 *
 * @param headers the delivered message's out-of-band transport headers (may be null/undefined)
 */
export function isReplay(headers: HeaderCarrier | null | undefined): boolean {
  return !!headers && headers[HEADER_REPLAY_BYPASS] !== undefined && headers[HEADER_REPLAY_BYPASS] !== "";
}

/**
 * Runs `effect` unless the delivered message is a {@link isReplay replay}, in which case it is
 * skipped and the returned promise resolves to `undefined`. Wrap the external, non-idempotent side
 * of a handler — sending an email, charging a card, calling a third party — so a replay re-runs the
 * idempotent core but does not re-fire effects that already happened.
 *
 * @param headers the delivered message's out-of-band transport headers (may be null/undefined)
 * @param effect  the external side-effect to run only when this is not a replay
 */
export async function bypassExternalEffects<T>(
  headers: HeaderCarrier | null | undefined,
  effect: () => T | Promise<T>,
): Promise<T | undefined> {
  if (isReplay(headers)) {
    return undefined;
  }
  return effect();
}
