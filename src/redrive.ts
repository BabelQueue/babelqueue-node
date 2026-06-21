/**
 * DLQ redrive tooling — safe replay off the dead-letter queue (ADR-0026).
 *
 * The Node mirror of the Go reference `babelqueue-go/redrive.go`. Because the Node core is
 * codec-only (no runtime / no transport), the orchestration takes a small {@link RedriveIO}
 * the caller implements over their transport — the same shape the optional `otel.publish`
 * helper used. {@link resetForRedrive} is the pure, transport-free core of it.
 *
 * A redriven message is **reset for reprocessing**: its `dead_letter` block is removed and
 * `attempts` reset to 0, while `job`, `trace_id`, `data` and `meta` are preserved verbatim, so
 * the replay is still fully traceable (same `trace_id`). The wire envelope is untouched (GR-1).
 *
 * Replay safety here is `dryRun` + `select` + redrive-to-`toQueue` (a sandbox). On top of those,
 * the **Replay-Bypass** guard (ADR-0027): with `opts.bypass`, a redriven message is stamped with
 * the `bq-replay-bypass` transport header — surfaced to the handler via {@link isReplay} /
 * {@link bypassExternalEffects} (see `./replay.js`) so a replay can skip external side-effects that
 * already fired. It rides the out-of-band {@link HeaderCarrier}, never the frozen envelope (GR-1),
 * and takes effect only when the {@link RedriveIO} can carry headers (its optional
 * {@link RedriveIO.publishWithHeaders}); otherwise it is a no-op and the per-item `bypassed` flag
 * stays `false`, exactly like the Go reference and the per-transport rollout of ADR-0028.
 */

import { EnvelopeCodec, type Envelope } from "./codec.js";
import type { HeaderCarrier } from "./contracts.js";
import { HEADER_REPLAY_BYPASS } from "./replay.js";

/** A message reserved from a queue, plus a way to acknowledge (remove) it. */
export interface RedriveMessage {
  body: string;
  ack(): Promise<void>;
}

/** The minimal transport surface {@link redrive} needs: reserve the next message, and publish. */
export interface RedriveIO {
  /** Reserve the next message from queue, or `null` when it is empty. */
  pop(queue: string): Promise<RedriveMessage | null>;
  /** Append an already-encoded body to queue. */
  publish(queue: string, body: string): Promise<void>;
  /**
   * Optional capability — publish a body together with out-of-band transport `headers` (e.g. the
   * `bq-replay-bypass` marker), for transports that carry per-message metadata on a native channel
   * (AMQP headers, SQS `MessageAttributes`, a Redis transport-owned frame). The Node counterpart of
   * Go's optional `HeaderPublisher`. A {@link RedriveIO} that omits it simply does not propagate
   * headers — {@link redrive} falls back to plain {@link RedriveIO.publish} and `bypass` is a no-op
   * (ADR-0027).
   */
  publishWithHeaders?(queue: string, body: string, headers: HeaderCarrier): Promise<void>;
}

/** Options for {@link redrive}. */
export interface RedriveOptions {
  /** Override where messages are re-published; default is each message's `dead_letter.original_queue`. Set a sandbox queue to replay safely. */
  toQueue?: string;
  /** Cap how many messages are pulled from the DLQ (0 / omitted = all currently available). */
  max?: number;
  /** Inspect without redriving: every message is read, reported, and returned to the DLQ unchanged. */
  dryRun?: boolean;
  /** Pick which messages to redrive (e.g. by reason or URN). Unselected messages are returned unchanged. */
  select?: (envelope: Envelope) => boolean;
  /**
   * Stamp the `bq-replay-bypass` transport header on each redriven message, so a handler can skip
   * external side-effects that already ran (see `./replay.js`). Takes effect only when the
   * {@link RedriveIO} implements {@link RedriveIO.publishWithHeaders}; otherwise it is a no-op and
   * the per-item `bypassed` flag stays `false` (ADR-0027).
   */
  bypass?: boolean;
}

/** What happened to one message during a {@link redrive} run. */
export interface RedriveItem {
  messageId: string;
  traceId: string;
  urn: string;
  reason: string;
  from: string;
  /** Target queue (the plan, even on a dry run; "" when skipped or undecodable). */
  to: string;
  /** True only when actually re-published to `to`. */
  redriven: boolean;
  /** True when the `bq-replay-bypass` header was stamped on the redriven message (ADR-0027). */
  bypassed: boolean;
}

/** Summary of a {@link redrive} run. */
export interface RedriveResult {
  redriven: number;
  skipped: number;
  items: RedriveItem[];
}

/**
 * Returns a copy of `envelope` reset for reprocessing: no `dead_letter` block and `attempts`
 * at 0, with `job`, `trace_id`, `data` and `meta` preserved verbatim. Pure — the input is not
 * mutated.
 */
export function resetForRedrive(envelope: Envelope): Envelope {
  return {
    job: envelope.job,
    trace_id: envelope.trace_id,
    data: envelope.data,
    meta: envelope.meta,
    attempts: 0,
  };
}

function sourceQueueOf(envelope: Envelope): string {
  return envelope.dead_letter?.original_queue || envelope.meta.queue;
}

/**
 * Moves dead-lettered messages off the `dlq` queue and re-publishes each — via {@link resetForRedrive} —
 * to its `dead_letter.original_queue` or `opts.toQueue`.
 *
 * Messages are drained from the DLQ first and then processed, so restored messages (skipped,
 * dry-run, or undecodable) are never re-encountered in the same run. A message is acknowledged
 * only after its re-publish succeeds; an undecodable body is restored, not dropped. On a publish
 * failure the message is restored to the DLQ and the error is re-thrown.
 */
export async function redrive(
  io: RedriveIO,
  dlq: string,
  opts: RedriveOptions = {},
): Promise<RedriveResult> {
  const max = opts.max ?? 0;

  interface Pending {
    message: RedriveMessage;
    envelope: Envelope | null;
  }
  const batch: Pending[] = [];
  while (max === 0 || batch.length < max) {
    const message = await io.pop(dlq);
    if (!message) {
      break;
    }
    const decoded = EnvelopeCodec.decode(message.body);
    batch.push({ message, envelope: EnvelopeCodec.accepts(decoded) ? decoded : null });
  }

  const result: RedriveResult = { redriven: 0, skipped: 0, items: [] };

  for (const { message, envelope } of batch) {
    if (!envelope) {
      await io.publish(dlq, message.body); // restore the undecodable body; never drop it
      await message.ack();
      result.skipped++;
      result.items.push({ messageId: "", traceId: "", urn: "", reason: "", from: dlq, to: "", redriven: false, bypassed: false });
      continue;
    }

    const item: RedriveItem = {
      messageId: envelope.meta.id,
      traceId: envelope.trace_id,
      urn: EnvelopeCodec.urn(envelope),
      reason: envelope.dead_letter?.reason ?? "",
      from: dlq,
      to: "",
      redriven: false,
      bypassed: false,
    };

    if (opts.select && !opts.select(envelope)) {
      await io.publish(dlq, message.body); // not selected: restore unchanged
      await message.ack();
      result.skipped++;
      result.items.push(item);
      continue;
    }

    const target = opts.toQueue ?? sourceQueueOf(envelope);
    item.to = target;

    if (opts.dryRun) {
      await io.publish(dlq, message.body); // report the plan; restore unchanged
      await message.ack();
      result.skipped++;
      result.items.push(item);
      continue;
    }

    const body = EnvelopeCodec.encode(resetForRedrive(envelope));
    let bypassed: boolean;
    try {
      bypassed = await publishRedriven(io, target, body, opts.bypass ?? false);
    } catch (err) {
      await io.publish(dlq, message.body); // restore on a publish failure
      await message.ack();
      throw err;
    }
    await message.ack();
    item.redriven = true;
    item.bypassed = bypassed;
    result.redriven++;
    result.items.push(item);
  }

  return result;
}

/**
 * Re-publishes a reset `body` to `queue`. When `bypass` is set and the {@link RedriveIO} can carry
 * headers ({@link RedriveIO.publishWithHeaders}), it stamps the `bq-replay-bypass` header and
 * reports `true`; otherwise it publishes plainly and reports `false` (ADR-0027).
 */
async function publishRedriven(io: RedriveIO, queue: string, body: string, bypass: boolean): Promise<boolean> {
  if (bypass && io.publishWithHeaders) {
    await io.publishWithHeaders(queue, body, { [HEADER_REPLAY_BYPASS]: "1" });
    return true;
  }
  await io.publish(queue, body);
  return false;
}
