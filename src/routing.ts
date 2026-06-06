/**
 * What a consumer does with a message whose URN has no registered handler.
 * Mirrors the constants in every other SDK core.
 */
export const UnknownUrnStrategy = {
  /** Surface an error; let the worker decide. */
  FAIL: "fail",
  /** Drop the message. */
  DELETE: "delete",
  /** Requeue for another consumer. */
  RELEASE: "release",
  /** Route to the dead-letter queue. */
  DEAD_LETTER: "dead_letter",
} as const;

export type UnknownUrnStrategy =
  (typeof UnknownUrnStrategy)[keyof typeof UnknownUrnStrategy];
