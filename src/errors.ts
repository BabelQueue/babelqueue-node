/** Base error for all BabelQueue failures. */
export class BabelQueueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BabelQueueError";
  }
}

/** Raised when no handler is mapped for a message URN. */
export class UnknownUrnError extends BabelQueueError {
  constructor(urn: string) {
    super(`No handler is mapped for the message URN "${urn}".`);
    this.name = "UnknownUrnError";
  }
}
