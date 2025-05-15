import { Message } from './Database';

/**
 * Queue processes Message instances in FIFO order with key-based locking.
 *
 * We rely on Message.key to prevent concurrent processing of messages sharing the same key.
 * Locked keys are tracked in the `inProgress` map.
 * - `Dequeue` skips any message whose key is currently in `inProgress`, returning only unlocked messages.
 * - `Confirm` removes the key from `inProgress` and deletes the message from the queue,
 *   allowing future messages with that key to be delivered.
 */
export class Queue {
  private messages: Message[];
  private inProgress: Map<
    string,
    {
      messageId: string;
      workerId: number;
    }
  >;

  constructor() {
    this.messages = [];
    this.inProgress = new Map();
  }

  Enqueue = (message: Message) => {
    this.messages.push(message);
  };

  Dequeue = (workerId: number): Message | undefined => {
    if (!this.messages.length) {
      return undefined;
    }

    const message = this.messages.find(msg => !this.inProgress.has(msg.key));

    if (!message) {
      return undefined;
    }

    this.putInProgress(workerId, message);

    return message;
  };

  Confirm = (workerId: number, messageId: string) => {
    for (const [messageKey, message] of this.inProgress.entries()) {
      if (message.messageId === messageId && message.workerId === workerId) {
        this.inProgress.delete(messageKey);
        this.removeFromQueue(messageId);
      }
    }
  };

  Size = () => {
    return this.messages.length;
  };

  private putInProgress(workerId: number, message: Message) {
    this.inProgress.set(message.key, {
      messageId: message.id,
      workerId,
    });
  }

  private removeFromQueue(messageId: string) {
    const idx = this.messages.findIndex(msg => msg.id === messageId);
    if (idx !== -1) {
      this.messages.splice(idx, 1);
    }
  }
}
