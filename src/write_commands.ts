/** What the write of commands needs from its client. */
export interface WriteHost {
  hasTransport(): boolean;
  /** Encodes commands for the transport. Throws when the codec refuses them. */
  encode(commands: any[]): any;
  /** Writes encoded commands on the transport of the session and node. Throws when the transport refuses the write. */
  send(data: any, session: string, node: string): void;
  debug(message: string, ...args: any[]): void;
  /** Settles the calls of the commands, which were not written, with a write error; refused ones with a refusal. */
  handleWriteError(commands: any[], refused: boolean): void;
  /** Whether the failed write of these commands ends the connection: a tracked call or a cancel was in them. */
  writeFailed(commands: any[]): boolean;
  /** Ends the connection after a failed write, for a reconnect. */
  disconnectWriteFailed(): void;
}

/** One frame of a write: its encoded data, and the commands it carries, in order. */
export interface WriteFrame {
  data: any;
  commands: any[];
}

/** The commands of one write, split by what the codec can encode. */
export interface EncodedBatch {
  // The frames to write, in order: the commands as one frame when the codec encodes them together, else one frame
  // per command that the codec encodes alone.
  frames: WriteFrame[];
  // The commands the codec refuses. They are not written, and each of them is refused on its own.
  refused: any[];
}


/**
 * Encodes the commands of one write. When the codec refuses the batch, each command is tried on its own, so a
 * command the codec cannot encode (e.g. a BigInt in its data) is refused alone, and the others are still written.
 * They go in one frame when the codec encodes them together; when it does not (a frame size limit), each of them
 * goes in a frame of its own.
 */
export function encodeBatch(encode: (commands: any[]) => any, commands: any[]): EncodedBatch {
  try {
    return { frames: [{ data: encode(commands), commands: commands }], refused: [] };
  } catch (e) {
    const alone: WriteFrame[] = [];
    const refused: any[] = [];
    for (const command of commands) {
      try {
        alone.push({ data: encode([command]), commands: [command] });
      } catch (refusal) {
        refused.push(command);
      }
    }
    return { frames: joinFrames(encode, alone), refused: refused };
  }
}


// The frames of commands the codec encodes alone: one frame of all of them when the codec encodes them together,
// else the frames as they are.
function joinFrames(encode: (commands: any[]) => any, alone: WriteFrame[]): WriteFrame[] {
  if (alone.length === 0) {
    return [];
  }
  const commands: any[] = alone.map((frame: WriteFrame): any => frame.commands[0]);
  try {
    return [{ data: encode(commands), commands: commands }];
  } catch (e) {
    return alone;
  }
}


/**
 * Writes commands on the transport of a client, and returns whether they were sent. A command the codec
 * refuses is refused alone, and the others of the write are sent (see encodeBatch). A transport that refuses
 * a write fails the commands of its frame and of the frames after it, and ends the connection when they include
 * a tracked call or a cancel.
 */
export function writeCommands(host: WriteHost, commands: any[], session: string = '', node: string = ''): boolean {
  if (!commands.length) {
    return true;
  }
  if (!host.hasTransport()) {
    return false;
  }
  const batch: EncodedBatch = encodeBatch((items: any[]): any => host.encode(items), commands);
  host.handleWriteError(batch.refused, true);
  for (let index: number = 0; index < batch.frames.length; index++) {
    const frame: WriteFrame = batch.frames[index];
    try {
      host.send(frame.data, session, node);
    } catch (e) {
      host.debug('error writing commands', e);
      const unwritten: any[] = [];
      for (const rest of batch.frames.slice(index)) {
        unwritten.push(...rest.commands);
      }
      host.handleWriteError(unwritten, false);
      if (host.writeFailed(unwritten)) {
        host.disconnectWriteFailed();
      }
      return false;
    }
  }
  return batch.frames.length > 0;
}
