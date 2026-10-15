import { encodeBatch } from './encode_commands';

/** What the write of commands needs from its client. */
export interface WriteHost {
  hasTransport(): boolean;
  /** Encodes commands for the transport. Throws when the codec refuses them. */
  encode(commands: any[]): any;
  /** Writes encoded commands on the transport of the session and node. Throws when the transport refuses the write. */
  send(data: any, session: string, node: string): void;
  debug(message: string, ...args: any[]): void;
  /** Settles the calls of the commands with a write error, refused ones with a refusal. */
  handleWriteError(commands: any[], refused: boolean): void;
  /** Whether the failed write of these commands ends the connection: a tracked call or a cancel was in them. */
  writeFailed(commands: any[]): boolean;
  /** Ends the connection after a failed write, for a reconnect. */
  disconnectWriteFailed(): void;
}


/**
 * Writes commands on the transport of a client, and returns whether they were sent. A command the codec
 * refuses is refused alone, and the others of the write are sent (see encodeBatch). A transport that refuses
 * the write fails its commands, and ends the connection when they include a tracked call or a cancel.
 */
export function writeCommands(host: WriteHost, commands: any[], session: string = '', node: string = ''): boolean {
  if (!commands.length) {
    return true;
  }
  if (!host.hasTransport()) {
    return false;
  }
  const batch = encodeBatch((items: any[]): any => host.encode(items), commands);
  host.handleWriteError(batch.refused, true);
  if (batch.data === null) {
    return false;
  }
  try {
    host.send(batch.data, session, node);
  } catch (e) {
    host.debug('error writing commands', e);
    host.handleWriteError(batch.encodable, false);
    if (host.writeFailed(batch.encodable)) {
      host.disconnectWriteFailed();
    }
    return false;
  }
  return true;
}
