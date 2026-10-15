import { WriteHost, writeCommands } from './write_commands';

// The write of commands against a host that records what it sends and settles.
class RecordingHost implements WriteHost {
  transport: boolean = true;
  sent: any[] = [];
  failWrite: boolean = false;
  handled: Array<{ commands: any[]; refused: boolean }> = [];
  disconnects: number = 0;
  writeFailedAnswer: boolean = true;

  hasTransport(): boolean {
    return this.transport;
  }

  // The codec refuses a command whose id is a BigInt, as JSON.stringify refuses one.
  encode(commands: any[]): any {
    for (const command of commands) {
      if (typeof command.id === 'bigint') {
        throw new TypeError('BigInt');
      }
    }
    return JSON.stringify(commands);
  }

  send(data: any): void {
    if (this.failWrite) {
      throw new Error('write failed');
    }
    this.sent.push(data);
  }

  debug(): void {}

  handleWriteError(commands: any[], refused: boolean): void {
    this.handled.push({ commands: commands, refused: refused });
  }

  writeFailed(): boolean {
    return this.writeFailedAnswer;
  }

  disconnectWriteFailed(): void {
    this.disconnects++;
  }
}

describe('writeCommands', () => {
  test('the commands of a write are sent as one frame', () => {
    const host: RecordingHost = new RecordingHost();
    expect(writeCommands(host, [{ id: 1 }, { id: 2 }])).toBe(true);
    expect(host.sent).toEqual([JSON.stringify([{ id: 1 }, { id: 2 }])]);
    expect(host.handled).toEqual([{ commands: [], refused: true }]);
  });

  test('a command the codec refuses is refused alone, and the others of the write are sent', () => {
    const host: RecordingHost = new RecordingHost();
    const refused: any = { id: BigInt(2) };
    expect(writeCommands(host, [{ id: 1 }, refused, { id: 3 }])).toBe(true);
    expect(host.handled[0]).toEqual({ commands: [refused], refused: true });
    expect(host.sent).toEqual([JSON.stringify([{ id: 1 }, { id: 3 }])]);
    expect(host.disconnects).toBe(0);
  });

  test('a write of refused commands only sends nothing and does not end the connection', () => {
    const host: RecordingHost = new RecordingHost();
    expect(writeCommands(host, [{ id: BigInt(1) }])).toBe(false);
    expect(host.sent).toEqual([]);
    expect(host.disconnects).toBe(0);
  });

  test('a write the transport refuses fails its commands, and ends the connection when they include a tracked call', () => {
    const host: RecordingHost = new RecordingHost();
    host.failWrite = true;
    expect(writeCommands(host, [{ id: 1 }])).toBe(false);
    expect(host.handled).toEqual([
      { commands: [], refused: true },
      { commands: [{ id: 1 }], refused: false },
    ]);
    expect(host.disconnects).toBe(1);
  });

  test('a failed write of commands with no tracked call or cancel keeps the connection', () => {
    const host: RecordingHost = new RecordingHost();
    host.failWrite = true;
    host.writeFailedAnswer = false;
    expect(writeCommands(host, [{ id: 1 }])).toBe(false);
    expect(host.disconnects).toBe(0);
  });

  test('no transport means nothing is written and nothing is settled', () => {
    const host: RecordingHost = new RecordingHost();
    host.transport = false;
    expect(writeCommands(host, [{ id: 1 }])).toBe(false);
    expect(host.sent).toEqual([]);
    expect(host.handled).toEqual([]);
  });

  test('no commands is a write that sends nothing and succeeds', () => {
    const host: RecordingHost = new RecordingHost();
    expect(writeCommands(host, [])).toBe(true);
    expect(host.sent).toEqual([]);
  });
});
