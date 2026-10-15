import { EncodedBatch, encodeBatch, WriteHost, writeCommands } from './write_commands';

// An encoder that refuses a command whose id is a BigInt, as JSON.stringify refuses one.
function encodeRefusingBigInt(commands: any[]): string {
  return JSON.stringify(commands);
}

// An encoder that refuses a frame longer than the limit, as a codec with a frame size limit does.
function encodeWithLimit(limit: number): (commands: any[]) => string {
  return (commands: any[]): string => {
    const data: string = JSON.stringify(commands);
    if (data.length > limit) {
      throw new RangeError('frame too large');
    }
    return data;
  };
}

// The write of commands against a host that records what it sends and settles.
class RecordingHost implements WriteHost {
  transport: boolean = true;
  sent: any[] = [];
  // The send with this index (0 for the first) throws; -1 never throws.
  failSend: number = -1;
  sends: number = 0;
  // A frame longer than this is refused by the codec, as a codec with a size limit does.
  frameLimit: number = Infinity;
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
    const data: string = JSON.stringify(commands);
    if (data.length > this.frameLimit) {
      throw new RangeError('frame too large');
    }
    return data;
  }

  send(data: any): void {
    const index: number = this.sends;
    this.sends++;
    if (index === this.failSend) {
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

describe('encodeBatch', () => {
  test('a batch the codec encodes is written as one frame, with nothing refused', () => {
    const commands: any[] = [{ id: 1 }, { id: 2 }];
    const batch: EncodedBatch = encodeBatch(encodeRefusingBigInt, commands);

    expect(batch.frames).toEqual([{ data: JSON.stringify(commands), commands: commands }]);
    expect(batch.refused).toEqual([]);
  });

  test('a command the codec refuses is refused alone, the others are written in one frame', () => {
    const valid: any = { id: 1 };
    const refused: any = { id: 2, rpc: { data: BigInt(1) } };
    const alsoValid: any = { id: 3 };
    const batch: EncodedBatch = encodeBatch(encodeRefusingBigInt, [valid, refused, alsoValid]);

    expect(batch.refused).toEqual([refused]);
    expect(batch.frames).toEqual([{ data: JSON.stringify([valid, alsoValid]), commands: [valid, alsoValid] }]);
  });

  test('a batch of refused commands has no frame', () => {
    const batch: EncodedBatch = encodeBatch(encodeRefusingBigInt, [{ id: 1, rpc: { data: BigInt(1) } }]);

    expect(batch.frames).toEqual([]);
    expect(batch.refused).toHaveLength(1);
  });

  test('a batch the codec refuses as a whole is written command by command, when each command fits alone', () => {
    const commands: any[] = [{ id: 1, data: 'aaaaaa' }, { id: 2, data: 'bbbbbb' }];
    const encode: (items: any[]) => string = encodeWithLimit(30);

    const batch: EncodedBatch = encodeBatch(encode, commands);

    expect(batch.refused).toEqual([]);
    expect(batch.frames).toEqual([
      { data: JSON.stringify([commands[0]]), commands: [commands[0]] },
      { data: JSON.stringify([commands[1]]), commands: [commands[1]] },
    ]);
  });

  test('a command too long for any frame is refused, the others are still written', () => {
    const long: any = { id: 1, data: 'x'.repeat(100) };
    const short: any = { id: 2 };
    const batch: EncodedBatch = encodeBatch(encodeWithLimit(50), [long, short]);

    expect(batch.refused).toEqual([long]);
    expect(batch.frames).toEqual([{ data: JSON.stringify([short]), commands: [short] }]);
  });

  test('an empty batch encodes to an empty frame', () => {
    const batch: EncodedBatch = encodeBatch(encodeRefusingBigInt, []);

    expect(batch.frames).toEqual([{ data: '[]', commands: [] }]);
    expect(batch.refused).toEqual([]);
  });
});

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

  test('a batch too long for one frame is written in frames of its own, and every command is sent', () => {
    const host: RecordingHost = new RecordingHost();
    host.frameLimit = 30;
    const commands: any[] = [{ id: 1, data: 'aaaaaa' }, { id: 2, data: 'bbbbbb' }];

    expect(writeCommands(host, commands)).toBe(true);

    expect(host.sent).toEqual([JSON.stringify([commands[0]]), JSON.stringify([commands[1]])]);
    expect(host.handled).toEqual([{ commands: [], refused: true }]);
    expect(host.disconnects).toBe(0);
  });

  test('a write the transport refuses fails its commands, and ends the connection when they include a tracked call', () => {
    const host: RecordingHost = new RecordingHost();
    host.failSend = 0;
    expect(writeCommands(host, [{ id: 1 }])).toBe(false);
    expect(host.handled).toEqual([
      { commands: [], refused: true },
      { commands: [{ id: 1 }], refused: false },
    ]);
    expect(host.disconnects).toBe(1);
  });

  test('a write the transport refuses after an earlier frame fails the commands of the frames not written', () => {
    const host: RecordingHost = new RecordingHost();
    host.frameLimit = 30;
    host.failSend = 1;
    const commands: any[] = [{ id: 1, data: 'aaaaaa' }, { id: 2, data: 'bbbbbb' }, { id: 3, data: 'cccccc' }];

    expect(writeCommands(host, commands)).toBe(false);

    expect(host.sent).toEqual([JSON.stringify([commands[0]])]);
    expect(host.handled).toEqual([
      { commands: [], refused: true },
      { commands: [commands[1], commands[2]], refused: false },
    ]);
    expect(host.disconnects).toBe(1);
  });

  test('a failed write of commands with no tracked call or cancel keeps the connection', () => {
    const host: RecordingHost = new RecordingHost();
    host.failSend = 0;
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
