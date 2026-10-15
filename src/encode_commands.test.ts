import { EncodedBatch, encodeBatch } from './encode_commands';

// An encoder that refuses a command whose id is a BigInt, as JSON.stringify refuses one.
function encodeRefusingBigInt(commands: any[]): string {
  return JSON.stringify(commands);
}

describe('encodeBatch', () => {
  test('a batch the codec encodes is written as one frame, with nothing refused', () => {
    const commands: any[] = [{ id: 1 }, { id: 2 }];
    const batch: EncodedBatch = encodeBatch(encodeRefusingBigInt, commands);

    expect(batch.data).toBe(JSON.stringify(commands));
    expect(batch.encodable).toEqual(commands);
    expect(batch.refused).toEqual([]);
  });

  test('a command the codec refuses is refused alone, the others are written in one frame', () => {
    const valid: any = { id: 1 };
    const refused: any = { id: 2, rpc: { data: BigInt(1) } };
    const alsoValid: any = { id: 3 };
    const batch: EncodedBatch = encodeBatch(encodeRefusingBigInt, [valid, refused, alsoValid]);

    expect(batch.refused).toEqual([refused]);
    expect(batch.encodable).toEqual([valid, alsoValid]);
    expect(batch.data).toBe(JSON.stringify([valid, alsoValid]));
  });

  test('a batch of refused commands has no frame', () => {
    const batch: EncodedBatch = encodeBatch(encodeRefusingBigInt, [{ id: 1, rpc: { data: BigInt(1) } }]);

    expect(batch.data).toBeNull();
    expect(batch.encodable).toEqual([]);
    expect(batch.refused).toHaveLength(1);
  });

  test('an empty batch encodes to an empty frame', () => {
    const batch: EncodedBatch = encodeBatch(encodeRefusingBigInt, []);

    expect(batch.data).toBe('[]');
    expect(batch.refused).toEqual([]);
  });
});
