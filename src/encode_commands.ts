/** The commands of one write, split by what the codec can encode. */
export interface EncodedBatch {
  // The encoded frame of the encodable commands; null when none of them is encodable.
  data: any;
  // The commands the frame carries, in order.
  encodable: any[];
  // The commands the codec refuses. They are not written, and each of them is refused on its own.
  refused: any[];
}


/**
 * Encodes the commands of one write. When the codec refuses the batch, each command is tried on its own, so a
 * command the codec cannot encode (e.g. a BigInt in its data) is refused alone, and the other commands of the
 * batch are still written in one frame.
 */
export function encodeBatch(encode: (commands: any[]) => any, commands: any[]): EncodedBatch {
  try {
    return { data: encode(commands), encodable: commands, refused: [] };
  } catch (e) {
    const encodable: any[] = [];
    const refused: any[] = [];
    for (const command of commands) {
      try {
        encode([command]);
        encodable.push(command);
      } catch (refusal) {
        refused.push(command);
      }
    }
    return {
      data: encodable.length > 0 ? encode(encodable) : null,
      encodable: encodable,
      refused: refused,
    };
  }
}
