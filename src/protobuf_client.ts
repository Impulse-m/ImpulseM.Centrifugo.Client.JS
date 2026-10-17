import { Centrifuge } from './centrifuge';
import { ProtobufCodec } from './protobuf.codec';

/**
 * The client of the protobuf entry: the Centrifuge client with the protobuf codec. It lives in its own module, so that
 * the RPC pool can create the clients without importing the entry module, which exports the pool itself.
 */
export class CentrifugeProtobuf extends Centrifuge {
  protected _formatOverride(): void {
    this._codec = new ProtobufCodec();
  }
}
