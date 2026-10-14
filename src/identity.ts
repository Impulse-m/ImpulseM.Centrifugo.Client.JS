import { AuthContext } from './types';

/**
 * The user and auth class the server confirmed for a connection, from its connect reply or its last
 * accepted refresh reply. A reply without them, as from a server that does not send them, gives empty values.
 */
export class ConnectionIdentity {
  user: string = '';
  authClass: string = '';
  // Called with the new identity when a refresh reply changes it.
  private onChange: (ctx: AuthContext) => void;

  constructor(onChange: (ctx: AuthContext) => void) {
    this.onChange = onChange;
  }

  /** Takes the identity of a connect reply. */
  applyConnect(result: any): void {
    this.user = result.user ?? '';
    this.authClass = result.auth_class ?? '';
  }

  /** Takes the identity of a refresh reply, and reports it when it changed the identity of the connection. */
  applyRefresh(result: any): void {
    const user: string = result.user ?? '';
    const authClass: string = result.auth_class ?? '';
    const changed: boolean = user !== this.user || authClass !== this.authClass;
    this.user = user;
    this.authClass = authClass;
    if (changed) {
      this.onChange(this.context(result.ttl ?? 0));
    }
  }

  /** The auth context of the connection, with the ttl of the reply that confirmed it. */
  context(ttl: number): AuthContext {
    return { user: this.user, authClass: this.authClass, ttl: ttl };
  }
}
