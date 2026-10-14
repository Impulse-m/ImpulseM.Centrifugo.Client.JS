import { hasFlag, serverErrorFields, hasHeaders, rpcOptionsFlag, serverTimeOffsetFrom } from './extensions';

describe('rpcOptionsFlag', () => {
  test('is bit 32 of the flag bitmask', () => {
    expect(rpcOptionsFlag).toBe(4294967296);
  });
});

describe('hasFlag', () => {
  test('detects bit 32 in a value above 32 bits', () => {
    expect(hasFlag(4294967296, rpcOptionsFlag)).toBe(true);
    expect(hasFlag(4294967297, rpcOptionsFlag)).toBe(true);
  });

  test('does not detect bit 32 when it is absent', () => {
    expect(hasFlag(0, rpcOptionsFlag)).toBe(false);
    expect(hasFlag(1, rpcOptionsFlag)).toBe(false);
    expect(hasFlag(8589934592, rpcOptionsFlag)).toBe(false);
  });

  test('detects bit 32 together with a higher bit', () => {
    expect(hasFlag(4294967296 + 8589934592, rpcOptionsFlag)).toBe(true);
  });

  test('detects low bits', () => {
    expect(hasFlag(1, 1)).toBe(true);
    expect(hasFlag(2, 1)).toBe(false);
  });
});

describe('serverTimeOffsetFrom', () => {
  test('is server time minus local time', () => {
    expect(serverTimeOffsetFrom(1700000100000, 1700000000000)).toBe(100000);
  });

  test('reads a number and a decoded Long-shaped value alike', () => {
    expect(serverTimeOffsetFrom({ low: 1, high: 0, unsigned: false }, 0)).toBe(1);
    expect(serverTimeOffsetFrom(1700000100000, 1700000000000)).toBe(100000);
  });

  test('is null when the reply carries no server time', () => {
    expect(serverTimeOffsetFrom(undefined, 1700000000000)).toBeNull();
    expect(serverTimeOffsetFrom(null, 1700000000000)).toBeNull();
    expect(serverTimeOffsetFrom(0, 1700000000000)).toBeNull();
  });
});

describe('serverErrorFields', () => {
  test('returns no keys for the proto zero values', () => {
    expect(serverErrorFields({ app_code: 0, app_type: '', metadata: {} })).toEqual({});
  });

  test('returns no keys for absent fields', () => {
    expect(serverErrorFields({})).toEqual({});
  });

  test('maps the wire names to the client names', () => {
    expect(serverErrorFields({
      app_code: 1234,
      app_type: 'EXAMPLE_RETRY_AFTER',
      metadata: { 'example-key': 'value' }
    })).toEqual({
      appCode: 1234,
      appType: 'EXAMPLE_RETRY_AFTER',
      metadata: { 'example-key': 'value' }
    });
  });
});

describe('hasHeaders', () => {
  test('is false for absent or empty headers', () => {
    expect(hasHeaders(undefined)).toBe(false);
    expect(hasHeaders({})).toBe(false);
  });

  test('is true when there is at least one header', () => {
    expect(hasHeaders({ 'example-header': 'value' })).toBe(true);
  });
});
