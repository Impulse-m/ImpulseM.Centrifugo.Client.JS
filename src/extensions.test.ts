import { hasFlag, int64ToNumber, serverErrorFields, hasHeaders, rpcOptionsFlag } from './extensions';

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

describe('int64ToNumber', () => {
  test('passes a number through', () => {
    expect(int64ToNumber(1700000000000)).toBe(1700000000000);
  });

  test('converts a Long-like value', () => {
    expect(int64ToNumber({ toNumber: () => 42 })).toBe(42);
  });

  test('returns undefined for absent values', () => {
    expect(int64ToNumber(undefined)).toBeUndefined();
    expect(int64ToNumber(null)).toBeUndefined();
  });

  test('returns undefined for an unknown shape', () => {
    expect(int64ToNumber({})).toBeUndefined();
    expect(int64ToNumber('42')).toBeUndefined();
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
