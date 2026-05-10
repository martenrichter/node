// Flags: --experimental-quic --experimental-stream-iter --no-warnings

// Test: large bidirectional data transfer with backpressure.
// The client sends >1MB of data using the writer API, exercising the
// QUIC flow control path. The server reads all data and verifies the
// total byte count and a checksum. This tests that backpressure is
// correctly applied and released across the full transfer.

import { hasQuic, skip, mustCall } from '../common/index.mjs';
import assert from 'node:assert';

const { strictEqual } = assert;

if (!hasQuic) {
  skip('QUIC is not enabled');
}

const { listen, connect } = await import('../common/quic.mjs');
const { bytes, drainableProtocol: dp } = await import('stream/iter');

const chunkSizes = [ 60000, 12, 50000,  1600, 20000, 30000, 0, 100 ]
const numChunks = chunkSizes.length
const byteLength = chunkSizes.reduce((accumulator, currentValue) => accumulator + currentValue, 0);
  

// Build a deterministic payload so we can verify integrity.
function buildChunk(index) {
  const chunk = new Uint8Array(chunkSizes[index]);
  // Fill with a pattern derived from the chunk index.
  const val = index & 0xff;
  for (let i = 0; i < chunkSizes[index]; i++) {
    chunk[i] = (val + i) & 0xff;
  }
  return chunk;
}

function checksum(data) {
  let sum = 0;
  for (let i = 0; i < data.byteLength; i++) {
    sum = (sum + data[i]) | 0;
  }
  return sum;
}

// Compute expected checksum.
let expectedChecksum = 0;
for (let i = 0; i < numChunks; i++) {
  const chunk = buildChunk(i);
  expectedChecksum = (expectedChecksum + checksum(chunk)) | 0;
}

const done = Promise.withResolvers();

const serverEndpoint = await listen(mustCall((serverSession) => {
  serverSession.onstream = mustCall(async (stream) => {
    // if I place here a throw the test passes?
    const received = await bytes(stream);
    strictEqual(received.byteLength, byteLength);
    strictEqual(checksum(received), expectedChecksum);

    stream.writer.endSync();
    await stream.closed;
    serverSession.close();
    done.resolve();
  });
}));

const clientSession = await connect(serverEndpoint.address);
await clientSession.opened;

const stream = await clientSession.createBidirectionalStream();
const w = stream.writer;

// Write chunks, respecting backpressure via drainableProtocol.
for (let i = 0; i < numChunks; i++) {
  const chunk = buildChunk(i);
  while (!w.writeSync(chunk)) {
    // Flow controlled — wait for drain before retrying.
    const drainable = w[dp]();
    if (drainable) await drainable;
  }
  // you can see that it never reaches the second run
  // if (i === 2) throw new Error("Hello3")
}

const totalWritten = w.endSync();
strictEqual(totalWritten, byteLength);

await Promise.all([stream.closed, done.promise]);
await clientSession.close();
await serverEndpoint.close();
