import { Readable, Transform, Writable, pipeline } from 'node:stream';
import { parser } from 'stream-json';
import { streamArray } from 'stream-json/streamers/StreamArray';
import * as fs from 'node:fs';

interface ChunkWithStream {
  stream: Readable;
  metadata: any;
}

class StreamProcessor extends Transform {
  constructor() {
    super({ objectMode: true });
  }

  async _transform(
    chunk: ChunkWithStream,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ) {
    const { stream: innerStream, metadata } = chunk;

    const outer = this;

    // Each item from StreamArray is { key: number, value: any }
    const innerTransform = new Transform({
      objectMode: true,
      transform(arrayItem: { key: number; value: any }, _enc, cb) {
        outer.push({ data: arrayItem.value, meta: metadata });
        cb();
      },
    });

    try {
      await new Promise<void>((resolve, reject) => {
        pipeline(
          [
            innerStream,
            parser(), // stream-json parser
            streamArray(), // only works on arrays
            innerTransform,
          ],
          (err) => {
            if (err) {
              console.error('Inner Pipeline failed:', err);
              reject(err);
            } else {
              console.log('Inner Pipeline complete');
              resolve();
            }
          },
        );
      });

      callback(); // done with this chunk
    } catch (err) {
      callback(err as Error);
    }
  }

  _flush(cb: (error?: Error | null) => void) {
    cb();
  }
}

async function run() {
  // read sources from argv
  const sourceFiles = process.argv.slice(2);
  if (sourceFiles.length < 1) {
    console.error(
      'Please provide at least one source file path as an argument.',
    );
    process.exit(1);
  }
  const sourceStreams = Readable.from(
    sourceFiles.map((filePath, index) => ({
      stream: fs.createReadStream(filePath),
      metadata: { id: index + 1, filePath },
    })),
  );

  await new Promise<void>((resolve, reject) => {
    pipeline(
      [
        sourceStreams,
        new StreamProcessor(),
        new Writable({
          objectMode: true,
          async write(chunk, _enc, cb) {
            try {
              console.log(`Simulating DB write for`, chunk);
              await fakeDbInsert(chunk); // Simulated DB call
              cb();
            } catch (err) {
              cb(err as Error);
            }
          },
        }),
      ],
      (err) => {
        if (err) {
          console.error('Pipeline failed:', err);
          reject(err);
        } else {
          console.log('Pipeline complete');
          resolve();
        }
      },
    );
  });
}

run().catch((err) => {
  console.error('Fatal error:', err);
});

async function fakeDbInsert(data: any): Promise<void> {
  // Simulate network/db latency
  await new Promise((res) => setTimeout(res, 100));
  console.log(`✅ Inserted into DB:`, data);
}
