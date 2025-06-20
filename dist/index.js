"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_stream_1 = require("node:stream");
const node_util_1 = require("node:util");
const stream_json_1 = require("stream-json");
const StreamArray_1 = require("stream-json/streamers/StreamArray");
const pipelineAsync = (0, node_util_1.promisify)(node_stream_1.pipeline);
class StreamProcessor extends node_stream_1.Transform {
    constructor() {
        super({ objectMode: true });
    }
    async _transform(chunk, _encoding, callback) {
        const { stream: innerStream, metadata } = chunk;
        const outer = this;
        // Each item from StreamArray is { key: number, value: any }
        const innerTransform = new node_stream_1.Transform({
            objectMode: true,
            transform(arrayItem, _enc, cb) {
                outer.push({ data: arrayItem.value, meta: metadata });
                cb();
            },
        });
        try {
            await pipelineAsync(innerStream, (0, stream_json_1.parser)(), // stream-json parser
            (0, StreamArray_1.streamArray)(), // only works on arrays
            innerTransform);
            callback(); // done with this chunk
        }
        catch (err) {
            callback(err);
        }
    }
    _flush(cb) {
        cb();
    }
}
async function run() {
    const source = node_stream_1.Readable.from([
        {
            stream: node_stream_1.Readable.from(['["a", "b"]']), // full JSON array as string
            metadata: { id: 1 },
        },
        {
            stream: node_stream_1.Readable.from(['["c", "d"]']),
            metadata: { id: 2 },
        },
    ]);
    await new Promise((resolve, reject) => {
        (0, node_stream_1.pipeline)(source, new StreamProcessor(), new node_stream_1.Writable({
            objectMode: true,
            write(chunk, _enc, cb) {
                console.log('Final output:', chunk);
                cb();
            },
        }), (err) => {
            if (err) {
                console.error('Pipeline failed:', err);
                reject(err);
            }
            else {
                console.log('Pipeline complete');
                resolve();
            }
        });
    });
}
run().catch((err) => {
    console.error('Fatal error:', err);
});
