const DuckDB = require('../../.wasm/build/hello_wasm.js');

async function main() {
    const db = await DuckDB();
    const HelloWasm = db['_HelloWasm'];
    const result = HelloWasm();
    const n = 10000;
    const expected = n * (n + 1) / 2;
    console.log(`expected ${expected}, received ${result}`);
    if (result != expected) {
        console.log('result differs');
        process.exit(1);
    } else {
        console.log('result ok');
        process.exit(0);
    }
}

main();