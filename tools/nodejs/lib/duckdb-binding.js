var os = require('os');
var binary = require('@mapbox/node-pre-gyp');
var path = require('path');
var binding_path = binary.find(path.resolve(path.join(__dirname,'../package.json')));

// // dlopen is used because we need to specify the RTLD_GLOBAL flag to be able to resolve duckdb symbols
// on linux where RTLD_LOCAL is the default.
process.dlopen(module, binding_path, os.constants.dlopen.RTLD_NOW | os.constants.dlopen.RTLD_GLOBAL)