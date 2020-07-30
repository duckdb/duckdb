const addon = require('bindings')('duckdb'); // import 'greet.node'
exports.connect = addon.connect;