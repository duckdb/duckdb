var assert = require('assert');
var fs = require('fs');
var pathExists = require('fs').existsSync || require('path').existsSync;

exports.deleteFile = function(name) {
    try {
        fs.unlinkSync(name);
    } catch(err) {
        if (err.errno !== process.ENOENT && err.code !== 'ENOENT' && err.syscall !== 'unlink') {
            throw err;
        }
    }
};

exports.ensureExists = function(name,cb) {
    if (!pathExists(name)) {
        fs.mkdirSync(name);
    };
}

assert.fileDoesNotExist = function(name) {
    try {
        fs.statSync(name);
    } catch(err) {
        if (err.errno !== process.ENOENT && err.code !== 'ENOENT' && err.syscall !== 'unlink') {
            throw err;
        }
    }
};

assert.fileExists = function(name) {
    try {
        fs.statSync(name);
    } catch(err) {
        throw err;
    }
};