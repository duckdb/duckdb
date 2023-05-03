import * as fs from 'fs';
import {constants} from "os";

const ENOENT = constants.errno.ENOENT;

function notUnlinkError(err: any) {
    return err.errno !== ENOENT && err.code !== 'ENOENT' && err.syscall !== 'unlink';
}

export function deleteFile(name: fs.PathLike) {
    try {
        fs.unlinkSync(name);
    } catch(err) {
        if (notUnlinkError(err)) {
            throw err;
        }
    }
}

export function ensureExists(name: fs.PathLike) {
    if (!fs.existsSync(name)) {
        fs.mkdirSync(name);
    }
}

export function fileDoesNotExist(name: fs.PathLike) {
    try {
        fs.statSync(name);
    } catch(err) {
        if (notUnlinkError(err)) {
            throw err;
        }
    }
}

export function fileExists(name: fs.PathLike) {
    try {
        fs.statSync(name);
    } catch(err) {
        throw err;
    }
}
