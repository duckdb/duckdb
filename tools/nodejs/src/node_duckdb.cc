#include <stdint.h>
#include <sstream>
#include <cstring>
#include <string>

#include "macros.h"
#include "database.h"
#include "statement.h"

using namespace node_duckdb;

namespace {

Napi::Object RegisterModule(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Database::Init(env, exports);
    Statement::Init(env, exports);

    exports.DefineProperties({
	    DEFINE_CONSTANT_INTEGER(exports, DUCKDB_NODEJS_ERROR, ERROR)

        DEFINE_CONSTANT_INTEGER(exports, DUCKDB_NODEJS_READONLY, OPEN_READONLY) // same as SQLite
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_READWRITE) // ignored
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_CREATE) // ignored
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_FULLMUTEX) // ignored
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_SHAREDCACHE) // ignored
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_PRIVATECACHE) // ignored
                                 /*

                                 DEFINE_CONSTANT_STRING(exports, SQLITE_VERSION, VERSION)
                         #ifdef SQLITE_SOURCE_ID
                                 DEFINE_CONSTANT_STRING(exports, SQLITE_SOURCE_ID, SOURCE_ID)
                         #endif
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_VERSION_NUMBER, VERSION_NUMBER)

                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_OK, OK)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_ERROR, ERROR)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_INTERNAL, INTERNAL)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_PERM, PERM)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_ABORT, ABORT)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_BUSY, BUSY)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_LOCKED, LOCKED)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_NOMEM, NOMEM)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_READONLY, READONLY)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_INTERRUPT, INTERRUPT)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_IOERR, IOERR)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_CORRUPT, CORRUPT)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_NOTFOUND, NOTFOUND)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_FULL, FULL)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_CANTOPEN, CANTOPEN)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_PROTOCOL, PROTOCOL)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_EMPTY, EMPTY)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_SCHEMA, SCHEMA)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_TOOBIG, TOOBIG)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_CONSTRAINT, CONSTRAINT)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_MISMATCH, MISMATCH)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_MISUSE, MISUSE)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_NOLFS, NOLFS)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_AUTH, AUTH)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_FORMAT, FORMAT)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_RANGE, RANGE)
                                 DEFINE_CONSTANT_INTEGER(exports, SQLITE_NOTADB, NOTADB)
                                 */
    });

    return exports;
}

}

NODE_API_MODULE(node_duckdb, RegisterModule)
