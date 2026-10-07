//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/capi_v2/extension_load_v2.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"

//! Defined in duckdb_v2.h. Declared here at global scope so the entrypoint type below names the same struct without
//! pulling the whole V2 header into everything that loads extensions.
struct duckdb_v2_extension_input;

namespace duckdb {

class ClientContext;
class DatabaseInstance;
struct ExtensionInitResult;

//! The entrypoint of a V2 C API extension: <filebase>_init_c_api_v2
typedef void (*ext_init_c_api_v2_fun_t)(::duckdb_v2_extension_input *input);

//! Calls a V2 C API extension entrypoint and translates its outcome into an exception.
//! `context` is the context the extension is handed: the caller's when loading through LOAD, and when it is not set an
//! internal connection is opened for the duration of the call instead. `statically_linked` extensions resolve DuckDB's
//! symbols at link time, so they get no get_api callback.
typedef void (*invoke_ext_capi_v2_fun_t)(DatabaseInstance &db, const ExtensionInitResult &init_result,
                                         const string &extension_name, ext_init_c_api_v2_fun_t init_fun,
                                         optional_ptr<ClientContext> context, bool statically_linked);

//! The implementation behind DatabaseInstance::invoke_capi_v2. Call it through DatabaseInstance rather than directly:
//! see the member for why.
void InvokeCAPIV2Entrypoint(DatabaseInstance &db, const ExtensionInitResult &init_result, const string &extension_name,
                            ext_init_c_api_v2_fun_t init_fun, optional_ptr<ClientContext> context,
                            bool statically_linked);

} // namespace duckdb
