//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb_static_extension.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"

#ifdef __cplusplus
extern "C" {
#endif

//! Descriptor layout this header defines.
#define DUCKDB_EXTENSION_DESCRIPTOR_VERSION 1

typedef struct duckdb_extension_descriptor duckdb_extension_descriptor;

//! Describes a statically linked extension. DuckDB allocates and zero-fills it, the extension's root fills it in.
struct duckdb_extension_descriptor {
	//! In: the layout DuckDB offers. Out: the layout the root filled, never higher than the offer.
	uint32_t version;

	// Layout 1, set by DuckDB
	//! Reports why the root refused. DuckDB copies the message.
	void (*set_error)(duckdb_extension_descriptor *descriptor, const char *message);
	//! DuckDB's state for set_error, opaque to the extension.
	void *internal;

	// Layout 1, set by the extension; DuckDB copies the strings before the root's caller returns
	const char *name;
	const char *extension_version;
	//! void (duckdb::ExtensionLoader &)
	void (*entry_cpp)(void);
	//! bool (duckdb_extension_info, struct duckdb_extension_access *)
	void (*entry_capi_v1)(void);
	//! void (struct duckdb_v2_extension_input *)
	void (*entry_capi_v2)(void);
};

//! Every statically linkable extension provides duckdb_extension_<name>_root with this signature. Returns 0 on success.
typedef int32_t (*duckdb_extension_root)(duckdb_extension_descriptor *descriptor);

//! Calls root and registers the extension it describes for every database opened afterwards.
//! Registering the same root again is a no-op; a different root under a registered name is an error.
//! A failed registration also makes opening a database fail with the reason.
DUCKDB_C_API duckdb_state duckdb_register_static_extension(duckdb_extension_root root);

#ifdef __cplusplus
}
#endif
