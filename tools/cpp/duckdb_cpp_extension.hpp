//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb_cpp_extension.hpp
//
// The entrypoint of an extension written against the C++ API. Include this
// instead of duckdb_extension_v2.h, in the file holding the entrypoint; every
// other file just includes duckdb_cpp.hpp.
//
// This is separate from duckdb_cpp.hpp because the entrypoint needs
// duckdb_extension_v2.h, and that header is not free to include: it rewrites
// every duckdb_v2_* name to an indirection through a vtable global, refuses to
// coexist with duckdb_extension.h, and forces a choice between the loadable and
// link-time flavors. duckdb_cpp.hpp names no C type at all, and a plain client
// application linking libduckdb should not have to answer any of that.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_cpp.hpp"

// Match the flavor duckdb_cpp.cpp was compiled in: the loadable build routes every call through the extension vtable,
// anything else binds DuckDB's symbols at link time.
#if !defined(DUCKDB_CPP_API_LOADABLE) && !defined(DUCKDB_BUILD_STATIC_EXTENSION)
#define DUCKDB_BUILD_STATIC_EXTENSION
#endif
#include "duckdb_extension_v2.h"

// The entrypoint macro below defines the vtable global that the C++ API archive references, so an extension must use it
// exactly once. Including this header more than once is harmless; expanding the macro twice is a duplicate symbol.

//! Defines the entrypoint of a C++ API extension. Requires DUCKDB_EXTENSION_NAME to be set, which the build helpers do.
//! Write the body as
//!
//!		DUCKDB_CPP_EXTENSION_ENTRYPOINT(duckdb::cxx::Extension &extension, duckdb::cxx::Context &context) {
//!			...
//!		}
//!
//! Throwing from the body fails the load and reports the exception; returning normally completes it.
#define DUCKDB_CPP_EXTENSION_ENTRYPOINT                                                                                \
	static void duckdb_cpp_extension_entry(duckdb::cxx::Extension &, duckdb::cxx::Context &);                          \
	DUCKDB_EXTENSION_ENTRYPOINT(duckdb_v2_extension_handle extension, duckdb_v2_context_handle context,                \
	                            duckdb_v2_error_info_handle *err) {                                                    \
		duckdb::cxx::detail::RunExtensionEntry(duckdb_cpp_extension_entry, extension, context, err);                   \
	}                                                                                                                  \
	static void duckdb_cpp_extension_entry
