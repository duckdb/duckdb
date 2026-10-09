//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/sql_render_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class SQLRenderMode : uint8_t { UNREDACTED, REDACTED };

struct SQLRenderContext {
	SQLRenderMode mode = SQLRenderMode::UNREDACTED;
	//! Inspect whether rendering would change the SQL without constructing a string.
	bool probe_only = false;
	//! Set by nodes that changed content, must be preserved as the context passes through children.
	bool changed = false;
};

} // namespace duckdb
