//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class ClientContext;

//! The QueryContext wraps an optional client context.
//! It makes query-related information available to operations.
class QueryContext {
public:
	QueryContext() : context(nullptr) {
	}
	QueryContext(optional_ptr<ClientContext> context) : context(context) { // NOLINT: allow implicit construction
	}
	QueryContext(ClientContext &context) : context(&context) { // NOLINT: allow implicit construction
	}

public:
	bool Valid() const {
		return context != nullptr;
	}
	optional_ptr<ClientContext> GetClientContext() const {
		return context;
	}

private:
	optional_ptr<ClientContext> context;
};

} // namespace duckdb
