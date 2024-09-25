//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

class ClientContext;

class ClientContextWrapper {
public:
	explicit ClientContextWrapper(const shared_ptr<ClientContext> &context);
	shared_ptr<ClientContext> GetContext();
	shared_ptr<ClientContext> TryGetContext();
	virtual void TryBindRelation(Relation &relation, vector<ColumnDefinition> &columns) {
		GetContext()->TryBindRelation(relation, columns);
	}

private:
	weak_ptr<ClientContext> client_context;
};

} // namespace duckdb
