//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/parser/column_definition.hpp"

namespace duckdb {

class ClientContext;

class Relation;

class ClientContextWrapper {
public:
	virtual ~ClientContextWrapper() = default;
	explicit ClientContextWrapper(const shared_ptr<ClientContext> &context);
	shared_ptr<ClientContext> GetContext();
	shared_ptr<ClientContext> TryGetContext();
	virtual void TryBindRelation(Relation &relation, vector<ColumnDefinition> &columns);

private:
	weak_ptr<ClientContext> client_context;
};

} // namespace duckdb
