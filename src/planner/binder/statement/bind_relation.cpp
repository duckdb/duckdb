#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/planner/bound_statement.hpp"

namespace duckdb {

BoundStatement Binder::Bind(RelationStatement &stmt) {
	return stmt.relation->Bind(*this);
}

} // namespace duckdb
