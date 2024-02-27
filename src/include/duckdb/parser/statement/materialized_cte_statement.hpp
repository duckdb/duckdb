//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/materialized_cte_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

class MaterializedCTEStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::MATERIALIZED_CTE_STATEMENT;

public:
	MaterializedCTEStatement() : SQLStatement(StatementType::MATERIALIZED_CTE_STATEMENT) {
	}

	string ctename;
	//! The query of the CTE
	unique_ptr<QueryNode> query;
	//! Child
	unique_ptr<SQLStatement> child;
	//! Aliases of the CTE node
	vector<string> aliases;

	//! CTEs
	CommonTableExpressionMap cte_map;

protected:
	MaterializedCTEStatement(const MaterializedCTEStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;
};
} // namespace duckdb
