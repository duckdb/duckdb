//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/merge_into_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

class ClusterStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::CLUSTER_STATEMENT;

public:
	ClusterStatement();

	unique_ptr<TableRef> target;
	vector<OrderByNode> modifiers;

protected:
	ClusterStatement(const ClusterStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace duckdb
