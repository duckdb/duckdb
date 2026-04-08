//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/planner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/bound_parameter_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class PreparedStatementData;
class Binder;
class SQLStatement;
struct BoundParameterData;

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder and LogicalPlanGenerator.
class Planner {
	friend class Binder;

public:
	explicit Planner(ClientContext &context);

public:
	unique_ptr<LogicalOperator> plan;
	vector<string> names;
	vector<LogicalType> types;
	case_insensitive_map_t<BoundParameterData> parameter_data;

	shared_ptr<Binder> binder;
	ClientContext &context;

	StatementProperties properties;
	bound_parameter_map_t value_map;

public:
	void CreatePlan(unique_ptr<SQLStatement> statement);
	static void VerifyPlan(ClientContext &context, unique_ptr<LogicalOperator> &op,
	                       optional_ptr<bound_parameter_map_t> map = nullptr);

private:
	void CreatePlan(SQLStatement &statement);
	shared_ptr<PreparedStatementData> PrepareSQLStatement(unique_ptr<SQLStatement> statement);
};
} // namespace duckdb
