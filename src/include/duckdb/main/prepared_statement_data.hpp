//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/prepared_statement_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"
#include "duckdb/planner/bound_parameter_map.hpp"

namespace duckdb {
class CatalogEntry;
class ClientContext;
class PhysicalPlan;
class SQLStatement;

class PreparedStatementData {
public:
	DUCKDB_API explicit PreparedStatementData(StatementType type);
	DUCKDB_API ~PreparedStatementData();

	StatementType statement_type;
	//! The unbound SQL statement that was prepared
	unique_ptr<SQLStatement> unbound_statement;

	//! The physical plan.
	unique_ptr<PhysicalPlan> physical_plan;

	//! The result names of the transaction
	vector<string> names;
	//! The result types of the transaction
	vector<LogicalType> types;

	//! The statement properties
	StatementProperties properties;

	//! The map of parameter index to the actual value entry
	bound_parameter_map_t value_map;
	//! Whether we are creating a streaming result or not
	QueryResultOutputType output_type;
	//! Whether we are creating a buffer-managed result or not
	QueryResultMemoryType memory_type;

public:
	void CheckParameterCount(idx_t parameter_count);
	//! Whether or not the prepared statement data requires the query to rebound for the given parameters
	bool RequireRebind(ClientContext &context, optional_ptr<case_insensitive_map_t<BoundParameterData>> values);
	//! Bind a set of values to the prepared statement data
	DUCKDB_API void Bind(case_insensitive_map_t<BoundParameterData> values);
	//! Get the expected SQL Type of the bound parameter
	DUCKDB_API LogicalType GetType(const string &identifier);
	//! Try to get the expected SQL Type of the bound parameter
	DUCKDB_API bool TryGetType(const string &identifier, LogicalType &result);
};

} // namespace duckdb
