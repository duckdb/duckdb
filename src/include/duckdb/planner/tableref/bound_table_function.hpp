//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Represents a reference to a table-producing function call
class BoundTableFunction {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::TABLE_FUNCTION;

public:
	explicit BoundTableFunction(unique_ptr<LogicalOperator> get) : get(std::move(get)) {
	}

	unique_ptr<LogicalOperator> get;
	BoundStatement subquery;
};

} // namespace duckdb
