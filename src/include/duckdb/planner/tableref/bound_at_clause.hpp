//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_at_clause.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"

namespace duckdb {

//! The AT clause specifies which version of a table to read
class BoundAtClause {
public:
	BoundAtClause(string unit_p, Value value_p) : unit(std::move(unit_p)), val(std::move(value_p)) {
	}

public:
	const string &Unit() const {
		return unit;
	}
	const Value &GetValue() const {
		return val;
	}

private:
	//! The unit (e.g. TIMESTAMP or VERSION)
	string unit;
	//! The value that is associated with the unit (e.g. TIMESTAMP '2020-01-01')
	Value val;
};

} // namespace duckdb
