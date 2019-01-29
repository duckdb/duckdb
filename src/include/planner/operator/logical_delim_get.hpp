//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_delim_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"
#include "planner/operator/logical_join.hpp"

namespace duckdb {

//! LogicalDelimGet represents a scan of a duplicate eliminated set "D" created through a DUPLICATE ELIMINATED JOIN. This is used only for subquery flattening. For more detailde information see the paper "Unnesting Arbitrary Subqueries".
class LogicalDelimGet : public LogicalOperator {
public:
	LogicalDelimGet(LogicalOperator *delim_join, size_t table_index, vector<TypeId> types) :
		LogicalOperator(LogicalOperatorType::DELIM_GET), delim_join(delim_join), table_index(table_index) {
		assert(delim_join->type == LogicalOperatorType::JOIN);
		assert(((LogicalJoin&)*delim_join).is_duplicate_eliminated);
		this->types = types;
	}

	vector<string> GetNames() override {
		vector<string> names;
		for(size_t i = 0; i < types.size(); i++) {
			names.push_back(std::to_string(i));
		}
		return names;
	}

	//! The reference to the duplicate eliminated join
	LogicalOperator *delim_join;
	//! The table index in the current bind context
	size_t table_index;

protected:
	void ResolveTypes() override { }
};
} // namespace duckdb
