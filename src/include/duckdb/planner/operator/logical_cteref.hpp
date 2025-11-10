//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_cteref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/cte_materialize.hpp"

namespace duckdb {

//! LogicalCTERef represents a reference to a recursive CTE
class LogicalCTERef : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CTE_REF;

public:
	LogicalCTERef(idx_t table_index, idx_t cte_index, vector<LogicalType> types, vector<string> colnames,
	              bool is_recurring = false)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_CTE_REF), table_index(table_index), cte_index(cte_index),
	      correlated_columns(0), is_recurring(is_recurring) {
		D_ASSERT(!types.empty());
		chunk_types = std::move(types);
		bound_columns = std::move(colnames);
	}

	vector<string> bound_columns;
	//! The table index in the current bind context
	idx_t table_index;
	//! CTE index
	idx_t cte_index;
	//! The types of the chunk
	vector<LogicalType> chunk_types;
	//! Number of correlated columns
	idx_t correlated_columns;
	//! Does this operator read the recurring CTE table
	bool is_recurring = false;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, chunk_types.size());
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override {
		// types are resolved in the constructor
		this->types = chunk_types;
	}
};
} // namespace duckdb
