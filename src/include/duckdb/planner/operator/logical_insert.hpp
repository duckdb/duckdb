//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

class Index;

//! LogicalInsert represents an insertion of data into a base table
class LogicalInsert : public LogicalOperator {
public:
	LogicalInsert(TableCatalogEntry *table, idx_t table_index)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT), table(table), table_index(table_index),
	      return_chunk(false), action_type(OnConflictAction::THROW) {
	}

	vector<vector<unique_ptr<Expression>>> insert_values;
	//! The insertion map ([table_index -> index in result, or DConstants::INVALID_INDEX if not specified])
	physical_index_vector_t<idx_t> column_index_map;
	//! The expected types for the INSERT statement (obtained from the column types)
	vector<LogicalType> expected_types;
	//! The base table to insert into
	TableCatalogEntry *table;
	idx_t table_index;
	//! if returning option is used, return actual chunk to projection
	bool return_chunk;
	//! The default statements used by the table
	vector<unique_ptr<Expression>> bound_defaults;

	//! Which action to take on conflict
	OnConflictAction action_type;
	// The types that the DO UPDATE .. SET (expressions) are cast to
	vector<LogicalType> expected_set_types;
	// The (distinct) column ids to apply the ON CONFLICT on
	unordered_set<column_t> on_conflict_filter;
	// The WHERE clause of the conflict_target (ON CONFLICT .. WHERE <condition>)
	unique_ptr<Expression> on_conflict_condition;
	// The WHERE clause of the DO UPDATE clause
	unique_ptr<Expression> do_update_condition;
	// The DO UPDATE SET expressions
	vector<unique_ptr<Expression>> set_expressions;
	// The table_index referring to the column references qualified with 'excluded'
	idx_t excluded_table_index;
	// The columns to fetch from the 'destination' table
	vector<column_t> columns_to_fetch;
	// The columns to fetch from the 'source' table
	vector<column_t> source_columns;

public:
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

protected:
	vector<ColumnBinding> GetColumnBindings() override {
		if (return_chunk) {
			return GenerateColumnBindings(table_index, table->GetTypes().size());
		}
		return {ColumnBinding(0, 0)};
	}

	void ResolveTypes() override {
		if (return_chunk) {
			types = table->GetTypes();
		} else {
			types.emplace_back(LogicalType::BIGINT);
		}
	}

	idx_t EstimateCardinality(ClientContext &context) override;
	vector<idx_t> GetTableIndex() const override;
};
} // namespace duckdb
