//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

class InsertLocalState;

//! Physically insert a set of data into a table
class PhysicalInsert : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INSERT;

public:
	//! INSERT INTO
	PhysicalInsert(vector<LogicalType> types, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map,
	               vector<unique_ptr<Expression>> bound_defaults, vector<unique_ptr<BoundConstraint>> bound_constraints,
	               vector<unique_ptr<Expression>> set_expressions, vector<PhysicalIndex> set_columns,
	               vector<LogicalType> set_types, idx_t estimated_cardinality, bool return_chunk, bool parallel,
	               OnConflictAction action_type, unique_ptr<Expression> on_conflict_condition,
	               unique_ptr<Expression> do_update_condition, unordered_set<column_t> on_conflict_filter,
	               vector<column_t> columns_to_fetch);
	//! CREATE TABLE AS
	PhysicalInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info,
	               idx_t estimated_cardinality, bool parallel);

	//! The map from insert column index to table column index
	physical_index_vector_t<idx_t> column_index_map;
	//! The table to insert into
	optional_ptr<TableCatalogEntry> insert_table;
	//! The insert types
	vector<LogicalType> insert_types;
	//! The default expressions of the columns for which no value is provided
	vector<unique_ptr<Expression>> bound_defaults;
	//! The bound constraints for the table
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	//! If the returning statement is present, return the whole chunk
	bool return_chunk;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! Whether or not the INSERT can be executed in parallel
	//! This insert is not order preserving if executed in parallel
	bool parallel;
	// Which action to perform on conflict
	OnConflictAction action_type;

	// The DO UPDATE set expressions, if 'action_type' is UPDATE
	vector<unique_ptr<Expression>> set_expressions;
	// Which columns are targeted by the set expressions
	vector<PhysicalIndex> set_columns;
	// The types of the columns targeted by a SET expression
	vector<LogicalType> set_types;

	// Condition for the ON CONFLICT clause
	unique_ptr<Expression> on_conflict_condition;
	// Condition for the DO UPDATE clause
	unique_ptr<Expression> do_update_condition;
	// The column ids to apply the ON CONFLICT on
	unordered_set<column_t> conflict_target;

	// Column ids from the original table to fetch
	vector<column_t> columns_to_fetch;
	// Matching types to the column ids to fetch
	vector<LogicalType> types_to_fetch;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return parallel;
	}

	bool SinkOrderDependent() const override {
		return true;
	}

public:
	static void GetInsertInfo(const BoundCreateTableInfo &info, vector<LogicalType> &insert_types,
	                          vector<unique_ptr<Expression>> &bound_defaults);
	static void ResolveDefaults(const TableCatalogEntry &table, DataChunk &chunk,
	                            const physical_index_vector_t<idx_t> &column_index_map,
	                            ExpressionExecutor &defaults_executor, DataChunk &result);

protected:
	void CombineExistingAndInsertTuples(DataChunk &result, DataChunk &scan_chunk, DataChunk &input_chunk,
	                                    ClientContext &client) const;
	//! Returns the amount of updated tuples
	void CreateUpdateChunk(ExecutionContext &context, DataChunk &chunk, TableCatalogEntry &table, Vector &row_ids,
	                       DataChunk &result) const;
	idx_t OnConflictHandling(TableCatalogEntry &table, ExecutionContext &context, InsertLocalState &lstate) const;
};

} // namespace duckdb
