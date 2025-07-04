//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_merge_into.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/merge_action_type.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {
class TableCatalogEntry;
class LogicalGet;
class LogicalProjection;

class BoundMergeIntoAction {
public:
	//! The merge action type
	MergeActionType action_type;
	//! Condition - or NULL if this should always be performed for the given action
	unique_ptr<Expression> condition;
	//! The set of referenced physical columns (for UPDATE)
	vector<PhysicalIndex> columns;
	//! Set of expressions for INSERT or UPDATE
	vector<unique_ptr<Expression>> expressions;
	//! Column index map (for INSERT)
	physical_index_vector_t<idx_t> column_index_map;
	//! Whether or not an UPDATE is a DELETE + INSERT
	bool update_is_del_and_insert = false;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<BoundMergeIntoAction> Deserialize(Deserializer &deserializer);
};

class LogicalMergeInto : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_MERGE_INTO;

public:
	explicit LogicalMergeInto(TableCatalogEntry &table);

	//! The base table to merge into
	TableCatalogEntry &table;
	//! projection index
	idx_t table_index;
	vector<unique_ptr<Expression>> bound_defaults;
	idx_t row_id_start;
	optional_idx source_marker;
	//! Bound constraints
	vector<unique_ptr<BoundConstraint>> bound_constraints;

	map<MergeActionCondition, vector<unique_ptr<BoundMergeIntoAction>>> actions;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	vector<ColumnBinding> GetColumnBindings() override;
	void ResolveTypes() override;

private:
	LogicalMergeInto(ClientContext &context, const unique_ptr<CreateInfo> &table_info);
};

} // namespace duckdb
