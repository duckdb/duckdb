//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/relation_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

namespace duckdb {

class JoinOrderOptimizer;

//! Represents a single relation and any metadata accompanying that relation
struct SingleJoinRelation {
	LogicalOperator &op;
	optional_ptr<LogicalOperator> parent;
	RelationStats stats;

	SingleJoinRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent) : op(op), parent(parent) {
	}
	SingleJoinRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent, RelationStats stats)
	    : op(op), parent(parent), stats(std::move(stats)) {
	}
};

//! FilterInfo models strores filter information so that edges between relations can be made
//! with the original ColumnBinding information available so that the cardinality estimator can
//! view the statistics of the underlying base tables.
class FilterInfo {
public:
	FilterInfo(unique_ptr<Expression> filter, optional_ptr<JoinRelationSet> set, idx_t filter_index, JoinType join_type,
	           optional_ptr<JoinRelationSet> left_relation_set, optional_ptr<JoinRelationSet> right_relation_set,
	           ColumnBinding left_binding, ColumnBinding right_binding)
	    : filter(std::move(filter)), set(set), filter_index(filter_index), join_type(join_type),
	      left_relation_set(left_relation_set), right_relation_set(right_relation_set), left_binding(left_binding),
	      right_binding(right_binding) {
	}
	FilterInfo(unique_ptr<Expression> filter, optional_ptr<JoinRelationSet> set, idx_t filter_index, JoinType join_type,
	           optional_ptr<JoinRelationSet> left_relation_set, optional_ptr<JoinRelationSet> right_relation_set)
	    : filter(std::move(filter)), set(set), filter_index(filter_index), join_type(join_type),
	      left_relation_set(left_relation_set), right_relation_set(right_relation_set) {
	}

public:
	unique_ptr<Expression> filter;
	optional_ptr<JoinRelationSet> set;
	idx_t filter_index;
	JoinType join_type;
	optional_ptr<JoinRelationSet> left_relation_set;
	optional_ptr<JoinRelationSet> right_relation_set;
	// TODO: change this to be a binding set
	ColumnBinding left_binding;
	ColumnBinding right_binding;
	bool from_residual_predicate = false;
};

class RelationManager {
public:
	explicit RelationManager(ClientContext &context) : context(context) {
	}

	idx_t NumRelations();

	bool ExtractJoinRelations(JoinOrderOptimizer &optimizer, LogicalOperator &input_op,
	                          vector<reference<LogicalOperator>> &filter_operators,
	                          optional_ptr<LogicalOperator> parent = nullptr);

	//! for each join filter in the logical plan op, extract the relations that are referred to on
	//! both sides of the join filter, along with the tables & indexes.
	vector<unique_ptr<FilterInfo>> ExtractEdges(vector<reference<LogicalOperator>> &filter_operators,
	                                            JoinRelationSetManager &set_manager);

	//! Extract the set of relations referred to inside an expression
	bool ExtractBindings(Expression &expression, unordered_set<RelationIndex> &bindings);
	//! Inspects an expression and creates filter info instances that can connect two relations
	//! If the expression (or conjunction expression children cannot create a FilterInfo), then
	//! they are returned to be added to the filter_op so they are pushed down at the end of reconstruction.
	vector<unique_ptr<Expression>> CreateFilterInfoFromExpression(unique_ptr<Expression> expr,
	                                                              JoinRelationSetManager &set_manager,
	                                                              JoinType join_type = JoinType::INNER);
	vector<unique_ptr<Expression>>
	CreateFilterFromConjunctionChildren(unique_ptr<BoundConjunctionExpression> conjunction_expression,
	                                    JoinRelationSetManager &set_manager, JoinType join_type);

	optional_ptr<JoinRelationSet> GetJoinRelations(column_binding_set_t &column_bindings,
	                                               JoinRelationSetManager &set_manager);
	void GetColumnBindingsFromExpression(Expression &expression, column_binding_set_t &column_bindings);
	void AddRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent, const RelationStats &stats);
	//! Add an unnest relation which can come from a logical unnest or a logical get which has an unnest function
	void AddRelationWithChildren(JoinOrderOptimizer &optimizer, LogicalOperator &op, LogicalOperator &input_op,
	                             optional_ptr<LogicalOperator> parent, RelationStats &child_stats,
	                             optional_ptr<LogicalOperator> limit_op,
	                             vector<reference<LogicalOperator>> &datasource_filters);
	void AddAggregateOrWindowRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent,
	                                  const RelationStats &stats, LogicalOperatorType op_type);
	vector<unique_ptr<SingleJoinRelation>> GetRelations();

	const vector<RelationStats> GetRelationStats();
	//! A mapping of base table index -> index into relations array (relation number)
	unordered_map<TableIndex, RelationIndex> relation_mapping;

	bool CrossProductWithRelationAllowed(idx_t relation_id);

	void PrintRelationStats();

private:
	ClientContext &context;
	//! Set of all relations considered in the join optimizer
	vector<unique_ptr<SingleJoinRelation>> relations;
	unordered_set<idx_t> no_cross_product_relations;

	//! Used when extracting edges from the relations. They are then passed to the query graph manager
	vector<unique_ptr<FilterInfo>> filter_infos_;
};

} // namespace duckdb
