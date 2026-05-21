//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/query_graph_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {

class QueryGraphEdges;

struct GenerateJoinRelation {
	GenerateJoinRelation(optional_ptr<JoinRelationSet> set, unique_ptr<LogicalOperator> op_p)
	    : set(set), op(std::move(op_p)) {
	}

	optional_ptr<JoinRelationSet> set;
	unique_ptr<LogicalOperator> op;
};

//! Filter info struct that is used by the cardinality estimator to set the initial cardinality
//! but is also eventually transformed into a query edge.
class FilterInfo {
public:
	FilterInfo(unique_ptr<Expression> filter, JoinRelationSet &set, idx_t filter_index,
	           JoinType join_type = JoinType::INNER)
	    : filter(std::move(filter)), set(set), filter_index(filter_index), join_type(join_type) {
	}

public:
	unique_ptr<Expression> filter;
	reference<JoinRelationSet> set;
	idx_t filter_index;
	JoinType join_type;
	optional_ptr<JoinRelationSet> left_set;
	optional_ptr<JoinRelationSet> right_set;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
	bool from_residual_predicate = false;
	//! Index of the equivalence group for INNER equality/IS NOT DISTINCT FROM join filters.
	//! All filters transitively connected by equality (a=b, b=c -> a=c all share the same index).
	//! Used to skip redundant conditions during plan reconstruction and cardinality estimation.
	optional_idx edge_equivalence_index;

	void SetLeftSet(optional_ptr<JoinRelationSet> left_set_new);
	void SetRightSet(optional_ptr<JoinRelationSet> right_set_new);
};

enum class JoinPredicateClass { INNER_EQUALITY, INNER_NON_EQUALITY, LEFT_JOIN, SEMI_ANTI_JOIN, OTHER };

struct JoinOrderUtil {
	//! Return the comparison type when the predicate expression itself is a comparison.
	static ExpressionType GetJoinPredicateComparisonType(const FilterInfo &filter) {
		if (!filter.filter) {
			return ExpressionType::INVALID;
		}
		if (!BoundComparisonExpression::IsComparison(*filter.filter)) {
			return ExpressionType::INVALID;
		}
		return filter.filter->GetExpressionType();
	}

	//! Classify the predicate for equivalence closure, denominator assembly and cost-model use.
	static JoinPredicateClass ClassifyJoinPredicate(const FilterInfo &filter) {
		switch (filter.join_type) {
		case JoinType::INNER: {
			const auto comparison_type = GetJoinPredicateComparisonType(filter);
			if (comparison_type == ExpressionType::COMPARE_EQUAL ||
			    comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
				return JoinPredicateClass::INNER_EQUALITY;
			}
			return comparison_type == ExpressionType::INVALID ? JoinPredicateClass::OTHER
			                                                  : JoinPredicateClass::INNER_NON_EQUALITY;
		}
		case JoinType::LEFT:
			return JoinPredicateClass::LEFT_JOIN;
		case JoinType::SEMI:
		case JoinType::ANTI:
			return JoinPredicateClass::SEMI_ANTI_JOIN;
		default:
			return JoinPredicateClass::OTHER;
		}
	}

	//! INNER equality and IS NOT DISTINCT FROM predicates are the only predicates that form equivalence closures.
	static bool IsEquivalenceJoinPredicate(const FilterInfo &filter) {
		return ClassifyJoinPredicate(filter) == JoinPredicateClass::INNER_EQUALITY;
	}

	//! LEFT/SEMI/ANTI preserve one side's cardinality and need hidden input-work costing.
	static bool IsCardinalityPreservingJoinPredicate(const FilterInfo &filter) {
		const auto predicate_class = ClassifyJoinPredicate(filter);
		return predicate_class == JoinPredicateClass::LEFT_JOIN ||
		       predicate_class == JoinPredicateClass::SEMI_ANTI_JOIN;
	}
};

//! The QueryGraphManager manages the process of extracting the reorderable and nonreorderable operations
//! from the logical plan and creating the intermediate structures needed by the plan enumerator.
//! When the plan enumerator finishes, the Query Graph Manger can then recreate the logical plan.
class QueryGraphManager {
public:
	explicit QueryGraphManager(ClientContext &context) : relation_manager(context), context(context) {
	}

	//! manage relations and the logical operators they represent
	RelationManager relation_manager;

	//! A structure holding all the created JoinRelationSet objects
	JoinRelationSetManager set_manager;

	ClientContext &context;

	//! Extract the join relations, optimizing non-reoderable relations when encountered
	bool Build(JoinOrderOptimizer &optimizer, LogicalOperator &op);

	//! Reconstruct the logical plan using the plan found by the plan enumerator
	unique_ptr<LogicalOperator> Reconstruct(unique_ptr<LogicalOperator> plan);

	//! Get a reference to the QueryGraphEdges structure that stores edges between
	//! nodes and hypernodes.
	const QueryGraphEdges &GetQueryGraphEdges() const;

	//! Get a list of the join filters in the join plan than eventually are
	//! transformed into the query graph edges
	const vector<unique_ptr<FilterInfo>> &GetFilterBindings() const;

	//! Plan enumerator may not find a full plan and therefore will need to create cross
	//! products to create edges.
	void CreateQueryGraphCrossProduct(JoinRelationSet &left, JoinRelationSet &right);

	//! A map to store the optimal join plan found for a specific JoinRelationSet
	optional_ptr<const reference_map_t<JoinRelationSet, unique_ptr<DPJoinNode>>> plans;

private:
	vector<reference<LogicalOperator>> filter_operators;

	//! Filter information including the column_bindings that join filters
	//! used by the cardinality estimator to estimate distinct counts
	vector<unique_ptr<FilterInfo>> filters_and_bindings;

	QueryGraphEdges query_graph;

	void GetColumnBinding(const Expression &expression, ColumnBinding &binding);

	void CreateHyperGraphEdges();

	//! Assign edge_equivalence_index to INNER equality/IS NOT DISTINCT FROM filters using union-find over column
	//! bindings. All filters in the same transitive equality closure receive the same index.
	void MarkEdgeEquivalences();

	GenerateJoinRelation GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinRelationSet &set);
};

} // namespace duckdb
