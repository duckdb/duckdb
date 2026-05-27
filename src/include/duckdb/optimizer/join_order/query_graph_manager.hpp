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
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {

class QueryGraphEdges;
class JoinPredicate;

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
	//! Used by cardinality estimation to skip redundant transitive conditions.
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

	static bool HasAnyValidJoinBinding(const FilterInfo &filter) {
		return filter.left_binding.table_index.IsValid() || filter.right_binding.table_index.IsValid();
	}

	static bool HasValidJoinEndpoints(const FilterInfo &filter) {
		return filter.left_set && filter.right_set && !filter.left_set->Empty() && !filter.right_set->Empty() &&
		       filter.left_set != filter.right_set;
	}

	static bool HasDisjointJoinEndpoints(const FilterInfo &filter) {
		if (!HasValidJoinEndpoints(filter)) {
			return false;
		}
		auto &left = *filter.left_set;
		auto &right = *filter.right_set;
		for (idx_t left_idx = 0; left_idx < left.count; left_idx++) {
			for (idx_t right_idx = 0; right_idx < right.count; right_idx++) {
				if (left.relations[left_idx] == right.relations[right_idx]) {
					return false;
				}
			}
		}
		return true;
	}

	static RelationIndex GetBindingRelation(const ColumnBinding &binding) {
		D_ASSERT(binding.table_index.IsValid());
		return RelationIndex(binding.table_index.index);
	}

	static bool ContainsRelation(JoinRelationSet &set, RelationIndex relation) {
		for (idx_t i = 0; i < set.count; i++) {
			if (set.relations[i] == relation) {
				return true;
			}
		}
		return false;
	}
};

class JoinPredicate {
public:
	JoinPredicate(idx_t index, FilterInfo &filter, JoinPredicateClass predicate_class,
	              ColumnBinding left_equality_binding, ColumnBinding right_equality_binding)
	    : index(index), filter(filter), predicate_class(predicate_class),
	      comparison_type(JoinOrderUtil::GetJoinPredicateComparisonType(filter)),
	      left_equality_binding(left_equality_binding), right_equality_binding(right_equality_binding) {
	}

	idx_t GetIndex() const {
		return index;
	}

	FilterInfo &GetFilter() const {
		return filter.get();
	}

	JoinPredicateClass GetPredicateClass() const {
		return predicate_class;
	}

	JoinType GetJoinType() const {
		return filter.get().join_type;
	}

	ExpressionType GetComparisonType() const {
		return comparison_type;
	}

	JoinRelationSet &GetSet() const {
		return filter.get().set;
	}

	JoinRelationSet &GetLeftSet() const {
		D_ASSERT(filter.get().left_set);
		return *filter.get().left_set;
	}

	JoinRelationSet &GetRightSet() const {
		D_ASSERT(filter.get().right_set);
		return *filter.get().right_set;
	}

	optional_ptr<JoinRelationSet> GetLeftSetOptional() const {
		return filter.get().left_set;
	}

	optional_ptr<JoinRelationSet> GetRightSetOptional() const {
		return filter.get().right_set;
	}

	const ColumnBinding &GetStatsBinding(bool left) const {
		return left ? filter.get().left_binding : filter.get().right_binding;
	}

	const ColumnBinding &GetEqualityBinding(bool left) const {
		return left ? left_equality_binding : right_equality_binding;
	}

	bool HasValidJoinEndpoints() const {
		return JoinOrderUtil::HasValidJoinEndpoints(filter.get());
	}

	bool HasDisjointJoinEndpoints() const {
		return JoinOrderUtil::HasDisjointJoinEndpoints(filter.get());
	}

	bool HasAnyValidStatsBinding() const {
		return JoinOrderUtil::HasAnyValidJoinBinding(filter.get());
	}

	bool HasValidEqualityBindings() const {
		return left_equality_binding.table_index.IsValid() && right_equality_binding.table_index.IsValid();
	}

	bool CanBuildEqualityClosure() const {
		return predicate_class == JoinPredicateClass::INNER_EQUALITY && HasValidEqualityBindings() &&
		       HasValidJoinEndpoints();
	}

	bool CanBuildSelectivityDomain() const {
		if (!HasValidJoinEndpoints() || !HasAnyValidStatsBinding()) {
			return false;
		}
		return predicate_class != JoinPredicateClass::INNER_EQUALITY || !CanBuildEqualityClosure();
	}

	bool IsEquivalencePredicate() const {
		return CanBuildEqualityClosure();
	}

	void SetEqualityClassIndex(optional_idx equality_class_index) {
		this->equality_class_index = equality_class_index;
		filter.get().edge_equivalence_index = equality_class_index;
	}

	optional_idx GetEqualityClassIndex() const {
		return equality_class_index;
	}

private:
	idx_t index;
	reference<FilterInfo> filter;
	JoinPredicateClass predicate_class;
	ExpressionType comparison_type;
	ColumnBinding left_equality_binding;
	ColumnBinding right_equality_binding;
	optional_idx equality_class_index;
};

struct JoinEqualityPredicateEdge {
	JoinEqualityPredicateEdge(JoinPredicate &predicate, RelationIndex left_relation, RelationIndex right_relation,
	                          ColumnBinding left_binding, ColumnBinding right_binding)
	    : predicate(predicate), left_relation(left_relation), right_relation(right_relation),
	      left_binding(left_binding), right_binding(right_binding) {
	}

	reference<JoinPredicate> predicate;
	RelationIndex left_relation;
	RelationIndex right_relation;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
};

struct JoinEqualityClass {
	idx_t index = DConstants::INVALID_INDEX;
	column_binding_set_t columns;
	unordered_set<RelationIndex> relations;
	vector<JoinEqualityPredicateEdge> edges;
};

struct RelationPairEqualitySummary {
	vector<idx_t> equality_class_indices;
};

class JoinPredicateModel {
public:
	void Clear();
	JoinPredicate &RegisterPredicate(FilterInfo &filter, JoinPredicateClass predicate_class,
	                                 ColumnBinding left_equality_binding, ColumnBinding right_equality_binding);
	void AddEqualityClass(JoinEqualityClass equality_class);
	void AddEqualityPairClass(JoinRelationSet &pair, idx_t equality_class_index);

	const vector<reference<JoinPredicate>> &GetPredicates() const;
	const vector<reference<JoinPredicate>> &GetEqualityJoinPredicates() const;
	const vector<reference<JoinPredicate>> &GetSelectivityPredicates() const;
	const vector<reference<JoinPredicate>> &GetGraphPredicates() const;
	const vector<JoinEqualityClass> &GetEqualityClasses() const;
	bool HasLeftJoinPredicates() const;

	bool EqualityClassConnectsPairInScope(idx_t class_index, JoinRelationSet &pair, JoinRelationSet &scope) const;
	idx_t CountActiveEqualityClasses(JoinRelationSet &pair, JoinRelationSet &scope) const;

private:
	static bool ContainsClassIndex(const vector<idx_t> &class_indices, idx_t equality_class_index);

	vector<unique_ptr<JoinPredicate>> predicates;
	vector<reference<JoinPredicate>> all_predicates;
	vector<reference<JoinPredicate>> equality_join_predicates;
	vector<reference<JoinPredicate>> selectivity_predicates;
	vector<reference<JoinPredicate>> graph_predicates;
	bool has_left_join_predicates = false;
	vector<JoinEqualityClass> equality_classes;
	reference_map_t<JoinRelationSet, RelationPairEqualitySummary> equality_pairs;
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

	const JoinPredicateModel &GetPredicateModel() const;

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
	JoinPredicateModel predicate_model;

	void GetColumnBinding(const Expression &expression, ColumnBinding &binding);
	void GetEquivalenceBinding(const Expression &expression, ColumnBinding &binding);

	void BindFilterEndpoints();
	void CreateHyperGraphEdges();

	//! Build the normalized predicate model after filter endpoints and stats bindings are populated.
	void BuildPredicateModel();

	GenerateJoinRelation GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinRelationSet &set);
};

} // namespace duckdb
