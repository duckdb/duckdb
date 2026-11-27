//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_predicate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/filter_info.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

enum class JoinPredicateClass { INNER_EQUALITY, INNER_NON_EQUALITY, LEFT_JOIN, SEMI_ANTI_JOIN, OTHER };

struct JoinOrderUtil {
	//! Return the comparison type when the predicate expression itself is a comparison.
	static ExpressionType GetJoinPredicateComparisonType(const FilterInfo &filter);
	//! Classify the predicate for equivalence closure, denominator assembly and cost-model use.
	static JoinPredicateClass ClassifyJoinPredicate(const FilterInfo &filter);
	static bool HasAnyValidJoinBinding(const FilterInfo &filter);
	static bool HasValidJoinEndpoints(const FilterInfo &filter);
	static bool HasDisjointJoinEndpoints(const FilterInfo &filter);
	static RelationIndex GetBindingRelation(const ColumnBinding &binding);
	//! Extract a column binding through invertible, non-TRY casts.
	static ColumnBinding GetEquivalenceBinding(const Expression &expression);
};

class JoinPredicate {
public:
	JoinPredicate(idx_t index, FilterInfo &filter, JoinPredicateClass predicate_class,
	              ColumnBinding left_equality_binding, ColumnBinding right_equality_binding);

public:
	idx_t GetIndex() const;
	FilterInfo &GetFilter() const;
	JoinPredicateClass GetPredicateClass() const;
	JoinType GetJoinType() const;
	ExpressionType GetComparisonType() const;

	JoinRelationSet &GetSet() const;
	JoinRelationSet &GetLeftSet() const;
	JoinRelationSet &GetRightSet() const;
	optional_ptr<JoinRelationSet> GetLeftSetOptional() const;
	optional_ptr<JoinRelationSet> GetRightSetOptional() const;
	optional_ptr<JoinRelationSet> GetSemanticLeftSetOptional() const;
	optional_ptr<JoinRelationSet> GetSemanticRightSetOptional() const;
	const ColumnBinding &GetLeftStatsBinding() const;
	const ColumnBinding &GetRightStatsBinding() const;
	const ColumnBinding &GetLeftEqualityBinding() const;
	const ColumnBinding &GetRightEqualityBinding() const;

	bool HasValidJoinEndpoints() const;
	bool HasDisjointJoinEndpoints() const;
	bool HasAnyValidStatsBinding() const;
	bool HasValidEqualityBindings() const;
	bool CanBuildEqualityClosure() const;
	bool CanBuildSelectivityDomain() const;
	bool IsResidualFilter() const;
	bool IsEquivalencePredicate() const;
	bool IsDirectValueEquality() const;

	void SetEqualityClassIndex(optional_idx equality_class_index);
	optional_idx GetEqualityClassIndex() const;

private:
	idx_t index;
	reference<FilterInfo> filter;
	JoinPredicateClass predicate_class;
	ExpressionType comparison_type;
	bool direct_value_equality;

	ColumnBinding left_equality_binding;
	ColumnBinding right_equality_binding;
	optional_idx equality_class_index;
};

struct JoinEqualityPredicateEdge {
public:
	JoinEqualityPredicateEdge(JoinPredicate &predicate, RelationIndex left_relation, RelationIndex right_relation,
	                          ColumnBinding left_binding, ColumnBinding right_binding);

public:
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

class JoinEqualityGroups {
public:
	void AddEquality(const ColumnBinding &left, const ColumnBinding &right);
	idx_t GetGroup(const ColumnBinding &binding);

private:
	idx_t GetOrCreateGroup(const ColumnBinding &binding);
	idx_t FindRoot(idx_t group);

private:
	column_binding_map_t<idx_t> binding_to_group;
	vector<idx_t> parents;
};

class JoinPredicateModel {
public:
	void Clear();
	JoinPredicate &RegisterPredicate(FilterInfo &filter, JoinPredicateClass predicate_class,
	                                 ColumnBinding left_equality_binding, ColumnBinding right_equality_binding);
	void AddEqualityClass(JoinEqualityClass equality_class);

	optional_ptr<const JoinPredicate> GetPredicateByFilterIndex(idx_t filter_index) const;
	const vector<reference<JoinPredicate>> &GetPredicates() const;
	const vector<reference<JoinPredicate>> &GetEqualityJoinPredicates() const;
	const vector<reference<JoinPredicate>> &GetSelectivityPredicates() const;
	const vector<reference<JoinPredicate>> &GetGraphPredicates() const;
	const vector<JoinEqualityClass> &GetEqualityClasses() const;

private:
	vector<unique_ptr<JoinPredicate>> predicates;
	//! Views retain registration order and borrow the owned predicates above.
	vector<reference<JoinPredicate>> all_predicates;
	vector<reference<JoinPredicate>> equality_join_predicates;
	vector<reference<JoinPredicate>> selectivity_predicates;
	vector<reference<JoinPredicate>> graph_predicates;

	vector<JoinEqualityClass> equality_classes;
};

} // namespace duckdb
