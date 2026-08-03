#include "catch.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/subquery/duplicate_eliminated_domain_properties.hpp"

using namespace duckdb;

static unique_ptr<LogicalOperator> CreateIntegerInput(TableIndex table_index, idx_t column_count = 1) {
	vector<unique_ptr<Expression>> expressions;
	for (idx_t column_idx = 0; column_idx < column_count; column_idx++) {
		expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(NumericCast<int32_t>(column_idx))));
	}
	return make_uniq<LogicalProjection>(table_index, std::move(expressions));
}

static unique_ptr<LogicalOperator> CreateGroupedInput(TableIndex input_index, TableIndex group_index,
                                                      TableIndex aggregate_index, idx_t group_count = 1,
                                                      bool multiple_grouping_sets = false) {
	vector<unique_ptr<Expression>> aggregates;
	auto aggregate = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	aggregate->grouping_sets.emplace_back();
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		aggregate->groups.push_back(make_uniq<BoundColumnRefExpression>(
		    LogicalType::INTEGER, ColumnBinding(input_index, ProjectionIndex(group_idx))));
		aggregate->grouping_sets.back().insert(ProjectionIndex(group_idx));
	}
	if (multiple_grouping_sets) {
		aggregate->grouping_sets.emplace_back();
	}
	aggregate->children.push_back(CreateIntegerInput(input_index, group_count));
	return std::move(aggregate);
}

struct SingleJoinTestPlan {
	unique_ptr<LogicalOperator> root;
	reference<LogicalComparisonJoin> single_join;
};

static SingleJoinTestPlan CreateSingleJoinTestPlan(bool duplicate_free_opposite_side, ExpressionType comparison_type,
                                                   JoinType intermediate_join_type = JoinType::LEFT,
                                                   bool project_left = true, bool multiple_grouping_sets = false,
                                                   bool recurring_cte_ref = false) {
	auto cte_index = TableIndex(10);
	auto cte_group_index = TableIndex(11);
	auto cte_definition = CreateGroupedInput(TableIndex(12), cte_group_index, TableIndex(13));

	auto cte_ref_index = TableIndex(14);
	vector<LogicalType> cte_types {LogicalType::INTEGER};
	vector<Identifier> cte_names {Identifier("key")};
	auto cte_ref = make_uniq<LogicalCTERef>(cte_ref_index, cte_index, cte_types, cte_names, recurring_cte_ref);

	auto opposite_group_index = TableIndex(15);
	auto opposite_binding = ColumnBinding(opposite_group_index, ProjectionIndex(0));
	auto opposite_group_count = duplicate_free_opposite_side ? 1 : 2;
	auto opposite_side = CreateGroupedInput(TableIndex(16), opposite_group_index, TableIndex(17), opposite_group_count,
	                                        multiple_grouping_sets);

	auto intermediate_join = make_uniq<LogicalComparisonJoin>(intermediate_join_type);
	intermediate_join->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(cte_ref_index, ProjectionIndex(0))),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, opposite_binding), comparison_type);
	intermediate_join->children.push_back(std::move(cte_ref));
	intermediate_join->children.push_back(std::move(opposite_side));

	auto rhs_projection_index = TableIndex(19);
	vector<unique_ptr<Expression>> rhs_expressions;
	auto projected_binding = project_left ? ColumnBinding(cte_ref_index, ProjectionIndex(0)) : opposite_binding;
	rhs_expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, projected_binding));
	auto rhs_projection = make_uniq<LogicalProjection>(rhs_projection_index, std::move(rhs_expressions));
	rhs_projection->children.push_back(std::move(intermediate_join));

	auto lhs_index = TableIndex(20);
	auto single_join = make_uniq<LogicalComparisonJoin>(JoinType::SINGLE);
	auto &single_join_ref = *single_join;
	single_join->conditions.emplace_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(lhs_index, ProjectionIndex(0))),
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
	                                        ColumnBinding(rhs_projection_index, ProjectionIndex(0))),
	    comparison_type);
	single_join->children.push_back(CreateIntegerInput(lhs_index));
	single_join->children.push_back(std::move(rhs_projection));

	auto root = make_uniq<LogicalMaterializedCTE>(Identifier("domain"), cte_index, 1, std::move(cte_definition),
	                                              std::move(single_join), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	return {std::move(root), single_join_ref};
}

TEST_CASE("Duplicate-free scalar RHS proof follows generated CTE joins", "[optimizer][subquery]") {
	for (auto comparison_type : {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_NOT_DISTINCT_FROM}) {
		auto duplicate_free = CreateSingleJoinTestPlan(true, comparison_type);
		REQUIRE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(duplicate_free.single_join,
		                                                                         *duplicate_free.root));

		// Grouping by a second column permits duplicate rows for the actual join key and must be rejected.
		auto duplicate_opposite = CreateSingleJoinTestPlan(false, comparison_type);
		REQUIRE_FALSE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(duplicate_opposite.single_join,
		                                                                               *duplicate_opposite.root));
	}
}

TEST_CASE("Duplicate-free scalar RHS proof respects join cardinality and proof barriers", "[optimizer][subquery]") {
	for (auto comparison_type : {ExpressionType::COMPARE_EQUAL, ExpressionType::COMPARE_NOT_DISTINCT_FROM}) {
		for (auto join_type : {JoinType::INNER, JoinType::LEFT}) {
			auto duplicate_free = CreateSingleJoinTestPlan(true, comparison_type, join_type);
			REQUIRE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(duplicate_free.single_join,
			                                                                         *duplicate_free.root));

			auto duplicate_opposite = CreateSingleJoinTestPlan(false, comparison_type, join_type);
			REQUIRE_FALSE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(
			    duplicate_opposite.single_join, *duplicate_opposite.root));
		}

		for (auto join_type : {JoinType::RIGHT, JoinType::RIGHT_SEMI, JoinType::RIGHT_ANTI}) {
			auto duplicate_free = CreateSingleJoinTestPlan(true, comparison_type, join_type, false);
			REQUIRE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(duplicate_free.single_join,
			                                                                         *duplicate_free.root));

			auto duplicate_opposite = CreateSingleJoinTestPlan(false, comparison_type, join_type, false);
			REQUIRE_FALSE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(
			    duplicate_opposite.single_join, *duplicate_opposite.root));
		}

		for (auto join_type : {JoinType::SEMI, JoinType::ANTI, JoinType::SINGLE}) {
			auto plan = CreateSingleJoinTestPlan(false, comparison_type, join_type);
			REQUIRE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(plan.single_join, *plan.root));
		}

		for (auto project_left : {false, true}) {
			auto full_join =
			    CreateSingleJoinTestPlan(true, comparison_type, JoinType::OUTER, project_left, false, false);
			REQUIRE_FALSE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(full_join.single_join,
			                                                                               *full_join.root));
		}

		auto grouping_sets = CreateSingleJoinTestPlan(true, comparison_type, JoinType::LEFT, true, true, false);
		REQUIRE_FALSE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(grouping_sets.single_join,
		                                                                               *grouping_sets.root));

		auto recurring_ref = CreateSingleJoinTestPlan(true, comparison_type, JoinType::LEFT, true, false, true);
		REQUIRE_FALSE(DuplicateEliminatedDomainProperties::SingleJoinRHSIsDeduplicated(recurring_ref.single_join,
		                                                                               *recurring_ref.root));
	}
}
