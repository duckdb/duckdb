#include "catch.hpp"

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;

static unique_ptr<LogicalOperator> CreateCandidateCTERef(TableIndex table_index, TableIndex cte_index) {
	vector<LogicalType> types {LogicalType::INTEGER, LogicalType::INTEGER};
	vector<Identifier> names {Identifier("k0"), Identifier("k1")};
	return make_uniq<LogicalCTERef>(table_index, cte_index, std::move(types), std::move(names), false);
}

static unique_ptr<LogicalComparisonJoin> CreateCandidateOuterJoin(TableIndex source_cte_index) {
	auto payload_index = TableIndex(10);
	auto result = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	result->children.push_back(CreateCandidateCTERef(payload_index, source_cte_index));
	result->duplicate_eliminated_columns.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(payload_index, ProjectionIndex(0))));
	result->duplicate_eliminated_columns.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, ColumnBinding(payload_index, ProjectionIndex(1))));
	return result;
}

static unique_ptr<LogicalOperator> CreateCandidateRestriction(TableIndex generated_cte_index,
                                                              TableIndex retained_cte_index,
                                                              const vector<idx_t> &retained_order, idx_t index_offset,
                                                              bool computed_projection = false) {
	auto retained_index = TableIndex(20 + index_offset);
	unique_ptr<LogicalOperator> retained = CreateCandidateCTERef(retained_index, retained_cte_index);
	if (computed_projection) {
		auto projection_index = TableIndex(30 + index_offset);
		vector<unique_ptr<Expression>> expressions;
		expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
		                                                          ColumnBinding(retained_index, ProjectionIndex(0))));
		expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
		auto projection = make_uniq<LogicalProjection>(projection_index, std::move(expressions));
		projection->children.push_back(std::move(retained));
		retained = std::move(projection);
		retained_index = projection_index;
	}

	auto generated_index = TableIndex(40 + index_offset);
	auto restriction = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	for (idx_t domain_index = 0; domain_index < retained_order.size(); domain_index++) {
		restriction->conditions.emplace_back(
		    make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER,
		                                        ColumnBinding(generated_index, ProjectionIndex(domain_index))),
		    make_uniq<BoundColumnRefExpression>(
		        LogicalType::INTEGER, ColumnBinding(retained_index, ProjectionIndex(retained_order[domain_index]))),
		    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}
	restriction->children.push_back(std::move(retained));
	restriction->children.push_back(CreateCandidateCTERef(generated_index, generated_cte_index));
	return std::move(restriction);
}

static unique_ptr<LogicalOperator> CreateCandidateGroupedRHS(unique_ptr<LogicalOperator> child) {
	vector<unique_ptr<Expression>> aggregates;
	auto result = make_uniq<LogicalAggregate>(TableIndex(80), TableIndex(81), std::move(aggregates));
	result->children.push_back(std::move(child));
	return std::move(result);
}

TEST_CASE("Equivalent-source domain elimination requires exact CTE key provenance", "[optimizer][subquery]") {
	auto source_cte_index = TableIndex(1);
	auto generated_cte_index = TableIndex(2);
	const vector<idx_t> direct_order {0, 1};

	auto outer = CreateCandidateOuterJoin(source_cte_index);
	auto exact =
	    CreateCandidateGroupedRHS(CreateCandidateRestriction(generated_cte_index, source_cte_index, direct_order, 0));
	REQUIRE(DuplicateEliminatedDomainAnalyzer::CanEliminateEquivalentSourceDomain(*outer, *exact, generated_cte_index));

	auto reordered =
	    CreateCandidateGroupedRHS(CreateCandidateRestriction(generated_cte_index, source_cte_index, {1, 0}, 10));
	REQUIRE_FALSE(
	    DuplicateEliminatedDomainAnalyzer::CanEliminateEquivalentSourceDomain(*outer, *reordered, generated_cte_index));

	auto different_source =
	    CreateCandidateGroupedRHS(CreateCandidateRestriction(generated_cte_index, TableIndex(3), direct_order, 20));
	REQUIRE_FALSE(DuplicateEliminatedDomainAnalyzer::CanEliminateEquivalentSourceDomain(*outer, *different_source,
	                                                                                    generated_cte_index));

	auto computed = CreateCandidateGroupedRHS(
	    CreateCandidateRestriction(generated_cte_index, source_cte_index, direct_order, 30, true));
	REQUIRE_FALSE(
	    DuplicateEliminatedDomainAnalyzer::CanEliminateEquivalentSourceDomain(*outer, *computed, generated_cte_index));
}

TEST_CASE("Equivalent-source domain elimination validates every grouped restriction", "[optimizer][subquery]") {
	auto source_cte_index = TableIndex(1);
	auto generated_cte_index = TableIndex(2);
	const vector<idx_t> direct_order {0, 1};
	auto outer = CreateCandidateOuterJoin(source_cte_index);

	auto exact_pair = LogicalCrossProduct::Create(
	    CreateCandidateRestriction(generated_cte_index, source_cte_index, direct_order, 0),
	    CreateCandidateRestriction(generated_cte_index, source_cte_index, direct_order, 10));
	auto exact = CreateCandidateGroupedRHS(std::move(exact_pair));
	REQUIRE(DuplicateEliminatedDomainAnalyzer::CanEliminateEquivalentSourceDomain(*outer, *exact, generated_cte_index));

	auto mixed_pair =
	    LogicalCrossProduct::Create(CreateCandidateRestriction(generated_cte_index, source_cte_index, direct_order, 20),
	                                CreateCandidateRestriction(generated_cte_index, TableIndex(3), direct_order, 30));
	auto mixed = CreateCandidateGroupedRHS(std::move(mixed_pair));
	REQUIRE_FALSE(
	    DuplicateEliminatedDomainAnalyzer::CanEliminateEquivalentSourceDomain(*outer, *mixed, generated_cte_index));
}

TEST_CASE("Equivalent-source domain elimination traces keys through cross products", "[optimizer][subquery]") {
	auto source_cte_index = TableIndex(1);
	auto generated_cte_index = TableIndex(2);
	auto outer = CreateCandidateOuterJoin(source_cte_index);
	outer->children[0] = LogicalCrossProduct::Create(std::move(outer->children[0]),
	                                                 CreateCandidateCTERef(TableIndex(90), TableIndex(3)));

	auto rhs = CreateCandidateGroupedRHS(CreateCandidateRestriction(generated_cte_index, source_cte_index, {0, 1}, 0));
	REQUIRE(DuplicateEliminatedDomainAnalyzer::CanEliminateEquivalentSourceDomain(*outer, *rhs, generated_cte_index));
}
