//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/apply_functional_dependencies.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"

#include "duckdb/optimizer/distinctness.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

struct BoundOrderByNode;

//! Simplifies the keys of orderings and of window partitions / orderings using the unique column combinations of
//! their input. The unique column combinations themselves are inferred by Distinctness.
class ApplyFunctionalDependencies : public LogicalOperatorVisitor {
public:
	ApplyFunctionalDependencies() : distinctness(DistinctnessPropagation::UNARY_OPERATORS) {
	}

	//! The unique column combinations of the output of an operator
	static vector<column_binding_set_t> GetUniqueColumnCombinations(LogicalOperator &op) {
		Distinctness distinctness(DistinctnessPropagation::UNARY_OPERATORS);
		return distinctness.GetUniqueSets(op);
	}

	void VisitOperator(LogicalOperator &op) override;
	void VisitExpression(unique_ptr<Expression> *expression) override;

private:
	void VisitLogicalOrder(LogicalOrder &order);

	void VisitWindowExpression(BoundWindowExpression &wexpr) const;

	void VisitPartitioning(vector<unique_ptr<Expression>> &partitions) const;
	void VisitOrderBys(vector<BoundOrderByNode> &orders) const;

	//! The order keys and window partitions that this visitor rewrites are not read by the inference of unique sets,
	//! and no operator is replaced, so the cached unique sets stay valid during the visit
	Distinctness distinctness;
	//! The unique column combinations of the input of the operator that is being simplified
	vector<column_binding_set_t> result;
};

} // namespace duckdb
