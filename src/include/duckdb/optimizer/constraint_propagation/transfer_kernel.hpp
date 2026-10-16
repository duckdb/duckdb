#pragma once

#include "duckdb/optimizer/constraint_propagation/constraint_facts.hpp"
#include "duckdb/common/enums/join_type.hpp"

namespace duckdb {

class ConstraintPropagator;
class LogicalOperator;
class LogicalComparisonJoin;

class TransferKernel {
public:
	explicit TransferKernel(ConstraintPropagator &owner);

	void Walk(LogicalOperator &root);

private:
	void Visit(LogicalOperator &op);

	void VisitGet(LogicalOperator &op, ScopeFacts &props);
	void VisitProjection(LogicalOperator &op, ScopeFacts &props);
	void VisitPassthrough(LogicalOperator &op, ScopeFacts &props);
	void VisitAggregate(LogicalOperator &op, ScopeFacts &props);
	void VisitDistinct(LogicalOperator &op, ScopeFacts &props);
	void VisitSetOperation(LogicalOperator &op, ScopeFacts &props);

	void VisitComparisonJoin(LogicalOperator &op, ScopeFacts &props);
	void VisitAsofJoin(LogicalOperator &op, ScopeFacts &props);
	void VisitDelimJoin(LogicalOperator &op, ScopeFacts &props);
	void VisitAnyJoin(LogicalOperator &op, ScopeFacts &props);
	void VisitCrossProduct(LogicalOperator &op, ScopeFacts &props);
	void TransferJoinValueFacts(LogicalOperator &op, JoinType join_type, ScopeFacts &props, bool conservative_not_null);

	ConstraintPropagator &owner_;
};

} // namespace duckdb
