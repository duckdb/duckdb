//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_projection.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
//! LogicalProjection represents the projection list in a SELECT clause
class LogicalProjection : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_PROJECTION;

public:
	LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list);

	idx_t table_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(FieldWriter &writer) const override;

	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

	vector<idx_t> GetTableIndex() const override;

protected:
	void ResolveTypes() override;

public:
	CKeyCollection *DeriveKeyCollection(CExpressionHandle &exprhdl) override;

	CPropConstraint *DerivePropertyConstraint(CExpressionHandle &exprhdl) override;
	// Rehydrate expression from a given cost context and child expressions
	Operator *SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
	                        CDrvdPropCtxtPlan *pdpctxtplan) override;
	// Transformations: candidate set of xforms
	CXform_set *PxfsCandidates() const override;

	duckdb::unique_ptr<Operator> Copy() override;

	duckdb::unique_ptr<Operator> CopyWithNewGroupExpression(CGroupExpression *pgexpr) override;

	duckdb::unique_ptr<Operator> CopyWithNewChildren(CGroupExpression *pgexpr,
	                                                 duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
	                                                 double cost) override;

	void CE() override;
};
} // namespace duckdb
