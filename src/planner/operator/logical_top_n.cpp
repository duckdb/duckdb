#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

namespace duckdb {

void LogicalTopN::Serialize(FieldWriter &writer) const {
	writer.WriteRegularSerializableList(orders);
	writer.WriteField(offset);
	writer.WriteField(limit);
}

unique_ptr<LogicalOperator> LogicalTopN::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(state.gstate);
	auto offset = reader.ReadRequired<idx_t>();
	auto limit = reader.ReadRequired<idx_t>();
	return make_uniq<LogicalTopN>(std::move(orders), limit, offset);
}

idx_t LogicalTopN::EstimateCardinality(ClientContext &context)
{
	auto child_cardinality = LogicalOperator::EstimateCardinality(context);
	if (limit >= 0 && child_cardinality < idx_t(limit))
	{
		return limit;
	}
	return child_cardinality;
}

CKeyCollection* LogicalTopN::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	const ULONG arity = exprhdl.Arity();
	return PkcDeriveKeysPassThru(exprhdl, arity - 1);
}
	
CPropConstraint* LogicalTopN::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	return PpcDeriveConstraintPassThru(exprhdl, exprhdl.Arity() - 1);
}

Operator* LogicalTopN::SelfRehydrate(CCostContext* pcc, duckdb::vector<Operator*> pdrgpexpr, CDrvdPropCtxtPlan* pdpctxtplan)
{
	CGroupExpression* pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	LogicalTopN* tmp = (LogicalTopN*)pgexpr->m_pop.get();
	LogicalTopN* pexpr = new LogicalTopN(tmp->orders, tmp->limit, tmp->offset);
	for(auto &child : pdrgpexpr)
	{
		pexpr->AddChild(child->Copy());
	}
	pexpr->m_cost = cost;
	pexpr->m_group_expression = pgexpr;
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet* LogicalTopN::PxfsCandidates() const
{
	CXformSet* xform_set = new CXformSet();
	(void) xform_set->set(CXform::ExfImplementLimit);
	(void) xform_set->set(CXform::ExfSplitLimit);
	return xform_set;
}
} // namespace duckdb
