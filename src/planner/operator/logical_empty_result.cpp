#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

namespace duckdb
{
LogicalEmptyResult::LogicalEmptyResult(unique_ptr<LogicalOperator> op)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT)
{
	this->bindings = op->GetColumnBindings();
	op->ResolveOperatorTypes();
	this->return_types = op->types;
}

LogicalEmptyResult::LogicalEmptyResult()
	: LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT)
{
}

void LogicalEmptyResult::Serialize(FieldWriter &writer) const
{
	writer.WriteRegularSerializableList(return_types);
	writer.WriteList<ColumnBinding>(bindings);
}

unique_ptr<LogicalOperator> LogicalEmptyResult::Deserialize(LogicalDeserializationState &state, FieldReader &reader)
{
	auto return_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	auto bindings = reader.ReadRequiredList<ColumnBinding>();
	auto result = unique_ptr<LogicalEmptyResult>(new LogicalEmptyResult());
	result->return_types = return_types;
	result->bindings = bindings;
	return std::move(result);
}

CPropConstraint* LogicalEmptyResult::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	return NULL;
}

ULONG LogicalEmptyResult::DeriveJoinDepth(CExpressionHandle &exprhdl)
{
	return 0;
}
	
// Rehydrate expression from a given cost context and child expressions
Operator* LogicalEmptyResult::SelfRehydrate(CCostContext* pcc, duckdb::vector<Operator*> pdrgpexpr, CDrvdPropCtxtPlan* pdpctxtplan)
{
	LogicalEmptyResult* pexpr = new LogicalEmptyResult(unique_ptr_cast<Operator, LogicalOperator>(pdrgpexpr[0]->Copy()));
	pexpr->m_cost = pcc->m_cost;
	pexpr->m_pgexpr = pcc->m_pgexpr;
	return pexpr;
}

CKeyCollection* LogicalEmptyResult::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	return NULL;
}

//-------------------------------------------------------------------------------------
// Transformations
//-------------------------------------------------------------------------------------
// candidate set of xforms
CXformSet* LogicalEmptyResult::PxfsCandidates() const
{
	CXformSet* xform_set = new CXformSet();
	return xform_set;
}
} // namespace duckdb
