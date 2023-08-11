#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"

namespace duckdb {
LogicalFilter::LogicalFilter()
	: LogicalOperator(LogicalOperatorType::LOGICAL_FILTER)
{
	m_derived_property_relation = new CDrvdPropRelational();
	m_group_expression = nullptr;
	m_derived_property_plan = nullptr;
	m_required_plan_property = nullptr;
}

LogicalFilter::LogicalFilter(unique_ptr<Expression> expression)
	: LogicalOperator(LogicalOperatorType::LOGICAL_FILTER)
{
	m_derived_property_relation = new CDrvdPropRelational();
	m_group_expression = nullptr;
	m_derived_property_plan = nullptr;
	m_required_plan_property = nullptr;
	expressions.push_back(std::move(expression));
	SplitPredicates(expressions);
}

void LogicalFilter::ResolveTypes()
{
	types = MapTypes(children[0]->types, projection_map);
}

vector<ColumnBinding> LogicalFilter::GetColumnBindings()
{
	return MapBindings(children[0]->GetColumnBindings(), projection_map);
}

// Split the predicates separated by AND statements
// These are the predicates that are safe to push down because all of them MUST
// be true
bool LogicalFilter::SplitPredicates(vector<unique_ptr<Expression>> &expressions)
{
	bool found_conjunction = false;
	for (idx_t i = 0; i < expressions.size(); i++)
	{
		if (expressions[i]->type == ExpressionType::CONJUNCTION_AND)
		{
			auto &conjunction = expressions[i]->Cast<BoundConjunctionExpression>();
			found_conjunction = true;
			// AND expression, append the other children
			for (idx_t k = 1; k < conjunction.children.size(); k++)
			{
				expressions.push_back(std::move(conjunction.children[k]));
			}
			// replace this expression with the first child of the conjunction
			expressions[i] = std::move(conjunction.children[0]);
			// we move back by one so the right child is checked again
			// in case it is an AND expression as well
			i--;
		}
	}
	return found_conjunction;
}

void LogicalFilter::Serialize(FieldWriter &writer) const
{
	writer.WriteSerializableList<Expression>(expressions);
	writer.WriteList<idx_t>(projection_map);
}

unique_ptr<LogicalOperator> LogicalFilter::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	auto projection_map = reader.ReadRequiredList<idx_t>();
	auto result = make_uniq<LogicalFilter>();
	result->expressions = std::move(expressions);
	result->projection_map = std::move(projection_map);
	return std::move(result);
}

CKeyCollection* LogicalFilter::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	return PkcDeriveKeysPassThru(exprhdl, 0);
}

// derive constraint property
CPropConstraint* LogicalFilter::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	return PpcDeriveConstraintFromPredicates(exprhdl);
}

// Rehydrate expression from a given cost context and child expressions
Operator* LogicalFilter::SelfRehydrate(CCostContext* pcc, duckdb::vector<Operator*> pdrgpexpr, CDrvdPropCtxtPlan* pdpctxtplan)
{
	CGroupExpression* pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	LogicalFilter* pexpr = new LogicalFilter();
	pexpr->expressions = std::move(pgexpr->m_pop->expressions);
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
//		LogicalFilter::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet* LogicalFilter::PxfsCandidates() const
{
	CXformSet* xform_set = new CXformSet();
	(void) xform_set->set(CXform::ExfSelect2Apply);
	(void) xform_set->set(CXform::ExfRemoveSubqDistinct);
	(void) xform_set->set(CXform::ExfInlineCTEConsumerUnderSelect);
	(void) xform_set->set(CXform::ExfPushGbWithHavingBelowJoin);
	(void) xform_set->set(CXform::ExfSelect2IndexGet);
	(void) xform_set->set(CXform::ExfSelect2DynamicIndexGet);
	(void) xform_set->set(CXform::ExfSelect2PartialDynamicIndexGet);
	(void) xform_set->set(CXform::ExfSelect2BitmapBoolOp);
	(void) xform_set->set(CXform::ExfSelect2DynamicBitmapBoolOp);
	(void) xform_set->set(CXform::ExfSimplifySelectWithSubquery);
	(void) xform_set->set(CXform::ExfSelect2Filter);
	return xform_set;
}
} // namespace duckdb