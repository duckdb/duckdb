//---------------------------------------------------------------------------
//	@filename:
//		CXformJoinCommutativity.cpp
//
//	@doc:
//		Implementation of join commute transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformJoinCommutativity.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/joinside.hpp"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformJoinCommutativity::CXformJoinCommutativity
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformJoinCommutativity::CXformJoinCommutativity()
	: CXformExploration(make_uniq<LogicalComparisonJoin>(JoinType::INNER))
{
	this->m_operator->AddChild(make_uniq<CPatternLeaf>());
	this->m_operator->AddChild(make_uniq<CPatternLeaf>());
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::XformPromise
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise CXformJoinCommutativity::XformPromise(CExpressionHandle &exprhdl) const
{
	return CXform::ExfpMedium;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformJoinCommutativity::Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const
{
	LogicalComparisonJoin* popJoin = (LogicalComparisonJoin*)pexpr;
	
	// create alternative expression
	duckdb::unique_ptr<LogicalComparisonJoin> pexprAlt = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	/* LogicalComparisonJoin fields */
	for(auto &child : popJoin->conditions) {
		JoinCondition jc;
		jc.left = child.right->Copy();
		jc.right = child.left->Copy();
		jc.comparison = child.comparison;
		pexprAlt->conditions.emplace_back(std::move(jc));
	}
	pexprAlt->delim_types = popJoin->delim_types;
	pexprAlt->estimated_cardinality = popJoin->estimated_cardinality;
	
	/* LogicalJoin fields */
	pexprAlt->mark_index = popJoin->mark_index;
	pexprAlt->left_projection_map = popJoin->right_projection_map;
	pexprAlt->right_projection_map = popJoin->left_projection_map;
	
	/* Operator fields */
	pexprAlt->m_derived_property_relation = popJoin->m_derived_property_relation;
	pexprAlt->m_derived_property_plan = popJoin->m_derived_property_plan;
	pexprAlt->m_required_plan_property = popJoin->m_required_plan_property;
	if (nullptr != popJoin->estimated_props) {
		pexprAlt->estimated_props = popJoin->estimated_props->Copy();
	}
	pexprAlt->AddChild(popJoin->children[1]->Copy());
	pexprAlt->AddChild(popJoin->children[0]->Copy());
	pexprAlt->ResolveTypes();
	pexprAlt->estimated_cardinality = popJoin->estimated_cardinality;
	for (auto &child : popJoin->expressions) {
		pexprAlt->expressions.push_back(child->Copy());
	}
	pexprAlt->has_estimated_cardinality = popJoin->has_estimated_cardinality;
	pexprAlt->m_cost = GPOPT_INVALID_COST;
	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}