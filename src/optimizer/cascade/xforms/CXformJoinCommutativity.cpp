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
	return CXform::ExfpHigh;
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
	duckdb::vector<duckdb::unique_ptr<Operator>> reordered_childs(2);
	reordered_childs[1] = popJoin->children[0]->Copy();
	reordered_childs[0] = popJoin->children[1]->Copy();
	// create alternative expression
	duckdb::unique_ptr<Operator> pexprAlt = popJoin->CopyWithNewChildren(popJoin->m_group_expression, std::move(reordered_childs), GPOPT_INVALID_COST);
	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}