//---------------------------------------------------------------------------
//	@filename:
//		CXformGet2TableScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformInnerJoin2HashJoin.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/joinside.hpp"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::CXformGet2TableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerJoin2HashJoin::CXformInnerJoin2HashJoin()
	: CXformImplementation(make_uniq<LogicalComparisonJoin>(JoinType::INNER))
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
CXform::EXformPromise CXformInnerJoin2HashJoin::XformPromise(CExpressionHandle &exprhdl) const
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
void CXformInnerJoin2HashJoin::Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const
{
	LogicalComparisonJoin* popJoin = (LogicalComparisonJoin*)pexpr;
    PerfectHashJoinStats perfect_join_stats;
	duckdb::vector<JoinCondition> v;
	for(auto &child : popJoin->conditions) {
		JoinCondition jc;
		jc.left = child.left->Copy();
		jc.right = child.right->Copy();
		jc.comparison = child.comparison;
		v.push_back(std::move(jc));
	}
	// create alternative expression
	duckdb::unique_ptr<PhysicalHashJoin> pexprAlt = make_uniq<PhysicalHashJoin>(*popJoin, popJoin->children[0]->Copy(), popJoin->children[1]->Copy(), 
                                                                         		std::move(v), popJoin->join_type,
                                                                        		popJoin->left_projection_map, popJoin->right_projection_map,
                                                                        		popJoin->delim_types, popJoin->estimated_cardinality,
																				perfect_join_stats);
	pexprAlt->v_column_binding = popJoin->GetColumnBindings();
	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}