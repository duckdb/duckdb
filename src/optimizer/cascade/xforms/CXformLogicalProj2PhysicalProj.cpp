//---------------------------------------------------------------------------
//	@filename:
//		CXformGet2TableScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformLogicalProj2PhysicalProj.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::CXformGet2TableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformLogicalProj2PhysicalProj::CXformLogicalProj2PhysicalProj()
	: CXformImplementation(make_uniq<LogicalProjection>(0, std::move(duckdb::vector<duckdb::unique_ptr<Expression>>())))
{
	this->m_pop->AddChild(make_uniq<CPatternLeaf>());
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise CXformLogicalProj2PhysicalProj::Exfp(CExpressionHandle &exprhdl) const
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
void CXformLogicalProj2PhysicalProj::Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const
{
	LogicalProjection* popGet = (LogicalProjection*)pexpr;
	// create/extract components for alternative
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
    for(auto &child : popGet->expressions)
    {
        v.push_back(child->Copy());
    }
	// create alternative expression
	duckdb::unique_ptr<Operator> pexprAlt = make_uniq<PhysicalProjection>(popGet->types, std::move(v), popGet->estimated_cardinality);
    for(auto &child : pexpr->children)
    {
        pexprAlt->AddChild(child->Copy());
    }
	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}