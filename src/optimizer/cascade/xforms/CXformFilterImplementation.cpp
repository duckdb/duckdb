//---------------------------------------------------------------------------
//	@filename:
//		CXformFilterImplementation.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformFilterImplementation.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformFilterImplementation::CXformFilterImplementation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformFilterImplementation::CXformFilterImplementation()
    :CXformImplementation(make_uniq<LogicalFilter>())
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
CXform::EXformPromise CXformFilterImplementation::Exfp(CExpressionHandle &exprhdl) const
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
void CXformFilterImplementation::Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const
{
	LogicalFilter* popFilter = (LogicalFilter*)pexpr;
    duckdb::vector<duckdb::unique_ptr<Expression>> v;
     for(auto &child : popFilter->expressions)
    {
        v.push_back(child->Copy());
    }
	// create alternative expression
	duckdb::unique_ptr<Operator> pexprAlt = make_uniq<PhysicalFilter>(popFilter->types, std::move(v), popFilter->estimated_cardinality);
    for(auto &child : pexpr->children)
    {
        pexprAlt->AddChild(child->Copy());
    }
	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}