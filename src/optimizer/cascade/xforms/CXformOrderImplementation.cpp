//---------------------------------------------------------------------------
//	@filename:
//		CXformOrderImplementation.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformOrderImplementation.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformOrderImplementation::CXformOrderImplementation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformOrderImplementation::CXformOrderImplementation()
    :CXformImplementation(make_uniq<LogicalOrder>(duckdb::vector<BoundOrderByNode>()))
{
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
CXform::EXformPromise CXformOrderImplementation::XformPromise(CExpressionHandle &exprhdl) const
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
void CXformOrderImplementation::Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const
{
	LogicalOrder* popOrder = (LogicalOrder*)pexpr;
    duckdb::vector<BoundOrderByNode> vorders;
    for(auto &child : popOrder->orders)
    {
        vorders.push_back(child.Copy());
    }
    duckdb::vector<idx_t> projections;
    if (popOrder->projections.empty())
	{
		for (idx_t i = 0; i < popOrder->types.size(); i++)
		{
			projections.push_back(i);
		}
	}
	else
	{
        for(auto child : popOrder->projections) {
		    projections.push_back(child);
        }
	}
	// create alternative expression
	duckdb::unique_ptr<PhysicalOrder> pexprAlt = make_uniq<PhysicalOrder>(popOrder->types, std::move(vorders), std::move(projections), popOrder->estimated_cardinality);
    for(auto &child : pexpr->children)
    {
        pexprAlt->AddChild(child->Copy());
    }
	// add alternative to transformation result
	pxfres->Add(std::move(pexprAlt));
}