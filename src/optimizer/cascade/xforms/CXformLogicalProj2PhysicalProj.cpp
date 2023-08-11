//---------------------------------------------------------------------------
//	@filename:
//		CXformGet2TableScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformLogicalProj2PhysicalProj.h"

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::CXformGet2TableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformLogicalProj2PhysicalProj::CXformLogicalProj2PhysicalProj()
    : CXformImplementation(make_uniq<LogicalProjection>(0, duckdb::vector<duckdb::unique_ptr<Expression>>())) {
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
CXform::EXformPromise CXformLogicalProj2PhysicalProj::XformPromise(CExpressionHandle &expression_handle) const {
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
void CXformLogicalProj2PhysicalProj::Transform(CXformContext *pxfctxt, CXformResult *pxfres, Operator *pexpr) const {
	LogicalProjection *operator_get = static_cast<LogicalProjection *>(pexpr);
	// create/extract components for alternative
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for (auto &child : operator_get->expressions) {
		v.push_back(child->Copy());
	}
	// create alternative expression
	duckdb::unique_ptr<Operator> alternative_expression =
	    make_uniq<PhysicalProjection>(operator_get->types, std::move(v), operator_get->estimated_cardinality);
	for (auto &child : pexpr->children) {
		alternative_expression->AddChild(child->Copy());
	}
	// add alternative to transformation result
	pxfres->Add(std::move(alternative_expression));
}
} // namespace gpopt