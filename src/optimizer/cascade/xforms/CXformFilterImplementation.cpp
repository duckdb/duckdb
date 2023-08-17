//---------------------------------------------------------------------------
//	@filename:
//		CXformFilterImplementation.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformFilterImplementation.h"

#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CXformFilterImplementation::CXformFilterImplementation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformFilterImplementation::CXformFilterImplementation() : CXformImplementation(make_uniq<LogicalFilter>()) {
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
CXform::EXformPromise CXformFilterImplementation::XformPromise(CExpressionHandle &expression_handle) const {
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
void CXformFilterImplementation::Transform(CXformContext *xform_context, CXformResult *xform_result,
                                           Operator *expression) const {
	LogicalFilter *operator_filter = static_cast<LogicalFilter *>(expression);
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for (auto &child : operator_filter->expressions) {
		v.push_back(child->Copy());
	}
	// create alternative expression
	duckdb::unique_ptr<PhysicalFilter> alternative_expression =
	    make_uniq<PhysicalFilter>(operator_filter->types, std::move(v), operator_filter->estimated_cardinality);
	for (auto &child : expression->children) {
		alternative_expression->AddChild(child->Copy());
	}
	// add alternative to transformation result
	xform_result->Add(std::move(alternative_expression));
}
} // namespace gpopt