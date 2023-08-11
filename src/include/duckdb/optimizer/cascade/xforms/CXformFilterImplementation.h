//---------------------------------------------------------------------------
//	@filename:
//		CXformFilterImplementation.h
//
//	@doc:
//		Transform Logical Projection to Physical Projection
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformFilterImplementation_H
#define GPOPT_CXformFilterImplementation_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXformImplementation.h"

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGet2TableScan
//
//	@doc:
//		Transform Get to TableScan
//
//---------------------------------------------------------------------------
class CXformFilterImplementation : public CXformImplementation {
public:
	// ctor
	explicit CXformFilterImplementation();

	CXformFilterImplementation(const CXformFilterImplementation &) = delete;

	// dtor
	~CXformFilterImplementation() override = default;

	// ident accessors
	EXformId ID() const override {
		return ExfFilterImplementation;
	}

	// return a string for xform name
	const CHAR *Name() const override {
		return "CXformFilterImplementation";
	}

	// compute xform promise for a given expression handle
	EXformPromise XformPromise(CExpressionHandle &expression_handle) const override;

	// actual transform
	void Transform(CXformContext *xform_context, CXformResult *xform_result, Operator *pexpr) const override;
};
} // namespace gpopt
#endif