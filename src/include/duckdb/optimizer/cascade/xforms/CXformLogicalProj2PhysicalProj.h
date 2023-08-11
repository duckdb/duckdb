//---------------------------------------------------------------------------
//	@filename:
//		CXformLogicalProj2PhysicalProj.h
//
//	@doc:
//		Transform Logical Projection to Physical Projection
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLogicalProj2PhysicalProj_H
#define GPOPT_CXformLogicalProj2PhysicalProj_H

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
class CXformLogicalProj2PhysicalProj : public CXformImplementation {
public:
	// ctor
	explicit CXformLogicalProj2PhysicalProj();

	CXformLogicalProj2PhysicalProj(const CXformLogicalProj2PhysicalProj &) = delete;

	// dtor
	~CXformLogicalProj2PhysicalProj() override = default;
	// ident accessors
	EXformId ID() const override {
		return ExfLogicalProj2PhysicalProj;
	}
	// return a string for xform name
	const CHAR *Name() const override {
		return "CXformLogicalProj2PhysicalProj";
	}
	// compute xform promise for a given expression handle
	EXformPromise XformPromise(CExpressionHandle &expression_handle) const override;
	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres, Operator *pexpr) const override;
};
} // namespace gpopt
#endif