//---------------------------------------------------------------------------
//	@filename:
//		CXformGet2TableScan.h
//
//	@doc:
//		Transform Get to TableScan
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformGet2TableScan_H
#define GPOPT_CXformGet2TableScan_H

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
class CXformGet2TableScan : public CXformImplementation {
private:
	// private copy ctor
	CXformGet2TableScan(const CXformGet2TableScan &);

public:
	// ctor
	explicit CXformGet2TableScan();

	// dtor
	~CXformGet2TableScan() override = default;

	// ident accessors
	EXformId ID() const override {
		return ExfGet2TableScan;
	}

	// return a string for xform name
	const CHAR *Name() const override {
		return "CXformGet2TableScan";
	}

	// compute xform promise for a given expression handle
	EXformPromise XformPromise(CExpressionHandle &expression_handle) const override;

	// actual transform
	void Transform(CXformContext *xform_context, CXformResult *xform_result, Operator *expression) const override;
};
} // namespace gpopt

#endif