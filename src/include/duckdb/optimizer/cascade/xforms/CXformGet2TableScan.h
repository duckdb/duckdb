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

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGet2TableScan
//
//	@doc:
//		Transform Get to TableScan
//
//---------------------------------------------------------------------------
class CXformGet2TableScan : public CXformImplementation
{
private:
	// private copy ctor
	CXformGet2TableScan(const CXformGet2TableScan &);

public:
	// ctor
	explicit CXformGet2TableScan();

	// dtor
	virtual ~CXformGet2TableScan()
	{
	}

	// ident accessors
	virtual EXformId Exfid() const
	{
		return ExfGet2TableScan;
	}

	// return a string for xform name
	virtual const CHAR* SzId() const
	{
		return "CXformGet2TableScan";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const override;
};
}  // namespace gpopt

#endif