//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDynamicIndexGet2DynamicIndexScan.h
//
//	@doc:
//		Transform DynamicIndexGet to DynamicIndexScan
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformDynamicIndexGet2DynamicIndexScan_H
#define GPOPT_CXformDynamicIndexGet2DynamicIndexScan_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformDynamicIndexGet2DynamicIndexScan
//
//	@doc:
//		Transform DynamicIndexGet to DynamicIndexScan
//
//---------------------------------------------------------------------------
class CXformDynamicIndexGet2DynamicIndexScan : public CXformImplementation
{
private:
	// private copy ctor
	CXformDynamicIndexGet2DynamicIndexScan(
		const CXformDynamicIndexGet2DynamicIndexScan &);

public:
	// ctor
	explicit CXformDynamicIndexGet2DynamicIndexScan(CMemoryPool *mp);

	// dtor
	virtual ~CXformDynamicIndexGet2DynamicIndexScan()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfDynamicIndexGet2DynamicIndexScan;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformDynamicIndexGet2DynamicIndexScan";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformDynamicIndexGet2DynamicIndexScan

}  // namespace gpopt


#endif	// !GPOPT_CXformDynamicIndexGet2DynamicIndexScan_H

// EOF
