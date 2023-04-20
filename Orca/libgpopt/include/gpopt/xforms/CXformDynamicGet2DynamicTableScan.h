//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDynamicGet2DynamicTableScan.h
//
//	@doc:
//		Transform DynamicGet to DynamicTableScan
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformDynamicGet2DynamicTableScan_H
#define GPOPT_CXformDynamicGet2DynamicTableScan_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformDynamicGet2DynamicTableScan
//
//	@doc:
//		Transform DynamicGet to DynamicTableScan
//
//---------------------------------------------------------------------------
class CXformDynamicGet2DynamicTableScan : public CXformImplementation
{
private:
	// private copy ctor
	CXformDynamicGet2DynamicTableScan(
		const CXformDynamicGet2DynamicTableScan &);

public:
	// ctor
	explicit CXformDynamicGet2DynamicTableScan(CMemoryPool *mp);

	// dtor
	virtual ~CXformDynamicGet2DynamicTableScan()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfDynamicGet2DynamicTableScan;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformDynamicGet2DynamicTableScan";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &  // exprhdl
	) const
	{
		return CXform::ExfpHigh;
	}

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformDynamicGet2DynamicTableScan

}  // namespace gpopt


#endif	// !GPOPT_CXformDynamicGet2DynamicTableScan_H

// EOF
