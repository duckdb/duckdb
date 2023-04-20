//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CXformImplementBitmapTableGet
//
//	@doc:
//		Implement BitmapTableGet
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CXformImplementBitmapTableGet_H
#define GPOPT_CXformImplementBitmapTableGet_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CXformImplementBitmapTableGet
//
//	@doc:
//		Implement CLogicalBitmapTableGet as a CPhysicalBitmapTableScan
//
//---------------------------------------------------------------------------
class CXformImplementBitmapTableGet : public CXformImplementation
{
private:
	// disable copy ctor
	CXformImplementBitmapTableGet(const CXformImplementBitmapTableGet &);

public:
	// ctor
	explicit CXformImplementBitmapTableGet(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementBitmapTableGet()
	{
	}

	// identifier
	virtual EXformId
	Exfid() const
	{
		return ExfImplementBitmapTableGet;
	}

	// xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementBitmapTableGet";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformImplementBitmapTableGet
}  // namespace gpopt

#endif	// !GPOPT_CXformImplementBitmapTableGet_H

// EOF
