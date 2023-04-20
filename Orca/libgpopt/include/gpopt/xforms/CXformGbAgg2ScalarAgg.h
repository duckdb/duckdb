//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformGbAgg2ScalarAgg.h
//
//	@doc:
//		Transform GbAgg to ScalarAgg
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformGbAgg2ScalarAgg_H
#define GPOPT_CXformGbAgg2ScalarAgg_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformGbAgg2ScalarAgg
//
//	@doc:
//		Transform Get to TableScan
//
//---------------------------------------------------------------------------
class CXformGbAgg2ScalarAgg : public CXformImplementation
{
private:
	// private copy ctor
	CXformGbAgg2ScalarAgg(const CXformGbAgg2ScalarAgg &);

public:
	// ctor
	CXformGbAgg2ScalarAgg(CMemoryPool *mp);

	// dtor
	virtual ~CXformGbAgg2ScalarAgg()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfGbAgg2ScalarAgg;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformGbAgg2ScalarAgg";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbAgg2ScalarAgg

}  // namespace gpopt


#endif	// !GPOPT_CXformGbAgg2ScalarAgg_H

// EOF
