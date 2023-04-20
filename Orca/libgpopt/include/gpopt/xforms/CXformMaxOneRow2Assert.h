//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CXformMaxOneRow2Assert.h
//
//	@doc:
//		Transform MaxOneRow into LogicalAssert
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformMaxOneRow2Assert_H
#define GPOPT_CXformMaxOneRow2Assert_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformMaxOneRow2Assert
//
//	@doc:
//		Transform MaxOneRow into LogicalAssert
//
//---------------------------------------------------------------------------
class CXformMaxOneRow2Assert : public CXformExploration
{
private:
	// private copy ctor
	CXformMaxOneRow2Assert(const CXformMaxOneRow2Assert &);

public:
	// ctor
	explicit CXformMaxOneRow2Assert(CMemoryPool *mp);

	// dtor
	virtual ~CXformMaxOneRow2Assert()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfMaxOneRow2Assert;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformMaxOneRow2Assert";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformMaxOneRow2Assert
}  // namespace gpopt

#endif	// !GPOPT_CXformMaxOneRow2Assert_H

// EOF
