//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInlineCTEConsumer.h
//
//	@doc:
//		Transform logical CTE consumer to a copy of the expression under its
//		corresponding producer
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInlineCTEConsumer_H
#define GPOPT_CXformInlineCTEConsumer_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInlineCTEConsumer
//
//	@doc:
//		Transform logical CTE consumer to a copy of the expression under its
//		corresponding producer
//
//---------------------------------------------------------------------------
class CXformInlineCTEConsumer : public CXformExploration
{
private:
	// private copy ctor
	CXformInlineCTEConsumer(const CXformInlineCTEConsumer &);

public:
	// ctor
	explicit CXformInlineCTEConsumer(CMemoryPool *mp);

	// dtor
	virtual ~CXformInlineCTEConsumer()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInlineCTEConsumer;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformInlineCTEConsumer";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformInlineCTEConsumer
}  // namespace gpopt

#endif	// !GPOPT_CXformInlineCTEConsumer_H

// EOF
