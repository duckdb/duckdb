//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementCTEConsumer.h
//
//	@doc:
//		Transform Logical CTE Consumer to Physical CTE Consumer
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementCTEConsumer_H
#define GPOPT_CXformImplementCTEConsumer_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementCTEConsumer
//
//	@doc:
//		Transform Logical CTE Consumer to Physical CTE Consumer
//
//---------------------------------------------------------------------------
class CXformImplementCTEConsumer : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementCTEConsumer(const CXformImplementCTEConsumer &);

public:
	// ctor
	explicit CXformImplementCTEConsumer(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementCTEConsumer()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementCTEConsumer;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementCTEConsumer";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformImplementCTEConsumer
}  // namespace gpopt

#endif	// !GPOPT_CXformImplementCTEConsumer_H

// EOF
