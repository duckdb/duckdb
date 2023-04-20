//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CXformImplementSplit.h
//
//	@doc:
//		Transform Logical Split to Physical Split
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementSplit_H
#define GPOPT_CXformImplementSplit_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformImplementSplit
//
//	@doc:
//		Transform Logical Split to Physical Split
//
//---------------------------------------------------------------------------
class CXformImplementSplit : public CXformImplementation
{
private:
	// private copy ctor
	CXformImplementSplit(const CXformImplementSplit &);

public:
	// ctor
	explicit CXformImplementSplit(CMemoryPool *mp);

	// dtor
	virtual ~CXformImplementSplit()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementSplit;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementSplit";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformImplementSplit
}  // namespace gpopt

#endif	// !GPOPT_CXformImplementSplit_H

// EOF
