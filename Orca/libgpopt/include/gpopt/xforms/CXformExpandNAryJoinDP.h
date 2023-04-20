//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDP.h
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoinDP_H
#define GPOPT_CXformExpandNAryJoinDP_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformExpandNAryJoinDP
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//
//---------------------------------------------------------------------------
class CXformExpandNAryJoinDP : public CXformExploration
{
private:
	// private copy ctor
	CXformExpandNAryJoinDP(const CXformExpandNAryJoinDP &);

public:
	// ctor
	explicit CXformExpandNAryJoinDP(CMemoryPool *mp);

	// dtor
	virtual ~CXformExpandNAryJoinDP()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfExpandNAryJoinDP;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformExpandNAryJoinDP";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// do stats need to be computed before applying xform?
	virtual BOOL
	FNeedsStats() const
	{
		return true;
	}

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformExpandNAryJoinDP

}  // namespace gpopt


#endif	// !GPOPT_CXformExpandNAryJoinDP_H

// EOF
