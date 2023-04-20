//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformSimplifyLeftOuterJoin.h
//
//	@doc:
//		Simplify Left Outer Join with constant false predicate
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSimplifyLeftOuterJoin_H
#define GPOPT_CXformSimplifyLeftOuterJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSimplifyLeftOuterJoin
//
//	@doc:
//		Simplify Left Outer Join with constant false predicate
//
//---------------------------------------------------------------------------
class CXformSimplifyLeftOuterJoin : public CXformExploration
{
private:
	// private copy ctor
	CXformSimplifyLeftOuterJoin(const CXformSimplifyLeftOuterJoin &);

public:
	// ctor
	explicit CXformSimplifyLeftOuterJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformSimplifyLeftOuterJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSimplifyLeftOuterJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSimplifyLeftOuterJoin";
	}

	// Compatibility function for simplifying aggregates
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return (CXform::ExfSimplifyLeftOuterJoin != exfid);
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformSimplifyLeftOuterJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformSimplifyLeftOuterJoin_H

// EOF
