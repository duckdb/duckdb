//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformLeftOuterJoin2NLJoin.h
//
//	@doc:
//		Transform left outer join to left outer NLJ
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftOuterJoin2NLJoin_H
#define GPOPT_CXformLeftOuterJoin2NLJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftOuterJoin2NLJoin
//
//	@doc:
//		Transform left outer join to left outer NLJ
//
//---------------------------------------------------------------------------
class CXformLeftOuterJoin2NLJoin : public CXformImplementation
{
private:
	// private copy ctor
	CXformLeftOuterJoin2NLJoin(const CXformLeftOuterJoin2NLJoin &);

public:
	// ctor
	explicit CXformLeftOuterJoin2NLJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftOuterJoin2NLJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoin2NLJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoin2NLJoin";
	}


	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftOuterJoin2NLJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftOuterJoin2NLJoin_H

// EOF
