//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftSemiJoin2HashJoin.h
//
//	@doc:
//		Transform left semi join to left semi hash join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiJoin2HashJoin_H
#define GPOPT_CXformLeftSemiJoin2HashJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftSemiJoin2HashJoin
//
//	@doc:
//		Transform left semi join to left semi hash join
//
//---------------------------------------------------------------------------
class CXformLeftSemiJoin2HashJoin : public CXformImplementation
{
private:
	// private copy ctor
	CXformLeftSemiJoin2HashJoin(const CXformLeftSemiJoin2HashJoin &);

public:
	// ctor
	explicit CXformLeftSemiJoin2HashJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftSemiJoin2HashJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftSemiJoin2HashJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftSemiJoin2HashJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftSemiJoin2HashJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftSemiJoin2HashJoin_H

// EOF
