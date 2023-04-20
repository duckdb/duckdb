//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformInnerJoin2HashJoin.h
//
//	@doc:
//		Transform inner join to inner Hash Join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoin2HashJoin_H
#define GPOPT_CXformInnerJoin2HashJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoin2HashJoin
//
//	@doc:
//		Transform inner join to inner Hash Join
//
//---------------------------------------------------------------------------
class CXformInnerJoin2HashJoin : public CXformImplementation
{
private:
	// private copy ctor
	CXformInnerJoin2HashJoin(const CXformInnerJoin2HashJoin &);


public:
	// ctor
	explicit CXformInnerJoin2HashJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformInnerJoin2HashJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoin2HashJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoin2HashJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformInnerJoin2HashJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformInnerJoin2HashJoin_H

// EOF
