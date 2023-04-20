//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftOuterJoin2HashJoin.h
//
//	@doc:
//		Transform left outer join to left outer hash join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftOuterJoin2HashJoin_H
#define GPOPT_CXformLeftOuterJoin2HashJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftOuterJoin2HashJoin
//
//	@doc:
//		Transform left outer join to left outer hash join
//
//---------------------------------------------------------------------------
class CXformLeftOuterJoin2HashJoin : public CXformImplementation
{
private:
	// private copy ctor
	CXformLeftOuterJoin2HashJoin(const CXformLeftOuterJoin2HashJoin &);


public:
	// ctor
	explicit CXformLeftOuterJoin2HashJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftOuterJoin2HashJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftOuterJoin2HashJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftOuterJoin2HashJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;


	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLeftOuterJoin2HashJoin

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftOuterJoin2HashJoin_H

// EOF
