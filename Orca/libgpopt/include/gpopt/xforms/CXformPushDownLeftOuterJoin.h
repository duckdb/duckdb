//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformPushDownLeftOuterJoin.h
//
//	@doc:
//		Push LOJ below NAry join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformPushDownLeftOuterJoin_H
#define GPOPT_CXformPushDownLeftOuterJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformPushDownLeftOuterJoin
//
//	@doc:
//		Transform LOJ whose outer child is an NAry-join to be a child
//		of NAry-join
//
//---------------------------------------------------------------------------
class CXformPushDownLeftOuterJoin : public CXformExploration
{
private:
	// private copy ctor
	CXformPushDownLeftOuterJoin(const CXformPushDownLeftOuterJoin &);

public:
	// ctor
	explicit CXformPushDownLeftOuterJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformPushDownLeftOuterJoin()
	{
	}

	// xform promise
	virtual CXform::EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfPushDownLeftOuterJoin;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformPushDownLeftOuterJoin";
	}

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformPushDownLeftOuterJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformPushDownLeftOuterJoin_H

// EOF
