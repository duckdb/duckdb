//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinMinCard.h
//
//	@doc:
//		Expand n-ary join into series of binary joins while minimizing
//		cardinality of intermediate results
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoinMinCard_H
#define GPOPT_CXformExpandNAryJoinMinCard_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformExpandNAryJoinMinCard
//
//	@doc:
//		Expand n-ary join into series of binary joins while minimizing
//		cardinality of intermediate results
//
//---------------------------------------------------------------------------
class CXformExpandNAryJoinMinCard : public CXformExploration
{
private:
	// private copy ctor
	CXformExpandNAryJoinMinCard(const CXformExpandNAryJoinMinCard &);

public:
	// ctor
	explicit CXformExpandNAryJoinMinCard(CMemoryPool *mp);

	// dtor
	virtual ~CXformExpandNAryJoinMinCard()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfExpandNAryJoinMinCard;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformExpandNAryJoinMinCard";
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

	BOOL
	IsApplyOnce()
	{
		return true;
	}
};	// class CXformExpandNAryJoinMinCard

}  // namespace gpopt


#endif	// !GPOPT_CXformExpandNAryJoinMinCard_H

// EOF
