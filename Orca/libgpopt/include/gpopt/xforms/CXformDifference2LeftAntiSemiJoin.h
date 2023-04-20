//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformDifference2LeftAntiSemiJoin.h
//
//	@doc:
//		Class to transform logical difference into an aggregate over left anti-semi join
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformDifference2DifferenceAll_H
#define GPOPT_CXformDifference2DifferenceAll_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformDifference2LeftAntiSemiJoin
//
//	@doc:
//		Class to transform logical difference into an aggregate over
//		left anti-semi join
//
//---------------------------------------------------------------------------
class CXformDifference2LeftAntiSemiJoin : public CXformExploration
{
private:
	// private copy ctor
	CXformDifference2LeftAntiSemiJoin(
		const CXformDifference2LeftAntiSemiJoin &);

public:
	// ctor
	explicit CXformDifference2LeftAntiSemiJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformDifference2LeftAntiSemiJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfDifference2LeftAntiSemiJoin;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformDifference2LeftAntiSemiJoin";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &  // exprhdl
	) const
	{
		return CXform::ExfpHigh;
	}

	// actual transform
	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformDifference2LeftAntiSemiJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformDifference2DifferenceAll_H

// EOF
