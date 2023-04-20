//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformDifferenceAll2LeftAntiSemiJoin.h
//
//	@doc:
//		Class to transform logical difference all into a LASJ
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformDifferenceAll2LeftAntiSemiJoin_H
#define GPOPT_CXformDifferenceAll2LeftAntiSemiJoin_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformDifferenceAll2LeftAntiSemiJoin
//
//	@doc:
//		Class to transform logical difference all into a LASJ
//
//---------------------------------------------------------------------------
class CXformDifferenceAll2LeftAntiSemiJoin : public CXformExploration
{
private:
	// private copy ctor
	CXformDifferenceAll2LeftAntiSemiJoin(
		const CXformDifferenceAll2LeftAntiSemiJoin &);

public:
	// ctor
	explicit CXformDifferenceAll2LeftAntiSemiJoin(CMemoryPool *mp);

	// dtor
	virtual ~CXformDifferenceAll2LeftAntiSemiJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfDifferenceAll2LeftAntiSemiJoin;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformDifferenceAll2LeftAntiSemiJoin";
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

};	// class CXformDifferenceAll2LeftAntiSemiJoin

}  // namespace gpopt

#endif	// !GPOPT_CXformDifferenceAll2LeftAntiSemiJoin_H

// EOF
