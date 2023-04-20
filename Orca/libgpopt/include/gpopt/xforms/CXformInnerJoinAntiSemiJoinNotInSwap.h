//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformInnerJoinAntiSemiJoinNotInSwap.h
//
//	@doc:
//		Swap cascaded inner join and anti semi-join with NotIn semantics
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoinAntiSemiJoinNotInSwap_H
#define GPOPT_CXformInnerJoinAntiSemiJoinNotInSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoinAntiSemiJoinNotInSwap
//
//	@doc:
//		Swap cascaded inner join and anti semi-join with NotIn semantics
//
//---------------------------------------------------------------------------
class CXformInnerJoinAntiSemiJoinNotInSwap
	: public CXformJoinSwap<CLogicalInnerJoin, CLogicalLeftAntiSemiJoinNotIn>
{
private:
	// private copy ctor
	CXformInnerJoinAntiSemiJoinNotInSwap(
		const CXformInnerJoinAntiSemiJoinNotInSwap &);

public:
	// ctor
	explicit CXformInnerJoinAntiSemiJoinNotInSwap(CMemoryPool *mp)
		: CXformJoinSwap<CLogicalInnerJoin, CLogicalLeftAntiSemiJoinNotIn>(mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoinAntiSemiJoinNotInSwap()
	{
	}

	// Compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfAntiSemiJoinNotInInnerJoinSwap != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoinAntiSemiJoinNotInSwap;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoinAntiSemiJoinNotInSwap";
	}

};	// class CXformInnerJoinAntiSemiJoinNotInSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformInnerJoinAntiSemiJoinNotInSwap_H

// EOF
