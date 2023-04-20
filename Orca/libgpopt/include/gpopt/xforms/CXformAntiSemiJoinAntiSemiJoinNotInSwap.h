//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformAntiSemiJoinAntiSemiJoinNotInSwap.h
//
//	@doc:
//		Swap cascaded anti semi-join and anti semi-join (NotIn)
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformAntiSemiJoinAntiSemiJoinNotInSwap_H
#define GPOPT_CXformAntiSemiJoinAntiSemiJoinNotInSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformAntiSemiJoinAntiSemiJoinNotInSwap
//
//	@doc:
//		Swap cascaded anti semi-join and anti semi-join (NotIn)
//
//---------------------------------------------------------------------------
class CXformAntiSemiJoinAntiSemiJoinNotInSwap
	: public CXformJoinSwap<CLogicalLeftAntiSemiJoin,
							CLogicalLeftAntiSemiJoinNotIn>
{
private:
	// private copy ctor
	CXformAntiSemiJoinAntiSemiJoinNotInSwap(
		const CXformAntiSemiJoinAntiSemiJoinNotInSwap &);

public:
	// ctor
	explicit CXformAntiSemiJoinAntiSemiJoinNotInSwap(CMemoryPool *mp)
		: CXformJoinSwap<CLogicalLeftAntiSemiJoin,
						 CLogicalLeftAntiSemiJoinNotIn>(mp)
	{
	}

	// dtor
	virtual ~CXformAntiSemiJoinAntiSemiJoinNotInSwap()
	{
	}

	// compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfAntiSemiJoinNotInAntiSemiJoinSwap != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfAntiSemiJoinAntiSemiJoinNotInSwap;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformAntiSemiJoinAntiSemiJoinNotInSwap";
	}

};	// class CXformAntiSemiJoinAntiSemiJoinNotInSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformAntiSemiJoinAntiSemiJoinNotInSwap_H

// EOF
