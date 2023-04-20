//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformAntiSemiJoinAntiSemiJoinSwap.h
//
//	@doc:
//		Swap two cascaded anti semi-joins
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformAntiSemiJoinAntiSemiJoinSwap_H
#define GPOPT_CXformAntiSemiJoinAntiSemiJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformAntiSemiJoinAntiSemiJoinSwap
//
//	@doc:
//		Swap two cascaded anti semi-joins
//
//---------------------------------------------------------------------------
class CXformAntiSemiJoinAntiSemiJoinSwap
	: public CXformJoinSwap<CLogicalLeftAntiSemiJoin, CLogicalLeftAntiSemiJoin>
{
private:
	// private copy ctor
	CXformAntiSemiJoinAntiSemiJoinSwap(
		const CXformAntiSemiJoinAntiSemiJoinSwap &);

public:
	// ctor
	explicit CXformAntiSemiJoinAntiSemiJoinSwap(CMemoryPool *mp)
		: CXformJoinSwap<CLogicalLeftAntiSemiJoin, CLogicalLeftAntiSemiJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformAntiSemiJoinAntiSemiJoinSwap()
	{
	}

	// Compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfAntiSemiJoinAntiSemiJoinSwap != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfAntiSemiJoinAntiSemiJoinSwap;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformAntiSemiJoinAntiSemiJoinSwap";
	}

};	// class CXformAntiSemiJoinAntiSemiJoinSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformSemiJoinSemiJoinSwap_H

// EOF
