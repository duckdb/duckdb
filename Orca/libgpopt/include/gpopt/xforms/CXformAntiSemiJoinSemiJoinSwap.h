//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformAntiSemiJoinSemiJoinSwap.h
//
//	@doc:
//		Swap cascaded anti semi-join and semi-join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformAntiSemiJoinSemiJoinSwap_H
#define GPOPT_CXformAntiSemiJoinSemiJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformAntiSemiJoinSemiJoinSwap
//
//	@doc:
//		Swap cascaded anti semi-join and semi-join
//
//---------------------------------------------------------------------------
class CXformAntiSemiJoinSemiJoinSwap
	: public CXformJoinSwap<CLogicalLeftAntiSemiJoin, CLogicalLeftSemiJoin>
{
private:
	// private copy ctor
	CXformAntiSemiJoinSemiJoinSwap(const CXformAntiSemiJoinSemiJoinSwap &);

public:
	// ctor
	explicit CXformAntiSemiJoinSemiJoinSwap(CMemoryPool *mp)
		: CXformJoinSwap<CLogicalLeftAntiSemiJoin, CLogicalLeftSemiJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformAntiSemiJoinSemiJoinSwap()
	{
	}

	// Compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfSemiJoinAntiSemiJoinSwap != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfAntiSemiJoinSemiJoinSwap;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformAntiSemiJoinSemiJoinSwap";
	}

};	// class CXformAntiSemiJoinSemiJoinSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformAntiSemiJoinSemiJoinSwap_H

// EOF
