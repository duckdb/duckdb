//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformAntiSemiJoinInnerJoinSwap.h
//
//	@doc:
//		Swap cascaded anti semi-join and inner join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformAntiSemiJoinInnerJoinSwap_H
#define GPOPT_CXformAntiSemiJoinInnerJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformAntiSemiJoinInnerJoinSwap
//
//	@doc:
//		Swap cascaded anti semi-join and inner join
//
//---------------------------------------------------------------------------
class CXformAntiSemiJoinInnerJoinSwap
	: public CXformJoinSwap<CLogicalLeftAntiSemiJoin, CLogicalInnerJoin>
{
private:
	// private copy ctor
	CXformAntiSemiJoinInnerJoinSwap(const CXformAntiSemiJoinInnerJoinSwap &);

public:
	// ctor
	explicit CXformAntiSemiJoinInnerJoinSwap(CMemoryPool *mp)
		: CXformJoinSwap<CLogicalLeftAntiSemiJoin, CLogicalInnerJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformAntiSemiJoinInnerJoinSwap()
	{
	}

	// Compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfInnerJoinAntiSemiJoinSwap != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfAntiSemiJoinInnerJoinSwap;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformAntiSemiJoinInnerJoinSwap";
	}

};	// class CXformAntiSemiJoinInnerJoinSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformAntiSemiJoinInnerJoinSwap_H

// EOF
