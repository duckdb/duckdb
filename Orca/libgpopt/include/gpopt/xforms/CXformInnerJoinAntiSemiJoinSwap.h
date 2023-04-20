//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInnerJoinAntiSemiJoinSwap.h
//
//	@doc:
//		Swap cascaded inner join and anti semi-join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInnerJoinAntiSemiJoinSwap_H
#define GPOPT_CXformInnerJoinAntiSemiJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInnerJoinAntiSemiJoinSwap
//
//	@doc:
//		Swap cascaded inner join and anti semi-join
//
//---------------------------------------------------------------------------
class CXformInnerJoinAntiSemiJoinSwap
	: public CXformJoinSwap<CLogicalInnerJoin, CLogicalLeftAntiSemiJoin>
{
private:
	// private copy ctor
	CXformInnerJoinAntiSemiJoinSwap(const CXformInnerJoinAntiSemiJoinSwap &);

public:
	// ctor
	explicit CXformInnerJoinAntiSemiJoinSwap(CMemoryPool *mp)
		: CXformJoinSwap<CLogicalInnerJoin, CLogicalLeftAntiSemiJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformInnerJoinAntiSemiJoinSwap()
	{
	}

	// Compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfAntiSemiJoinInnerJoinSwap != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInnerJoinAntiSemiJoinSwap;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformInnerJoinAntiSemiJoinSwap";
	}

};	// class CXformInnerJoinAntiSemiJoinSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformInnerJoinAntiSemiJoinSwap_H

// EOF
