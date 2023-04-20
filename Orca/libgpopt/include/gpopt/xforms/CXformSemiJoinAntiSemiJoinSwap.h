//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSemiJoinAntiSemiJoinSwap.h
//
//	@doc:
//		Swap cascaded semi-join and anti semi-join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSemiJoinAntiSemiJoinSwap_H
#define GPOPT_CXformSemiJoinAntiSemiJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoinSwap.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSemiJoinAntiSemiJoinSwap
//
//	@doc:
//		Swap cascaded semi-join and anti semi-join
//
//---------------------------------------------------------------------------
class CXformSemiJoinAntiSemiJoinSwap
	: public CXformJoinSwap<CLogicalLeftSemiJoin, CLogicalLeftAntiSemiJoin>
{
private:
	// private copy ctor
	CXformSemiJoinAntiSemiJoinSwap(const CXformSemiJoinAntiSemiJoinSwap &);

public:
	// ctor
	explicit CXformSemiJoinAntiSemiJoinSwap(CMemoryPool *mp)
		: CXformJoinSwap<CLogicalLeftSemiJoin, CLogicalLeftAntiSemiJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformSemiJoinAntiSemiJoinSwap()
	{
	}

	// Compatibility function
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ExfAntiSemiJoinSemiJoinSwap != exfid;
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSemiJoinAntiSemiJoinSwap;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformSemiJoinAntiSemiJoinSwap";
	}

};	// class CXformSemiJoinAntiSemiJoinSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformSemiJoinAntiSemiJoinSwap_H

// EOF
