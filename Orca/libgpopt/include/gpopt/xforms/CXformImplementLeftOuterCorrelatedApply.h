//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementLeftOuterCorrelatedApply.h
//
//	@doc:
//		Transform LeftOuter correlated apply to physical LeftOuter correlated
//		apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementLeftOuterCorrelatedApply_H
#define GPOPT_CXformImplementLeftOuterCorrelatedApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementCorrelatedApply.h"

namespace gpopt
{
using namespace gpos;

//-------------------------------------------------------------------------
//	@class:
//		CXformImplementLeftOuterCorrelatedApply
//
//	@doc:
//		Transform LeftOuter correlated apply to physical LeftOuter correlated
//		apply
//
//-------------------------------------------------------------------------
class CXformImplementLeftOuterCorrelatedApply
	: public CXformImplementCorrelatedApply<CLogicalLeftOuterCorrelatedApply,
											CPhysicalCorrelatedLeftOuterNLJoin>
{
private:
	// private copy ctor
	CXformImplementLeftOuterCorrelatedApply(
		const CXformImplementLeftOuterCorrelatedApply &);

public:
	// ctor
	explicit CXformImplementLeftOuterCorrelatedApply(CMemoryPool *mp)
		: CXformImplementCorrelatedApply<CLogicalLeftOuterCorrelatedApply,
										 CPhysicalCorrelatedLeftOuterNLJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformImplementLeftOuterCorrelatedApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementLeftOuterCorrelatedApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementLeftOuterCorrelatedApply";
	}

};	// class CXformImplementLeftOuterCorrelatedApply

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementLeftOuterCorrelatedApply_H

// EOF
