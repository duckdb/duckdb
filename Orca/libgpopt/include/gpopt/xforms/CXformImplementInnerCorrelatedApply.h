//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementInnerCorrelatedApply.h
//
//	@doc:
//		Transform inner correlated apply to physical inner correlated apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementInnerCorrelatedApply_H
#define GPOPT_CXformImplementInnerCorrelatedApply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementCorrelatedApply.h"

namespace gpopt
{
using namespace gpos;

//-------------------------------------------------------------------------
//	@class:
//		CXformImplementInnerCorrelatedApply
//
//	@doc:
//		Transform inner correlated apply to physical inner correlated apply
//
//-------------------------------------------------------------------------
class CXformImplementInnerCorrelatedApply
	: public CXformImplementCorrelatedApply<CLogicalInnerCorrelatedApply,
											CPhysicalCorrelatedInnerNLJoin>
{
private:
	// private copy ctor
	CXformImplementInnerCorrelatedApply(
		const CXformImplementInnerCorrelatedApply &);

public:
	// ctor
	explicit CXformImplementInnerCorrelatedApply(CMemoryPool *mp)
		: CXformImplementCorrelatedApply<CLogicalInnerCorrelatedApply,
										 CPhysicalCorrelatedInnerNLJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformImplementInnerCorrelatedApply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementInnerCorrelatedApply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementInnerCorrelatedApply";
	}

};	// class CXformImplementInnerCorrelatedApply

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementInnerCorrelatedApply_H

// EOF
