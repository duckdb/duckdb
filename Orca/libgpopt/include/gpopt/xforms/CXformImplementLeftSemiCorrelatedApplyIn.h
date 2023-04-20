//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc..
//
//	@filename:
//		CXformImplementLeftSemiCorrelatedApplyIn.h
//
//	@doc:
//		Transform left semi correlated apply with IN/ANY semantics
//		to physical left semi correlated join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementLeftSemiCorrelatedApplyIn_H
#define GPOPT_CXformImplementLeftSemiCorrelatedApplyIn_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementCorrelatedApply.h"

namespace gpopt
{
using namespace gpos;

//-------------------------------------------------------------------------
//	@class:
//		CXformImplementLeftSemiCorrelatedApplyIn
//
//	@doc:
//		Transform left semi correlated apply with IN/ANY semantics
//		to physical left semi correlated join
//
//-------------------------------------------------------------------------
class CXformImplementLeftSemiCorrelatedApplyIn
	: public CXformImplementCorrelatedApply<CLogicalLeftSemiCorrelatedApplyIn,
											CPhysicalCorrelatedInLeftSemiNLJoin>
{
private:
	// private copy ctor
	CXformImplementLeftSemiCorrelatedApplyIn(
		const CXformImplementLeftSemiCorrelatedApplyIn &);

public:
	// ctor
	explicit CXformImplementLeftSemiCorrelatedApplyIn(CMemoryPool *mp)
		: CXformImplementCorrelatedApply<CLogicalLeftSemiCorrelatedApplyIn,
										 CPhysicalCorrelatedInLeftSemiNLJoin>(
			  mp)
	{
	}

	// dtor
	virtual ~CXformImplementLeftSemiCorrelatedApplyIn()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementLeftSemiCorrelatedApplyIn;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementLeftSemiCorrelatedApplyIn";
	}

};	// class CXformImplementLeftSemiCorrelatedApplyIn

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementLeftSemiCorrelatedApplyIn_H

// EOF
