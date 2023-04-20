//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc..
//
//	@filename:
//		CXformImplementLeftAntiSemiCorrelatedApplyNotIn.h
//
//	@doc:
//		Transform left anti semi correlated apply with NOT-IN/ALL semantics
//		to physical left anti semi correlated join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementLeftAntiSemiCorrelatedApplyNotIn_H
#define GPOPT_CXformImplementLeftAntiSemiCorrelatedApplyNotIn_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementCorrelatedApply.h"

namespace gpopt
{
using namespace gpos;

//-------------------------------------------------------------------------
//	@class:
//		CXformImplementLeftAntiSemiCorrelatedApplyNotIn
//
//	@doc:
//		Transform left anti semi correlated apply with NOT-IN/ALL semantics
//		to physical left anti semi correlated join
//
//-------------------------------------------------------------------------
class CXformImplementLeftAntiSemiCorrelatedApplyNotIn
	: public CXformImplementCorrelatedApply<
		  CLogicalLeftAntiSemiCorrelatedApplyNotIn,
		  CPhysicalCorrelatedNotInLeftAntiSemiNLJoin>
{
private:
	// private copy ctor
	CXformImplementLeftAntiSemiCorrelatedApplyNotIn(
		const CXformImplementLeftAntiSemiCorrelatedApplyNotIn &);

public:
	// ctor
	explicit CXformImplementLeftAntiSemiCorrelatedApplyNotIn(CMemoryPool *mp)
		: CXformImplementCorrelatedApply<
			  CLogicalLeftAntiSemiCorrelatedApplyNotIn,
			  CPhysicalCorrelatedNotInLeftAntiSemiNLJoin>(mp)
	{
	}

	// dtor
	virtual ~CXformImplementLeftAntiSemiCorrelatedApplyNotIn()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfImplementLeftAntiSemiCorrelatedApplyNotIn;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformImplementLeftAntiSemiCorrelatedApplyNotIn";
	}

};	// class CXformImplementLeftAntiSemiCorrelatedApplyNotIn

}  // namespace gpopt

#endif	// !GPOPT_CXformImplementLeftAntiSemiCorrelatedApplyNotIn_H

// EOF
