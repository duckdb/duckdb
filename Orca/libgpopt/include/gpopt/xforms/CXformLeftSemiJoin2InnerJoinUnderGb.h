//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformLeftSemiJoin2InnerJoinUnderGb.h
//
//	@doc:
//		Transform left semi join to inner join under a groupby
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiJoin2InnerJoinUnderGb_H
#define GPOPT_CXformLeftSemiJoin2InnerJoinUnderGb_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformLeftSemiJoin2InnerJoinUnderGb
//
//	@doc:
//		Transform left semi join to inner join under a groupby
//
//---------------------------------------------------------------------------
class CXformLeftSemiJoin2InnerJoinUnderGb : public CXformExploration
{
private:
	// private copy ctor
	CXformLeftSemiJoin2InnerJoinUnderGb(
		const CXformLeftSemiJoin2InnerJoinUnderGb &);

public:
	// ctor
	explicit CXformLeftSemiJoin2InnerJoinUnderGb(CMemoryPool *mp);

	// dtor
	virtual ~CXformLeftSemiJoin2InnerJoinUnderGb()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfLeftSemiJoin2InnerJoinUnderGb;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformLeftSemiJoin2InnerJoinUnderGb";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformLeftSemiJoin2InnerJoinUnderGb

}  // namespace gpopt


#endif	// !GPOPT_CXformLeftSemiJoin2InnerJoinUnderGb_H

// EOF
