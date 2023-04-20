//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformInlineCTEConsumerUnderSelect.h
//
//	@doc:
//		Transform logical Select on top of a CTE consumer to a select on top of
//		a copy of the expression under its corresponding producer then attempt
//		push the selection down
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformInlineCTEConsumerUnderSelect_H
#define GPOPT_CXformInlineCTEConsumerUnderSelect_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformInlineCTEConsumerUnderSelect
//
//	@doc:
//		Transform logical Select on top of a CTE consumer to a select on top of
//		a copy of the expression under its corresponding producer then attempt
//		push the selection down
//
//---------------------------------------------------------------------------
class CXformInlineCTEConsumerUnderSelect : public CXformExploration
{
private:
	// private copy ctor
	CXformInlineCTEConsumerUnderSelect(
		const CXformInlineCTEConsumerUnderSelect &);

public:
	// ctor
	explicit CXformInlineCTEConsumerUnderSelect(CMemoryPool *mp);

	// dtor
	virtual ~CXformInlineCTEConsumerUnderSelect()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfInlineCTEConsumerUnderSelect;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformInlineCTEConsumerUnderSelect";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformInlineCTEConsumerUnderSelect
}  // namespace gpopt

#endif	// !GPOPT_CXformInlineCTEConsumerUnderSelect_H

// EOF
