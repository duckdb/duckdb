//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformCTEAnchor2TrivialSelect.h
//
//	@doc:
//		Transform logical CTE anchor to select with "true" predicate
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformCTEAnchor2TrivialSelect_H
#define GPOPT_CXformCTEAnchor2TrivialSelect_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformCTEAnchor2TrivialSelect
//
//	@doc:
//		Transform logical CTE anchor to select with "true" predicate
//
//---------------------------------------------------------------------------
class CXformCTEAnchor2TrivialSelect : public CXformExploration
{
private:
	// private copy ctor
	CXformCTEAnchor2TrivialSelect(const CXformCTEAnchor2TrivialSelect &);

public:
	// ctor
	explicit CXformCTEAnchor2TrivialSelect(CMemoryPool *mp);

	// dtor
	virtual ~CXformCTEAnchor2TrivialSelect()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfCTEAnchor2TrivialSelect;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformCTEAnchor2TrivialSelect";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformCTEAnchor2TrivialSelect
}  // namespace gpopt

#endif	// !GPOPT_CXformCTEAnchor2TrivialSelect_H

// EOF
