//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformCollapseGbAgg.h
//
//	@doc:
//		Collapse two cascaded GbAgg operators into a single GbAgg
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformCollapseGbAgg_H
#define GPOPT_CXformCollapseGbAgg_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformCollapseGbAgg
//
//	@doc:
//		Collapse two cascaded GbAgg operators into a single GbAgg
//
//---------------------------------------------------------------------------
class CXformCollapseGbAgg : public CXformExploration
{
private:
	// private copy ctor
	CXformCollapseGbAgg(const CXformCollapseGbAgg &);

public:
	// ctor
	explicit CXformCollapseGbAgg(CMemoryPool *mp);

	// dtor
	virtual ~CXformCollapseGbAgg()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfCollapseGbAgg;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformCollapseGbAgg";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformCollapseGbAgg

}  // namespace gpopt

#endif	// !GPOPT_CXformCollapseGbAgg_H

// EOF
