//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSimplifyGbAgg.h
//
//	@doc:
//		Simplify an aggregate by splitting grouping columns into a set of
//		functional dependencies in preparation for pushing Gb below join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSimplifyGbAgg_H
#define GPOPT_CXformSimplifyGbAgg_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSimplifyGbAgg
//
//	@doc:
//		Simplify an aggregate by splitting grouping columns into a set of
//		functional dependencies
//
//---------------------------------------------------------------------------
class CXformSimplifyGbAgg : public CXformExploration
{
private:
	// helper to check if GbAgg can be transformed to a Select
	static BOOL FDropGbAgg(CMemoryPool *mp, CExpression *pexpr,
						   CXformResult *pxfres);

	// private copy ctor
	CXformSimplifyGbAgg(const CXformSimplifyGbAgg &);

public:
	// ctor
	explicit CXformSimplifyGbAgg(CMemoryPool *mp);

	// dtor
	virtual ~CXformSimplifyGbAgg()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSimplifyGbAgg;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSimplifyGbAgg";
	}

	// Compatibility function for simplifying aggregates
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return (CXform::ExfSimplifyGbAgg != exfid) &&
			   (CXform::ExfSplitDQA != exfid) &&
			   (CXform::ExfSplitGbAgg != exfid) &&
			   (CXform::ExfEagerAgg != exfid);
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *, CXformResult *, CExpression *) const;

};	// class CXformSimplifyGbAgg

}  // namespace gpopt

#endif	// !GPOPT_CXformSimplifyGbAgg_H

// EOF
