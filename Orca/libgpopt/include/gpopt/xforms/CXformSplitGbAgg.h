//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSplitGbAgg.h
//
//	@doc:
//		Split an aggregate into a pair of local and global aggregate
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSplitGbAgg_H
#define GPOPT_CXformSplitGbAgg_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSplitGbAgg
//
//	@doc:
//		Split an aggregate operator into pair of local and global aggregate
//
//---------------------------------------------------------------------------
class CXformSplitGbAgg : public CXformExploration
{
private:
	// private copy ctor
	CXformSplitGbAgg(const CXformSplitGbAgg &);

protected:
	// check if the transformation is applicable;
	static BOOL FApplicable(CExpression *pexpr);

	// generate a project lists for the local and global aggregates
	// from the original aggregate
	static void PopulateLocalGlobalProjectList(
		CMemoryPool *mp,  // memory pool
		CExpression *
			pexprProjListOrig,	// project list of the original global aggregate
		CExpression *
			*ppexprProjListLocal,  // project list of the new local aggregate
		CExpression *
			*ppexprProjListGlobal  // project list of the new global aggregate
	);

public:
	// ctor
	explicit CXformSplitGbAgg(CMemoryPool *mp);

	// ctor
	explicit CXformSplitGbAgg(CExpression *pexprPattern);

	// dtor
	virtual ~CXformSplitGbAgg()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSplitGbAgg;
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return "CXformSplitGbAgg";
	}

	// Compatibility function for splitting aggregates
	virtual BOOL
	FCompatible(CXform::EXformId exfid)
	{
		return ((CXform::ExfSplitDQA != exfid) &&
				(CXform::ExfSplitGbAgg != exfid) &&
				(CXform::ExfEagerAgg != exfid));
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	virtual void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						   CExpression *pexpr) const;

};	// class CXformSplitGbAgg

}  // namespace gpopt

#endif	// !GPOPT_CXformSplitGbAgg_H

// EOF
