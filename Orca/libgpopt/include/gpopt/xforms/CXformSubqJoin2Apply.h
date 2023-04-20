//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformSubqJoin2Apply.h
//
//	@doc:
//		Transform Inner Join to Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSubqJoin2Apply_H
#define GPOPT_CXformSubqJoin2Apply_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformSubqueryUnnest.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformSubqJoin2Apply
//
//	@doc:
//		Transform Inner Join with subq to Apply
//
//---------------------------------------------------------------------------
class CXformSubqJoin2Apply : public CXformSubqueryUnnest
{
private:
	// hash map between expression and a column reference
	typedef CHashMap<CExpression, CColRef, HashPtr<CExpression>,
					 EqualPtr<CExpression>, CleanupRelease<CExpression>,
					 CleanupNULL<CColRef> >
		ExprToColRefMap;

	// private copy ctor
	CXformSubqJoin2Apply(const CXformSubqJoin2Apply &);

	// helper to transform function
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr, BOOL fEnforceCorrelatedApply) const;

	// collect subqueries that exclusively use outer/inner child
	static void CollectSubqueries(CMemoryPool *mp, CExpression *pexpr,
								  CColRefSetArray *pdrgpcrs,
								  CExpressionArrays *pdrgpdrgpexprSubqs);

	// replace subqueries with scalar identifier based on given map
	static CExpression *PexprReplaceSubqueries(CMemoryPool *mp,
											   CExpression *pexprScalar,
											   ExprToColRefMap *phmexprcr);

	// push down subquery below join
	static CExpression *PexprSubqueryPushDown(CMemoryPool *mp,
											  CExpression *pexpr,
											  BOOL fEnforceCorrelatedApply);

public:
	// ctor
	explicit CXformSubqJoin2Apply(CMemoryPool *mp);

	// ctor
	explicit CXformSubqJoin2Apply(CExpression *pexprPattern)
		: CXformSubqueryUnnest(pexprPattern){};

	// dtor
	virtual ~CXformSubqJoin2Apply()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return ExfSubqJoin2Apply;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CXformSubqJoin2Apply";
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

};	// class CXformSubqJoin2Apply

}  // namespace gpopt

#endif	// !GPOPT_CXformSubqJoin2Apply_H

// EOF
