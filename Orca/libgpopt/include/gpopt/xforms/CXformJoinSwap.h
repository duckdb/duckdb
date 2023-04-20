//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformJoinSwap.h
//
//	@doc:
//		Join swap transformation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoinSwap_H
#define GPOPT_CXformJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/xforms/CXformUtils.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformJoinSwap
//
//	@doc:
//		Join swap transformation
//
//---------------------------------------------------------------------------
template <class TJoinTop, class TJoinBottom>
class CXformJoinSwap : public CXformExploration
{
private:
	// private copy ctor
	CXformJoinSwap(const CXformJoinSwap &);

public:
	// ctor
	explicit CXformJoinSwap(CMemoryPool *mp)
		: CXformExploration(
			  // pattern
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) TJoinTop(mp),
				  GPOS_NEW(mp) CExpression	// left child is a join tree
				  (mp, GPOS_NEW(mp) TJoinBottom(mp),
				   GPOS_NEW(mp) CExpression(
					   mp, GPOS_NEW(mp) CPatternLeaf(mp)),	// left child
				   GPOS_NEW(mp) CExpression(
					   mp, GPOS_NEW(mp) CPatternLeaf(mp)),	// right child
				   GPOS_NEW(mp) CExpression(
					   mp, GPOS_NEW(mp) CPatternLeaf(mp))  // predicate
				   ),
				  GPOS_NEW(mp) CExpression	// right child is a pattern leaf
				  (mp, GPOS_NEW(mp) CPatternLeaf(mp)),
				  GPOS_NEW(mp) CExpression(
					  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // top-join predicate
				  ))
	{
	}

	// dtor
	virtual ~CXformJoinSwap()
	{
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise
	Exfp(CExpressionHandle &  // exprhdl
	) const
	{
		return CXform::ExfpHigh;
	}

	// actual transform
	void
	Transform(CXformContext *pxfctxt, CXformResult *pxfres,
			  CExpression *pexpr) const
	{
		GPOS_ASSERT(NULL != pxfctxt);
		GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
		GPOS_ASSERT(FCheckPattern(pexpr));

		CMemoryPool *mp = pxfctxt->Pmp();

		CExpression *pexprResult =
			CXformUtils::PexprSwapJoins(mp, pexpr, (*pexpr)[0]);
		if (NULL == pexprResult)
		{
			return;
		}

		pxfres->Add(pexprResult);
	}

};	// class CXformJoinSwap

}  // namespace gpopt

#endif	// !GPOPT_CXformJoinSwap_H

// EOF
