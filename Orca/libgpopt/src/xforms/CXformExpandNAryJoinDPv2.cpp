//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (C) 2019 Pivotal Inc.
//
//	@filename:
//		CXformExpandNAryJoinDPv2.cpp
//
//	@doc:
//		Implementation of n-ary join expansion using dynamic programming
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformExpandNAryJoinDPv2.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarNAryJoinPredList.h"
#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/xforms/CJoinOrderDPv2.h"
#include "gpopt/xforms/CXformUtils.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDPv2::CXformExpandNAryJoinDPv2
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoinDPv2::CXformExpandNAryJoinDPv2(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalNAryJoin(mp),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp)),
			  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDPv2::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoinDPv2::Exfp(CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpExpandJoinOrder(exprhdl, this);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDPv2::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins using
//		dynamic programming
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoinDPv2::Transform(CXformContext *pxfctxt,
									CXformResult *pxfres,
									CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	const ULONG arity = pexpr->Arity();
	GPOS_ASSERT(arity >= 3);

	// Make an expression array with all the atoms (the logical children)
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	// Make an expression array with all the join predicates, one entry for
	// every conjunct (ANDed condition),
	// plus an array of all the non-inner join predicates, together with
	// a lookup table for each child whether it is a non-inner join
	CLogicalNAryJoin *naryJoin = CLogicalNAryJoin::PopConvert(pexpr->Pop());
	CExpression *pexprScalar = (*pexpr)[arity - 1];
	CExpressionArray *innerJoinPreds = NULL;
	CExpressionArray *onPreds = GPOS_NEW(mp) CExpressionArray(mp);
	ULongPtrArray *childPredIndexes = NULL;

	if (NULL != CScalarNAryJoinPredList::PopConvert(pexprScalar->Pop()))
	{
		innerJoinPreds =
			CPredicateUtils::PdrgpexprConjuncts(mp, (*pexprScalar)[0]);

		for (ULONG ul = 1; ul < pexprScalar->Arity(); ul++)
		{
			(*pexprScalar)[ul]->AddRef();
			onPreds->Append((*pexprScalar)[ul]);
		}

		childPredIndexes = naryJoin->GetLojChildPredIndexes();
		GPOS_ASSERT(NULL != childPredIndexes);
		childPredIndexes->AddRef();
	}
	else
	{
		innerJoinPreds = CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
	}

	CColRefSet *outerRefs = pexpr->DeriveOuterReferences();

	outerRefs->AddRef();

	// create join order using dynamic programming v2, record topk results in jodp
	CJoinOrderDPv2 jodp(mp, pdrgpexpr, innerJoinPreds, onPreds,
						childPredIndexes, outerRefs);
	jodp.PexprExpand();

	// Retrieve top K join orders from jodp and add as alternatives
	CExpression *nextJoinOrder = NULL;

	while (NULL != (nextJoinOrder = jodp.GetNextOfTopK()))
	{
		CExpression *pexprNormalized =
			CNormalizer::PexprNormalize(mp, nextJoinOrder);

		nextJoinOrder->Release();
		pxfres->Add(pexprNormalized);
	}
}

// EOF
