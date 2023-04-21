//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CXformGbAggWithMDQA2Join.cpp
//
//	@doc:
//		Transform a GbAgg with multiple distinct qualified aggregates (MDQAs)
//		to a join tree with single DQA leaves
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformGbAggWithMDQA2Join.h"

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::CXformGbAggWithMDQA2Join
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAggWithMDQA2Join::CXformGbAggWithMDQA2Join(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp)),  // relational child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAggWithMDQA2Join::Exfp(CExpressionHandle &exprhdl) const
{
	CAutoMemoryPool amp;

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());

	if (COperator::EgbaggtypeGlobal == popAgg->Egbaggtype() &&
		exprhdl.DeriveHasMultipleDistinctAggs(1))
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::PexprMDQAs2Join
//
//	@doc:
//		Converts GbAgg with multiple distinct aggregates into a join of single
//		distinct aggregates,
//
//		distinct aggregates that share the same argument are grouped together
//		in one leaf of the generated join expression,
//
//		non-distinct aggregates are also grouped together in one leaf of the
//		generated join expression
//
//---------------------------------------------------------------------------
CExpression *
CXformGbAggWithMDQA2Join::PexprMDQAs2Join(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalGbAgg == pexpr->Pop()->Eopid());
	GPOS_ASSERT((*pexpr)[1]->DeriveHasMultipleDistinctAggs());

	// extract components
	CExpression *pexprChild = (*pexpr)[0];

	CColRefSet *pcrsChildOutput = pexprChild->DeriveOutputColumns();
	CColRefArray *pdrgpcrChildOutput = pcrsChildOutput->Pdrgpcr(mp);

	// create a CTE producer based on child expression
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	const ULONG ulCTEId = pcteinfo->next_id();
	(void) CXformUtils::PexprAddCTEProducer(mp, ulCTEId, pdrgpcrChildOutput,
											pexprChild);

	// create a CTE consumer with child output columns
	CExpression *pexprConsumer = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEConsumer(mp, ulCTEId, pdrgpcrChildOutput));
	pcteinfo->IncrementConsumers(ulCTEId);

	// finalize GbAgg expression by replacing its child with CTE consumer
	pexpr->Pop()->AddRef();
	(*pexpr)[1]->AddRef();
	CExpression *pexprGbAggWithConsumer =
		GPOS_NEW(mp) CExpression(mp, pexpr->Pop(), pexprConsumer, (*pexpr)[1]);

	CExpression *pexprJoinDQAs =
		CXformUtils::PexprGbAggOnCTEConsumer2Join(mp, pexprGbAggWithConsumer);
	GPOS_ASSERT(NULL != pexprJoinDQAs);

	pexprGbAggWithConsumer->Release();

	return GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEId), pexprJoinDQAs);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::PexprExpandMDQAs
//
//	@doc:
//		Expand GbAgg with multiple distinct aggregates into a join of single
//		distinct aggregates,
//		return NULL if expansion is not done
//
//---------------------------------------------------------------------------
CExpression *
CXformGbAggWithMDQA2Join::PexprExpandMDQAs(CMemoryPool *mp, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalGbAgg == pexpr->Pop()->Eopid());

	COperator *pop = pexpr->Pop();
	if (CLogicalGbAgg::PopConvert(pop)->FGlobal())
	{
		BOOL fHasMultipleDistinctAggs =
			(*pexpr)[1]->DeriveHasMultipleDistinctAggs();
		if (fHasMultipleDistinctAggs)
		{
			CExpression *pexprExpanded = PexprMDQAs2Join(mp, pexpr);

			// recursively process the resulting expression
			CExpression *pexprResult = PexprTransform(mp, pexprExpanded);
			pexprExpanded->Release();

			return pexprResult;
		}
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::PexprTransform
//
//	@doc:
//		Main transformation driver
//
//---------------------------------------------------------------------------
CExpression *
CXformGbAggWithMDQA2Join::PexprTransform(CMemoryPool *mp, CExpression *pexpr)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (COperator::EopLogicalGbAgg == pop->Eopid())
	{
		CExpression *pexprResult = PexprExpandMDQAs(mp, pexpr);
		if (NULL != pexprResult)
		{
			return pexprResult;
		}
	}

	// recursively process child expressions
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprTransform(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggWithMDQA2Join::Transform
//
//	@doc:
//		Actual transformation to expand multiple distinct qualified aggregates
//		(MDQAs) to a join tree with single DQA leaves
//
//---------------------------------------------------------------------------
void
CXformGbAggWithMDQA2Join::Transform(CXformContext *pxfctxt,
									CXformResult *pxfres,
									CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprResult = PexprTransform(mp, pexpr);
	if (NULL != pexprResult)
	{
		pxfres->Add(pexprResult);
	}
}

BOOL
CXformGbAggWithMDQA2Join::IsApplyOnce()
{
	return true;
}
// EOF
