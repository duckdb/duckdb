//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandFullOuterJoin.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformExpandFullOuterJoin.h"

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandFullOuterJoin::CXformExpandFullOuterJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandFullOuterJoin::CXformExpandFullOuterJoin(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalFullOuterJoin(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp)),  // outer child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp)),  // inner child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar child
			  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformExpandFullOuterJoin::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandFullOuterJoin::Exfp(CExpressionHandle &	 //exprhdl
) const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformExpandFullOuterJoin::Transform
//
//	@doc:
//		Actual transformation
// 		The expression A FOJ B is translated to:
//
//		CTEAnchor(cteA)
//		+-- CTEAnchor(cteB)
//			+--UnionAll
//				|--	LOJ
//				|	|--	CTEConsumer(cteA)
//				|	+--	CTEConsumer(cteB)
//				+--	Project
//					+--	LASJ
//					|	|--	CTEConsumer(cteB)
//					|	+--	CTEConsumer(cteA)
//					+-- (NULLS - same schema of A)
//
//		Also, two CTE producers for cteA and cteB are added to CTE info
//
//---------------------------------------------------------------------------
void
CXformExpandFullOuterJoin::Transform(CXformContext *pxfctxt,
									 CXformResult *pxfres,
									 CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprA = (*pexpr)[0];
	CExpression *pexprB = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	// 1. create the CTE producers
	const ULONG ulCTEIdA = COptCtxt::PoctxtFromTLS()->Pcteinfo()->next_id();
	CColRefArray *pdrgpcrOutA = pexprA->DeriveOutputColumns()->Pdrgpcr(mp);
	(void) CXformUtils::PexprAddCTEProducer(mp, ulCTEIdA, pdrgpcrOutA, pexprA);

	const ULONG ulCTEIdB = COptCtxt::PoctxtFromTLS()->Pcteinfo()->next_id();
	CColRefArray *pdrgpcrOutB = pexprB->DeriveOutputColumns()->Pdrgpcr(mp);
	(void) CXformUtils::PexprAddCTEProducer(mp, ulCTEIdB, pdrgpcrOutB, pexprB);

	// 2. create the right child (PROJECT over LASJ)
	CColRefArray *pdrgpcrRightA = CUtils::PdrgpcrCopy(mp, pdrgpcrOutA);
	CColRefArray *pdrgpcrRightB = CUtils::PdrgpcrCopy(mp, pdrgpcrOutB);
	CExpression *pexprScalarRight = CXformUtils::PexprRemapColumns(
		mp, pexprScalar, pdrgpcrOutA, pdrgpcrRightA, pdrgpcrOutB,
		pdrgpcrRightB);
	CExpression *pexprLASJ = PexprLogicalJoinOverCTEs(
		mp, EdxljtLeftAntiSemijoin, ulCTEIdB, pdrgpcrRightB, ulCTEIdA,
		pdrgpcrRightA, pexprScalarRight);
	CExpression *pexprProject =
		CUtils::PexprLogicalProjectNulls(mp, pdrgpcrRightA, pexprLASJ);

	// 3. create the left child (LOJ) - this has to use the original output
	//    columns and the original scalar expression
	pexprScalar->AddRef();
	CExpression *pexprLOJ =
		PexprLogicalJoinOverCTEs(mp, EdxljtLeft, ulCTEIdA, pdrgpcrOutA,
								 ulCTEIdB, pdrgpcrOutB, pexprScalar);

	// 4. create the UNION ALL expression

	// output columns of the union are the same as the outputs of the first child (LOJ)
	CColRefArray *pdrgpcrOutput = GPOS_NEW(mp) CColRefArray(mp);
	pdrgpcrOutput->AppendArray(pdrgpcrOutA);
	pdrgpcrOutput->AppendArray(pdrgpcrOutB);

	// input columns of the union
	CColRef2dArray *pdrgdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp);

	// inputs from the first child (LOJ)
	pdrgpcrOutput->AddRef();
	pdrgdrgpcrInput->Append(pdrgpcrOutput);

	// inputs from the second child have to be in the correct order
	// a. add new computed columns from the project only
	CColRefSet *pcrsProjOnly = GPOS_NEW(mp) CColRefSet(mp);
	pcrsProjOnly->Include(pexprProject->DeriveOutputColumns());
	pcrsProjOnly->Exclude(pdrgpcrRightB);
	CColRefArray *pdrgpcrProj = pcrsProjOnly->Pdrgpcr(mp);
	pcrsProjOnly->Release();
	// b. add columns from the LASJ expression
	pdrgpcrProj->AppendArray(pdrgpcrRightB);

	pdrgdrgpcrInput->Append(pdrgpcrProj);

	CExpression *pexprUnionAll = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgdrgpcrInput),
		pexprLOJ, pexprProject);

	// 5. Add CTE anchor for the B subtree
	CExpression *pexprAnchorB = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEIdB), pexprUnionAll);

	// 6. Add CTE anchor for the A subtree
	CExpression *pexprAnchorA = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEIdA), pexprAnchorB);

	// add alternative to xform result
	pxfres->Add(pexprAnchorA);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformExpandFullOuterJoin::PexprLogicalJoinOverCTEs
//
//	@doc:
//		Construct a join expression of two CTEs using the given CTE ids
// 		and output columns
//
//---------------------------------------------------------------------------
CExpression *
CXformExpandFullOuterJoin::PexprLogicalJoinOverCTEs(
	CMemoryPool *mp, EdxlJoinType edxljointype, ULONG ulLeftCTEId,
	CColRefArray *pdrgpcrLeft, ULONG ulRightCTEId, CColRefArray *pdrgpcrRight,
	CExpression *pexprScalar) const
{
	GPOS_ASSERT(NULL != pexprScalar);

	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();

	CLogicalCTEConsumer *popConsumerLeft =
		GPOS_NEW(mp) CLogicalCTEConsumer(mp, ulLeftCTEId, pdrgpcrLeft);
	CExpression *pexprLeft = GPOS_NEW(mp) CExpression(mp, popConsumerLeft);
	pcteinfo->IncrementConsumers(ulLeftCTEId);

	CLogicalCTEConsumer *popConsumerRight =
		GPOS_NEW(mp) CLogicalCTEConsumer(mp, ulRightCTEId, pdrgpcrRight);
	CExpression *pexprRight = GPOS_NEW(mp) CExpression(mp, popConsumerRight);
	pcteinfo->IncrementConsumers(ulRightCTEId);

	pdrgpexprChildren->Append(pexprLeft);
	pdrgpexprChildren->Append(pexprRight);
	pdrgpexprChildren->Append(pexprScalar);

	return CUtils::PexprLogicalJoin(mp, edxljointype, pdrgpexprChildren);
}

// EOF
