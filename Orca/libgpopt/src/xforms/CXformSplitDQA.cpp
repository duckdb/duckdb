//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal Inc.
//
//	@filename:
//		CXformSplitDQA.cpp
//
//	@doc:
//		Implementation of the splitting of an aggregate into a three levels -- namely,
//		local, intermediate and global, aggregate
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSplitDQA.h"

#include "gpos/base.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::CXformSplitDQA
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitDQA::CXformSplitDQA(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSplitDQA::Exfp(CExpressionHandle &exprhdl) const
{
	// do not split aggregate if it is not a global aggregate,  has no distinct aggs, has MDQAs, has outer references,
	// or return types of Agg functions are ambiguous
	if (!CLogicalGbAgg::PopConvert(exprhdl.Pop())->FGlobal() ||
		0 == exprhdl.DeriveTotalDistinctAggs(1) ||
		exprhdl.DeriveHasMultipleDistinctAggs(1) ||
		0 < exprhdl.DeriveOuterReferences()->Size() ||
		NULL == exprhdl.PexprScalarExactChild(1) ||
		CXformUtils::FHasAmbiguousType(
			exprhdl.PexprScalarExactChild(1 /*child_index*/),
			COptCtxt::PoctxtFromTLS()->Pmda()))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::Transform
//
//	@doc:
//		Actual transformation to expand a global aggregate into local,
//		intermediate and global aggregates
//
//---------------------------------------------------------------------------
void
CXformSplitDQA::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
						  CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	ExprToColRefMap *phmexprcr = GPOS_NEW(mp) ExprToColRefMap(mp);
	CExpressionArray *pdrgpexprChildPrEl = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefArray *pdrgpcrArgDQA = NULL;

	ExtractDistinctCols(mp, col_factory, md_accessor, pexprProjectList,
						pdrgpexprChildPrEl, phmexprcr, &pdrgpcrArgDQA);

	if (NULL == pdrgpcrArgDQA)
	{
		// failed to extract distinct columns
		phmexprcr->Release();
		pdrgpexprChildPrEl->Release();

		return;
	}


	if (0 < pdrgpexprChildPrEl->Size())
	{
		pexprRelational->AddRef();

		// computed columns referred to in the DQA
		CExpression *pexprChildProject = CUtils::PexprLogicalProject(
			mp, pexprRelational,
			GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
									 pdrgpexprChildPrEl),
			true /*fNewComputedCol*/
		);
		pexprRelational = pexprChildProject;
	}

	CColRefArray *grouping_colref_array =
		CLogicalGbAgg::PopConvert(pexpr->Pop())->Pdrgpcr();
	BOOL fScalarDQA =
		(grouping_colref_array == NULL || grouping_colref_array->Size() == 0);

	// multi-stage for both scalar and non-scalar aggregates.
	CExpression *pexprThreeStageDQA =
		PexprSplitHelper(mp, col_factory, md_accessor, pexpr, pexprRelational,
						 phmexprcr, pdrgpcrArgDQA,
						 fScalarDQA ? CLogicalGbAgg::EasThreeStageScalarDQA
									: CLogicalGbAgg::EasOthers);

	pxfres->Add(pexprThreeStageDQA);

	if (fScalarDQA)
	{
		// generate two-stage agg for scalar DQA case
		// this transform is useful for cases where distinct column is same as distributed column.
		// for a query like "select count(distinct a) from bar;"
		// we generate a two stage agg where the aggregate operator gives us the distinct values.
		// CScalarProjectList for the Local agg below is empty on purpose.

		//		+--CLogicalGbAgg( Global ) Grp Cols: [][Global]
		//		|--CLogicalGbAgg( Local ) Grp Cols: ["a" (0)][Local],
		//		|  |--CLogicalGet "bar" ("bar"),
		//		|  +--CScalarProjectList
		//		+--CScalarProjectList
		//			+--CScalarProjectElement "count" (9)
		//				+--CScalarAggFunc (count , Distinct: false , Aggregate Stage: Global)
		//					+--CScalarIdent "a" (0)

		CExpression *pexprTwoStageScalarDQA = PexprSplitHelper(
			mp, col_factory, md_accessor, pexpr, pexprRelational, phmexprcr,
			pdrgpcrArgDQA, CLogicalGbAgg::EasTwoStageScalarDQA);
		pxfres->Add(pexprTwoStageScalarDQA);
	}

	// generate local DQA, global agg for both scalar and non-scalar agg cases.
	// for a query like "select count(distinct a) from bar;"
	// we generate a two-stage agg where the projectlist with AggFunc is pushed to
	// both local and global agg. We use the AggFunc to get our distinct values.

	//	+--CLogicalGbAgg( Global ) Grp Cols: []
	//	|--CLogicalGbAgg( Local ) Grp Cols: []
	//	|  |--CLogicalGet "bar" ("bar"),
	//	|  +--CScalarProjectList
	//	|     +--CScalarProjectElement "ColRef_0010" (10)
	//	|        +--CScalarAggFunc (count , Distinct: true , Aggregate Stage: Local)
	//	|           +--CScalarIdent "a" (0)
	//	+--CScalarProjectList
	//		+--CScalarProjectElement "count" (9)
	//			+--CScalarAggFunc (count , Distinct: false , Aggregate Stage: Global)
	//				+--CScalarIdent "ColRef_0010" (10)

	CExpression *pexprTwoStageDQA = PexprSplitIntoLocalDQAGlobalAgg(
		mp, col_factory, md_accessor, pexpr, pexprRelational, phmexprcr,
		pdrgpcrArgDQA,
		fScalarDQA ? CLogicalGbAgg::EasTwoStageScalarDQA
				   : CLogicalGbAgg::EasOthers);

	pxfres->Add(pexprTwoStageDQA);


	pdrgpcrArgDQA->Release();

	// clean up
	if (0 < pdrgpexprChildPrEl->Size())
	{
		pexprRelational->Release();
	}
	else
	{
		pdrgpexprChildPrEl->Release();
	}

	phmexprcr->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PexprSplitIntoLocalDQAGlobalAgg
//
//	@doc:
// 		Split the DQA into a local DQA and global agg function
//
//---------------------------------------------------------------------------
CExpression *
CXformSplitDQA::PexprSplitIntoLocalDQAGlobalAgg(
	CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
	CExpression *pexpr, CExpression *pexprRelational,
	ExprToColRefMap *phmexprcr, CColRefArray *pdrgpcrArgDQA,
	CLogicalGbAgg::EAggStage aggStage)
{
	CExpression *pexprPrL = (*pexpr)[1];

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	CColRefArray *pdrgpcrGlobal = popAgg->Pdrgpcr();

	// array of project elements for the local, intermediate and global aggregate operator
	CExpressionArray *pdrgpexprPrElFirstStage =
		GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprPrElLastStage =
		GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG arity = pexprPrL->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrEl = (*pexprPrL)[ul];
		CScalarProjectElement *popScPrEl =
			CScalarProjectElement::PopConvert(pexprPrEl->Pop());

		// get the scalar aggregate function
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->IsDistinct())
		{
			// create a new local DQA version of the original global DQA
			popScAggFunc->MDId()->AddRef();
			CScalarAggFunc *popScAggFuncLocal = CUtils::PopAggFunc(
				mp, popScAggFunc->MDId(),
				GPOS_NEW(mp)
					CWStringConst(mp, popScAggFunc->PstrAggFunc()->GetBuffer()),
				true /* is_distinct */, EaggfuncstageLocal /*eaggfuncstage*/,
				true /* fSplit */
			);

			GPOS_ASSERT(1 == pexprAggFunc->Arity());
			CExpression *pexprArg = (*pexprAggFunc)[0];
			CColRef *pcrDistinctCol = phmexprcr->Find(pexprArg);
			GPOS_ASSERT(NULL != pcrDistinctCol);
			CExpressionArray *pdrgpexprArgsLocal =
				GPOS_NEW(mp) CExpressionArray(mp);
			pdrgpexprArgsLocal->Append(
				CUtils::PexprScalarIdent(mp, pcrDistinctCol));

			const IMDAggregate *pmdagg =
				md_accessor->RetrieveAgg(popScAggFunc->MDId());
			const IMDType *pmdtype = md_accessor->RetrieveType(
				pmdagg->GetIntermediateResultTypeMdid());
			CColRef *pcrLocal =
				col_factory->PcrCreate(pmdtype, default_type_modifier);

			CExpression *pexprPrElLocal = CUtils::PexprScalarProjectElement(
				mp, pcrLocal,
				GPOS_NEW(mp)
					CExpression(mp, popScAggFuncLocal, pdrgpexprArgsLocal));

			pdrgpexprPrElFirstStage->Append(pexprPrElLocal);

			// create a new "non-distinct" global aggregate version of the original DQA
			popScAggFunc->MDId()->AddRef();
			CScalarAggFunc *popScAggFuncGlobal = CUtils::PopAggFunc(
				mp, popScAggFunc->MDId(),
				GPOS_NEW(mp)
					CWStringConst(mp, popScAggFunc->PstrAggFunc()->GetBuffer()),
				false /* is_distinct */, EaggfuncstageGlobal /*eaggfuncstage*/,
				true /* fSplit */
			);

			CExpressionArray *pdrgpexprArgsGlobal =
				GPOS_NEW(mp) CExpressionArray(mp);
			pdrgpexprArgsGlobal->Append(CUtils::PexprScalarIdent(mp, pcrLocal));

			CExpression *pexprPrElGlobal = CUtils::PexprScalarProjectElement(
				mp, popScPrEl->Pcr(),
				GPOS_NEW(mp)
					CExpression(mp, popScAggFuncGlobal, pdrgpexprArgsGlobal));

			pdrgpexprPrElLastStage->Append(pexprPrElGlobal);
		}
		else
		{
			// split regular aggregate function into multi-level aggregate functions
			PopulatePrLMultiPhaseAgg(
				mp, col_factory, md_accessor, pexprPrEl,
				pdrgpexprPrElFirstStage, NULL, /* pdrgpexprPrElSecondStage*/
				pdrgpexprPrElLastStage, true   /* fSplit2LevelsOnly */
			);
		}
	}

	CExpression *pexprGlobal = PexprMultiLevelAggregation(
		mp, pexprRelational, pdrgpexprPrElFirstStage,
		NULL, /* pdrgpexprPrElSecondStage */
		pdrgpexprPrElLastStage, pdrgpcrArgDQA, pdrgpcrGlobal,
		true /* fSplit2LevelsOnly */, false /* fAddDistinctColToLocalGb */,
		aggStage);

	return pexprGlobal;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PexprSplitHelper
//
//	@doc:
//		Helper function to Split DQA into 2-stage or 3-stage aggregation.
//
//		If fSpillTo2Level is FALSE, split the 'group by' operator into 3-stage aggregation,
//		e.g., first, second and last aggregates.
//
//		If fSpillTo2Level is TRUE, split the 'group by' operator into 2-stage aggregation,
//		e.g., first, last aggregates. (second aggregate function becomes empty.)
//
//		In both scenarios, add the new aggregate functions to the project list of the
//		corresponding group by operator at each stage of the multi-stage aggregation.
//
//---------------------------------------------------------------------------
CExpression *
CXformSplitDQA::PexprSplitHelper(CMemoryPool *mp, CColumnFactory *col_factory,
								 CMDAccessor *md_accessor, CExpression *pexpr,
								 CExpression *pexprRelational,
								 ExprToColRefMap *phmexprcr,
								 CColRefArray *pdrgpcrArgDQA,
								 CLogicalGbAgg::EAggStage aggStage)
{
	CExpression *pexprPrL = (*pexpr)[1];

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	CColRefArray *pdrgpcrGlobal = popAgg->Pdrgpcr();

	// array of project elements for the local (first), intermediate
	// (second) and global (third) aggregate operator
	CExpressionArray *pdrgpexprPrElFirstStage =
		GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprPrElSecondStage =
		GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgpexprPrElLastStage =
		GPOS_NEW(mp) CExpressionArray(mp);
	BOOL fSpillTo2Level = (aggStage == CLogicalGbAgg::EasTwoStageScalarDQA);



	const ULONG arity = pexprPrL->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrEl = (*pexprPrL)[ul];
		CScalarProjectElement *popScPrEl =
			CScalarProjectElement::PopConvert(pexprPrEl->Pop());

		// get the scalar aggregate function
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

		if (popScAggFunc->IsDistinct())
		{
			// create a new "non-distinct" version of the original aggregate function
			popScAggFunc->MDId()->AddRef();
			CScalarAggFunc *popScAggFuncNew = CUtils::PopAggFunc(
				mp, popScAggFunc->MDId(),
				GPOS_NEW(mp)
					CWStringConst(mp, popScAggFunc->PstrAggFunc()->GetBuffer()),
				false /* is_distinct */, EaggfuncstageGlobal /*eaggfuncstage*/,
				false /* fSplit */
			);

			GPOS_ASSERT(1 == pexprAggFunc->Arity());
			CExpression *pexprArg = (*pexprAggFunc)[0];

			CColRef *pcrDistinctCol = phmexprcr->Find(pexprArg);
			GPOS_ASSERT(NULL != pcrDistinctCol);
			CExpressionArray *pdrgpexprArgs = GPOS_NEW(mp) CExpressionArray(mp);
			pdrgpexprArgs->Append(CUtils::PexprScalarIdent(mp, pcrDistinctCol));

			CExpression *pexprPrElGlobal = CUtils::PexprScalarProjectElement(
				mp, popScPrEl->Pcr(),
				GPOS_NEW(mp) CExpression(mp, popScAggFuncNew, pdrgpexprArgs));

			pdrgpexprPrElLastStage->Append(pexprPrElGlobal);
		}
		else
		{
			// split the regular aggregate function into multi-level aggregate functions
			PopulatePrLMultiPhaseAgg(mp, col_factory, md_accessor, pexprPrEl,
									 pdrgpexprPrElFirstStage,
									 pdrgpexprPrElSecondStage,
									 pdrgpexprPrElLastStage, fSpillTo2Level);
		}
	}

	CExpression *pexprGlobal = PexprMultiLevelAggregation(
		mp, pexprRelational, pdrgpexprPrElFirstStage, pdrgpexprPrElSecondStage,
		pdrgpexprPrElLastStage, pdrgpcrArgDQA, pdrgpcrGlobal, fSpillTo2Level,
		true /* fAddDistinctColToLocalGb */, aggStage /* fTwoStageScalarDQA */
	);

	// clean-up the secondStage if spill to 2 level
	if (fSpillTo2Level)
	{
		pdrgpexprPrElSecondStage->Release();
	}

	return pexprGlobal;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PexprPrElAgg
//
//	@doc:
//		Create an aggregate function  of a particular level and a project
//		project element to hold it
//
//---------------------------------------------------------------------------
CExpression *
CXformSplitDQA::PexprPrElAgg(CMemoryPool *mp, CExpression *pexprAggFunc,
							 EAggfuncStage eaggfuncstage,
							 CColRef *pcrPreviousStage, CColRef *pcrCurrStage)
{
	GPOS_ASSERT(NULL != pexprAggFunc);
	GPOS_ASSERT(NULL != pcrCurrStage);
	GPOS_ASSERT(EaggfuncstageSentinel != eaggfuncstage);

	CScalarAggFunc *popScAggFunc =
		CScalarAggFunc::PopConvert(pexprAggFunc->Pop());
	GPOS_ASSERT(!popScAggFunc->IsDistinct());

	// project element of global aggregation
	CExpressionArray *pdrgpexprArg = NULL;
	if (EaggfuncstageLocal == eaggfuncstage)
	{
		CExpressionArray *pdrgpexprAggOrig = pexprAggFunc->PdrgPexpr();
		pdrgpexprAggOrig->AddRef();
		pdrgpexprArg = pdrgpexprAggOrig;
	}
	else
	{
		pdrgpexprArg = GPOS_NEW(mp) CExpressionArray(mp);
		pdrgpexprArg->Append(CUtils::PexprScalarIdent(mp, pcrPreviousStage));
	}

	popScAggFunc->MDId()->AddRef();
	CScalarAggFunc *popScAggFuncNew = CUtils::PopAggFunc(
		mp, popScAggFunc->MDId(),
		GPOS_NEW(mp)
			CWStringConst(mp, popScAggFunc->PstrAggFunc()->GetBuffer()),
		false,				/*fdistinct */
		eaggfuncstage, true /* fSplit */
	);

	return CUtils::PexprScalarProjectElement(
		mp, pcrCurrStage,
		GPOS_NEW(mp) CExpression(mp, popScAggFuncNew, pdrgpexprArg));
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PopulatePrLMultiPhaseAgg
//
//	@doc:
// 		Given a scalar aggregate generate the local, intermediate and global
// 		aggregate function. Then add it to the project list of the corresponding
// 		aggregate operator at each stage of the multi-stage aggregation
//
//---------------------------------------------------------------------------
void
CXformSplitDQA::PopulatePrLMultiPhaseAgg(
	CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
	CExpression *pexprPrEl, CExpressionArray *pdrgpexprPrElFirstStage,
	CExpressionArray *pdrgpexprPrElSecondStage,
	CExpressionArray *pdrgpexprPrElLastStage, BOOL fSplit2LevelsOnly)
{
	GPOS_ASSERT(NULL != pexprPrEl);
	GPOS_ASSERT(NULL != pdrgpexprPrElFirstStage);
	GPOS_ASSERT_IMP(NULL == pdrgpexprPrElSecondStage, fSplit2LevelsOnly);
	GPOS_ASSERT(NULL != pdrgpexprPrElLastStage);

	// get the components of the project element (agg func)
	CScalarProjectElement *popScPrEl =
		CScalarProjectElement::PopConvert(pexprPrEl->Pop());
	CExpression *pexprAggFunc = (*pexprPrEl)[0];
	CScalarAggFunc *popScAggFunc =
		CScalarAggFunc::PopConvert(pexprAggFunc->Pop());

	const IMDAggregate *pmdagg = md_accessor->RetrieveAgg(popScAggFunc->MDId());
	const IMDType *pmdtype =
		md_accessor->RetrieveType(pmdagg->GetIntermediateResultTypeMdid());

	// create new column reference for the first stage (local) project element
	CColRef *pcrLocal = col_factory->PcrCreate(pmdtype, default_type_modifier);

	CExpression *pexprPrElFirstStage =
		PexprPrElAgg(mp, pexprAggFunc, EaggfuncstageLocal,
					 NULL /*pcrPreviousStage*/, pcrLocal);
	pdrgpexprPrElFirstStage->Append(pexprPrElFirstStage);

	// column reference for the second stage project elements
	CColRef *pcrSecondStage = NULL;
	EAggfuncStage eaggfuncstage = EaggfuncstageIntermediate;
	if (fSplit2LevelsOnly)
	{
		eaggfuncstage = EaggfuncstageGlobal;
		pcrSecondStage = popScPrEl->Pcr();
	}
	else
	{
		// create a new column reference for the second stage (intermediate) project element
		pcrSecondStage = col_factory->PcrCreate(pmdtype, default_type_modifier);
	}

	CExpression *pexprPrElSecondStage =
		PexprPrElAgg(mp, pexprAggFunc, eaggfuncstage, pcrLocal, pcrSecondStage);
	if (fSplit2LevelsOnly)
	{
		pdrgpexprPrElLastStage->Append(pexprPrElSecondStage);
		return;
	}

	pdrgpexprPrElSecondStage->Append(pexprPrElSecondStage);

	// column reference for the third stage project elements
	CColRef *pcrGlobal = popScPrEl->Pcr();
	CExpression *pexprPrElGlobal = PexprPrElAgg(
		mp, pexprAggFunc, EaggfuncstageGlobal, pcrSecondStage, pcrGlobal);

	pdrgpexprPrElLastStage->Append(pexprPrElGlobal);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PcrAggFuncArgument
//
//	@doc:
//		Return the column reference of the argument to the aggregate function.
//		If the argument is a computed column then create a new project element
//		in the child's project list, else just return its column reference
//
//---------------------------------------------------------------------------
CColRef *
CXformSplitDQA::PcrAggFuncArgument(CMemoryPool *mp, CMDAccessor *md_accessor,
								   CColumnFactory *col_factory,
								   CExpression *pexprArg,
								   CExpressionArray *pdrgpexprChildPrEl)
{
	GPOS_ASSERT(NULL != pexprArg);
	GPOS_ASSERT(NULL != pdrgpexprChildPrEl);

	if (COperator::EopScalarIdent == pexprArg->Pop()->Eopid())
	{
		return (const_cast<CColRef *>(
			CScalarIdent::PopConvert(pexprArg->Pop())->Pcr()));
	}

	CScalar *popScalar = CScalar::PopConvert(pexprArg->Pop());
	// computed argument to the input
	const IMDType *pmdtype = md_accessor->RetrieveType(popScalar->MdidType());
	CColRef *pcrAdditionalGrpCol =
		col_factory->PcrCreate(pmdtype, popScalar->TypeModifier());

	pexprArg->AddRef();
	CExpression *pexprPrElNew =
		CUtils::PexprScalarProjectElement(mp, pcrAdditionalGrpCol, pexprArg);

	pdrgpexprChildPrEl->Append(pexprPrElNew);

	return pcrAdditionalGrpCol;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::PexprMultiLevelAggregation
//
//	@doc:
//		Generate an expression with multi-level aggregation
//
//---------------------------------------------------------------------------
CExpression *
CXformSplitDQA::PexprMultiLevelAggregation(
	CMemoryPool *mp, CExpression *pexprRelational,
	CExpressionArray *pdrgpexprPrElFirstStage,
	CExpressionArray *pdrgpexprPrElSecondStage,
	CExpressionArray *pdrgpexprPrElThirdStage, CColRefArray *pdrgpcrArgDQA,
	CColRefArray *pdrgpcrLastStage, BOOL fSplit2LevelsOnly,
	BOOL fAddDistinctColToLocalGb, CLogicalGbAgg::EAggStage aggStage)
{
	GPOS_ASSERT(NULL != pexprRelational);
	GPOS_ASSERT(NULL != pdrgpexprPrElFirstStage);
	GPOS_ASSERT(NULL != pdrgpexprPrElThirdStage);
	GPOS_ASSERT(NULL != pdrgpcrArgDQA);

	GPOS_ASSERT_IMP(!fAddDistinctColToLocalGb, fSplit2LevelsOnly);

	CColRefArray *pdrgpcrLocal = CUtils::PdrgpcrExactCopy(mp, pdrgpcrLastStage);
	const ULONG length = pdrgpcrArgDQA->Size();
	GPOS_ASSERT(0 < length);

	if (fAddDistinctColToLocalGb)
	{
		// add the distinct column to the group by at the first stage of
		// the multi-level aggregation
		CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrLocal);
		for (ULONG ul = 0; ul < length; ul++)
		{
			CColRef *colref = (*pdrgpcrArgDQA)[ul];
			if (!pcrs->FMember(colref))
			{
				pdrgpcrLocal->Append(colref);
				pcrs->Include(colref);
			}
		}
		pcrs->Release();
	}

	CLogicalGbAgg *popFirstStage = NULL;
	CLogicalGbAgg *popSecondStage = NULL;
	CExpressionArray *pdrgpexprLastStage = pdrgpexprPrElSecondStage;
	if (fSplit2LevelsOnly)
	{
		// for scalar DQA the local aggregate is responsible for removing duplicates
		BOOL fLocalAggGeneratesDuplicates = (0 < pdrgpcrLastStage->Size());

		pdrgpcrArgDQA->AddRef();
		popFirstStage = GPOS_NEW(mp) CLogicalGbAgg(
			mp, pdrgpcrLocal, COperator::EgbaggtypeLocal,
			fLocalAggGeneratesDuplicates, pdrgpcrArgDQA, aggStage);
		pdrgpcrLastStage->AddRef();
		popSecondStage = GPOS_NEW(mp) CLogicalGbAgg(
			mp, pdrgpcrLastStage, COperator::EgbaggtypeGlobal, /* egbaggtype */
			aggStage);
		pdrgpexprLastStage = pdrgpexprPrElThirdStage;
	}
	else
	{
		popFirstStage = GPOS_NEW(mp) CLogicalGbAgg(
			mp, pdrgpcrLocal, COperator::EgbaggtypeLocal, /* egbaggtype */
			aggStage);
		pdrgpcrLocal->AddRef();
		pdrgpcrArgDQA->AddRef();
		popSecondStage = GPOS_NEW(mp)
			CLogicalGbAgg(mp, pdrgpcrLocal, COperator::EgbaggtypeIntermediate,
						  false, /* fGeneratesDuplicates */
						  pdrgpcrArgDQA, aggStage);
	}

	pexprRelational->AddRef();
	CExpression *pexprFirstStage = GPOS_NEW(mp) CExpression(
		mp, popFirstStage, pexprRelational,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprPrElFirstStage));

	CExpression *pexprSecondStage = GPOS_NEW(mp) CExpression(
		mp, popSecondStage, pexprFirstStage,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprLastStage));

	if (fSplit2LevelsOnly)
	{
		return pexprSecondStage;
	}

	pdrgpcrLastStage->AddRef();
	CLogicalGbAgg *popGlobalThreeStage = GPOS_NEW(mp) CLogicalGbAgg(
		mp, pdrgpcrLastStage, COperator::EgbaggtypeGlobal, /* egbaggtype */
		aggStage);
	return GPOS_NEW(mp) CExpression(
		mp, popGlobalThreeStage, pexprSecondStage,
		GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp),
								 pdrgpexprPrElThirdStage));
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::ExtractDistinctCols
//
//	@doc:
//		Extract arguments of distinct aggs
//
//---------------------------------------------------------------------------
void
CXformSplitDQA::ExtractDistinctCols(
	CMemoryPool *mp, CColumnFactory *col_factory, CMDAccessor *md_accessor,
	CExpression *pexpr, CExpressionArray *pdrgpexprChildPrEl,
	ExprToColRefMap *phmexprcr,
	CColRefArray **ppdrgpcrArgDQA  // output: array of distinct aggs arguments
)
{
	GPOS_ASSERT(NULL != pdrgpexprChildPrEl);
	GPOS_ASSERT(NULL != ppdrgpcrArgDQA);
	GPOS_ASSERT(NULL != phmexprcr);

	const ULONG arity = pexpr->Arity();
	BOOL hasNonSplittableAgg = false;

	// use a set to deduplicate distinct aggs arguments
	CColRefSet *pcrsArgDQA = GPOS_NEW(mp) CColRefSet(mp);
	ULONG ulDistinct = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrEl = (*pexpr)[ul];

		// get the scalar child of the project element
		CExpression *pexprAggFunc = (*pexprPrEl)[0];
		CScalarAggFunc *popScAggFunc =
			CScalarAggFunc::PopConvert(pexprAggFunc->Pop());
		hasNonSplittableAgg =
			!md_accessor->RetrieveAgg(popScAggFunc->MDId())->IsSplittable();

		// if an agg fucntion is missing a combine function, then such an agg is
		// called non splittable. Non splittable aggs cannot participate in multi-phase DQAs
		// We do not track missing combine functions per DQA so we cannot have some
		// as single phase and some as multiple phases.
		if (hasNonSplittableAgg)
		{
			break;
		}

		if (popScAggFunc->IsDistinct())
		{
			GPOS_ASSERT(1 == pexprAggFunc->Arity());

			CExpression *pexprArg = (*pexprAggFunc)[0];
			GPOS_ASSERT(NULL != pexprArg);
			CColRef *pcrDistinctCol = phmexprcr->Find(pexprArg);
			if (NULL == pcrDistinctCol)
			{
				ulDistinct++;

				// get the column reference of the DQA argument
				pcrDistinctCol = PcrAggFuncArgument(
					mp, md_accessor, col_factory, pexprArg, pdrgpexprChildPrEl);

				// insert into the map between the expression representing the DQA argument
				// and its column reference
				pexprArg->AddRef();
#ifdef GPOS_DEBUG
				BOOL fInserted =
#endif
					phmexprcr->Insert(pexprArg, pcrDistinctCol);
				GPOS_ASSERT(fInserted);

				// add the distinct column to the set of distinct columns
				pcrsArgDQA->Include(pcrDistinctCol);
			}
		}
	}

	if (1 == ulDistinct && !hasNonSplittableAgg)
	{
		*ppdrgpcrArgDQA = pcrsArgDQA->Pdrgpcr(mp);
	}
	else
	{
		// failed to find a single DQA, or agg is defined as non-splittable
		*ppdrgpcrArgDQA = NULL;
	}
	pcrsArgDQA->Release();
}

// EOF
