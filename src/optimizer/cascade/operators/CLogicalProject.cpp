//---------------------------------------------------------------------------
//	@filename:
//		CLogicalProject.cpp
//
//	@doc:
//		Implementation of project operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalProject.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"
#include "duckdb/optimizer/cascade/base/CConstraintInterval.h"
#include "duckdb/optimizer/cascade/base/CDefaultComparator.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CPartIndexMap.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CScalarIdent.h"
#include "duckdb/optimizer/cascade/operators/CScalarProjectElement.h"

using namespace gpopt;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::CLogicalProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalProject::CLogicalProject(CMemoryPool *mp) : CLogicalUnary(mp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalProject::DeriveOutputColumns(CMemoryPool *mp,
									 CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(2 == exprhdl.Arity());

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	// the scalar child defines additional columns
	pcrs->Union(exprhdl.DeriveOutputColumns(0));
	pcrs->Union(exprhdl.DeriveDefinedColumns(1));

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalProject::DeriveKeyCollection(CMemoryPool *,	 // mp
									 CExpressionHandle &exprhdl) const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PdrgpcrsEquivClassFromScIdent
//
//	@doc:
//		Return equivalence class from scalar ident project element
//
//---------------------------------------------------------------------------
CColRefSetArray *
CLogicalProject::PdrgpcrsEquivClassFromScIdent(CMemoryPool *mp,
											   CExpression *pexprPrEl,
											   CColRefSet *not_null_columns)
{
	GPOS_ASSERT(NULL != pexprPrEl);

	CScalarProjectElement *popPrEl =
		CScalarProjectElement::PopConvert(pexprPrEl->Pop());
	CColRef *pcrPrEl = popPrEl->Pcr();
	CExpression *pexprScalar = (*pexprPrEl)[0];


	if (EopScalarIdent != pexprScalar->Pop()->Eopid())
	{
		return NULL;
	}

	CScalarIdent *popScIdent = CScalarIdent::PopConvert(pexprScalar->Pop());
	const CColRef *pcrScIdent = popScIdent->Pcr();
	GPOS_ASSERT(pcrPrEl->Id() != pcrScIdent->Id());
	GPOS_ASSERT(pcrPrEl->RetrieveType()->MDId()->Equals(
		pcrScIdent->RetrieveType()->MDId()));

	if (!CUtils::FConstrainableType(pcrPrEl->RetrieveType()->MDId()))
	{
		return NULL;
	}

	BOOL non_nullable = not_null_columns->FMember(pcrScIdent);

	// only add renamed columns to equivalent class if the column is not null-able
	// this is because equality predicates will be inferred from the equivalent class
	// during preprocessing
	if (CColRef::EcrtTable == pcrScIdent->Ecrt() && non_nullable)
	{
		// equivalence class
		CColRefSetArray *pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);

		CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
		pcrs->Include(pcrPrEl);
		pcrs->Include(pcrScIdent);
		pdrgpcrs->Append(pcrs);

		return pdrgpcrs;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::ExtractConstraintFromScConst
//
//	@doc:
//		Extract constraint from scalar constant project element
//
//---------------------------------------------------------------------------
void
CLogicalProject::ExtractConstraintFromScConst(
	CMemoryPool *mp, CExpression *pexprPrEl,
	CConstraintArray *pdrgpcnstr,  // array of range constraints
	CColRefSetArray *pdrgpcrs	   // array of equivalence class
)
{
	GPOS_ASSERT(NULL != pexprPrEl);
	GPOS_ASSERT(NULL != pdrgpcnstr);
	GPOS_ASSERT(NULL != pdrgpcrs);

	CScalarProjectElement *popPrEl =
		CScalarProjectElement::PopConvert(pexprPrEl->Pop());
	CColRef *colref = popPrEl->Pcr();
	CExpression *pexprScalar = (*pexprPrEl)[0];

	IMDId *mdid_type = colref->RetrieveType()->MDId();

	if (EopScalarConst != pexprScalar->Pop()->Eopid() ||
		!CUtils::FConstrainableType(mdid_type))
	{
		return;
	}

	CScalarConst *popConst = CScalarConst::PopConvert(pexprScalar->Pop());
	IDatum *datum = popConst->GetDatum();

	CRangeArray *pdrgprng = GPOS_NEW(mp) CRangeArray(mp);
	BOOL is_null = datum->IsNull();
	if (!is_null)
	{
		datum->AddRef();
		pdrgprng->Append(GPOS_NEW(mp) CRange(COptCtxt::PoctxtFromTLS()->Pcomp(),
											 IMDType::EcmptEq, datum));
	}

	pdrgpcnstr->Append(GPOS_NEW(mp)
						   CConstraintInterval(mp, colref, pdrgprng, is_null));

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(colref);
	pdrgpcrs->Append(pcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalProject::DerivePropertyConstraint(CMemoryPool *mp,
										  CExpressionHandle &exprhdl) const
{
	CExpression *pexprPrL = exprhdl.PexprScalarExactChild(1);

	if (NULL == pexprPrL)
	{
		return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	CConstraintArray *pdrgpcnstr = GPOS_NEW(mp) CConstraintArray(mp);
	CColRefSetArray *pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);

	const ULONG ulProjElems = pexprPrL->Arity();
	for (ULONG ul = 0; ul < ulProjElems; ul++)
	{
		CExpression *pexprPrEl = (*pexprPrL)[ul];
		CExpression *pexprProjected = (*pexprPrEl)[0];

		if (EopScalarConst == pexprProjected->Pop()->Eopid())
		{
			ExtractConstraintFromScConst(mp, pexprPrEl, pdrgpcnstr, pdrgpcrs);
		}
		else
		{
			CColRefSet *not_null_columns =
				exprhdl.DeriveNotNullColumns(0 /*ulChild*/);
			CColRefSetArray *pdrgpcrsChild =
				PdrgpcrsEquivClassFromScIdent(mp, pexprPrEl, not_null_columns);

			if (NULL != pdrgpcrsChild)
			{
				// merge with the equivalence classes we have so far
				CColRefSetArray *pdrgpcrsMerged =
					CUtils::PdrgpcrsMergeEquivClasses(mp, pdrgpcrs,
													  pdrgpcrsChild);

				// clean up
				pdrgpcrs->Release();
				pdrgpcrsChild->Release();

				pdrgpcrs = pdrgpcrsMerged;
			}
		}
	}

	if (0 == pdrgpcnstr->Size() && 0 == pdrgpcrs->Size())
	{
		// no constants or equivalence classes found, so just return the same constraint property of the child
		pdrgpcnstr->Release();
		pdrgpcrs->Release();
		return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	CPropConstraint *ppcChild =
		exprhdl.DerivePropertyConstraint(0 /* ulChild */);

	// equivalence classes coming from child
	CColRefSetArray *pdrgpcrsChild = ppcChild->PdrgpcrsEquivClasses();
	if (NULL != pdrgpcrsChild)
	{
		// merge with the equivalence classes we have so far
		CColRefSetArray *pdrgpcrsMerged =
			CUtils::PdrgpcrsMergeEquivClasses(mp, pdrgpcrs, pdrgpcrsChild);

		// clean up
		pdrgpcrs->Release();
		pdrgpcrs = pdrgpcrsMerged;
	}

	// constraint coming from child
	CConstraint *pcnstr = ppcChild->Pcnstr();
	if (NULL != pcnstr)
	{
		pcnstr->AddRef();
		pdrgpcnstr->Append(pcnstr);
	}

	CConstraint *pcnstrNew = CConstraint::PcnstrConjunction(mp, pdrgpcnstr);

	return GPOS_NEW(mp) CPropConstraint(mp, pdrgpcrs, pcnstrNew);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalProject::DeriveMaxCard(CMemoryPool *,  // mp
							   CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasNonScalarFunction(1))
	{
		// unbounded by default
		return CMaxCard();
	}

	// pass on max card of first child
	return exprhdl.DeriveMaxCard(0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalProject::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfSimplifyProjectWithSubquery);
	(void) xform_set->ExchangeSet(CXform::ExfProject2Apply);
	(void) xform_set->ExchangeSet(CXform::ExfProject2ComputeScalar);
	(void) xform_set->ExchangeSet(CXform::ExfCollapseProject);

	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalProject::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  IStatisticsArray *  // stats_ctxt
) const
{
	UlongToIDatumMap *phmuldatum = GPOS_NEW(mp) UlongToIDatumMap(mp);

	// extract scalar constant expression that can be used for
	// statistics calculation
	CExpression *pexprPrList = exprhdl.PexprScalarRepChild(1 /*child_index*/);
	const ULONG arity = pexprPrList->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprPrElem = (*pexprPrList)[ul];
		GPOS_ASSERT(1 == pexprPrElem->Arity());
		CColRef *colref =
			CScalarProjectElement::PopConvert(pexprPrElem->Pop())->Pcr();

		CExpression *pexprScalar = (*pexprPrElem)[0];
		COperator *pop = pexprScalar->Pop();
		if (COperator::EopScalarConst == pop->Eopid())
		{
			IDatum *datum = CScalarConst::PopConvert(pop)->GetDatum();
			if (datum->StatsMappable())
			{
				datum->AddRef();
#ifdef GPOS_DEBUG
				BOOL fInserted =
#endif
					phmuldatum->Insert(GPOS_NEW(mp) ULONG(colref->Id()), datum);
				GPOS_ASSERT(fInserted);
			}
		}
	}

	IStatistics *stats = PstatsDeriveProject(mp, exprhdl, phmuldatum);

	// clean up
	phmuldatum->Release();

	return stats;
}