//---------------------------------------------------------------------------
//	@filename:
//		CPartitionPropagationSpec.cpp
//
//	@doc:
//		Specification of partition propagation requirements
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CPartitionPropagationSpec.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/base/CPartIndexMap.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalPartitionSelector.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"

using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::CPartitionPropagationSpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec::CPartitionPropagationSpec(CPartIndexMap *ppim, CPartFilterMap *ppfm)
	: m_ppim(ppim), m_ppfm(ppfm)
{
	GPOS_ASSERT(NULL != ppim);
	GPOS_ASSERT(NULL != ppfm);
}


//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::~CPartitionPropagationSpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec::~CPartitionPropagationSpec()
{
	m_ppim->Release();
	m_ppfm->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::HashValue
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG CPartitionPropagationSpec::HashValue() const
{
	return m_ppim->HashValue();
}


//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::Matches
//
//	@doc:
//		Check whether two partition propagation specs are equal
//
//---------------------------------------------------------------------------
BOOL CPartitionPropagationSpec::Matches(const CPartitionPropagationSpec *ppps) const
{
	return m_ppim->Equals(ppps->Ppim()) && m_ppfm->Equals(ppps->m_ppfm);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void CPartitionPropagationSpec::AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdPropPlan *
#ifdef GPOS_DEBUG
											   prpp
#endif	// GPOS_DEBUG
										   , CExpressionArray *pdrgpexpr, CExpression *pexpr)
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);

	ULongPtrArray *pdrgpul = m_ppim->PdrgpulScanIds(mp);
	const ULONG size = pdrgpul->Size();

	for (ULONG ul = 0; ul < size; ul++)
	{
		ULONG scan_id = *((*pdrgpul)[ul]);
		GPOS_ASSERT(m_ppim->Contains(scan_id));

		if (CPartIndexMap::EpimConsumer != m_ppim->Epim(scan_id) ||
			0 < m_ppim->UlExpectedPropagators(scan_id))
		{
			continue;
		}

		if (!FRequiresPartitionPropagation(mp, pexpr, exprhdl, scan_id))
		{
			continue;
		}

		CExpression *pexprResolver = NULL;

		IMDId *mdid = m_ppim->GetRelMdId(scan_id);
		CColRef2dArray *pdrgpdrgpcrKeys = NULL;
		CPartKeysArray *pdrgppartkeys = m_ppim->Pdrgppartkeys(scan_id);
		CPartConstraint *ppartcnstr = m_ppim->PpartcnstrRel(scan_id);
		UlongToPartConstraintMap *ppartcnstrmap =
			m_ppim->Ppartcnstrmap(scan_id);
		mdid->AddRef();
		ppartcnstr->AddRef();
		ppartcnstrmap->AddRef();
		pexpr->AddRef();

		// check if there is a predicate on this part index id
		UlongToExprMap *phmulexprEqFilter = GPOS_NEW(mp) UlongToExprMap(mp);
		UlongToExprMap *phmulexprFilter = GPOS_NEW(mp) UlongToExprMap(mp);
		CExpression *pexprResidual = NULL;
		if (m_ppfm->FContainsScanId(scan_id))
		{
			CExpression *pexprScalar = PexprFilter(mp, scan_id);

			// find out which keys are used in the predicate, in case there are multiple
			// keys at this point (e.g. from a union of multiple CTE consumers)
			CColRefSet *pcrsUsed = pexprScalar->DeriveUsedColumns();
			const ULONG ulKeysets = pdrgppartkeys->Size();
			for (ULONG ulKey = 0; NULL == pdrgpdrgpcrKeys && ulKey < ulKeysets;
				 ulKey++)
			{
				// get partition key
				CPartKeys *ppartkeys = (*pdrgppartkeys)[ulKey];
				if (ppartkeys->FOverlap(pcrsUsed))
				{
					pdrgpdrgpcrKeys = ppartkeys->Pdrgpdrgpcr();
				}
			}

			// if we cannot find partition keys mapping the partition predicates, fall back to planner
			if (NULL == pdrgpdrgpcrKeys)
			{
				GPOS_RAISE(gpopt::ExmaGPOPT,
						   gpopt::ExmiUnsatisfiedRequiredProperties);
			}

			pdrgpdrgpcrKeys->AddRef();

			// split predicates and put them in the appropriate hashmaps
			SplitPartPredicates(mp, pexprScalar, pdrgpdrgpcrKeys,
								phmulexprEqFilter, phmulexprFilter,
								&pexprResidual);
			pexprScalar->Release();
		}
		else
		{
			// doesn't matter which keys we use here since there is no filter
			GPOS_ASSERT(1 <= pdrgppartkeys->Size());
			pdrgpdrgpcrKeys = (*pdrgppartkeys)[0]->Pdrgpdrgpcr();
			pdrgpdrgpcrKeys->AddRef();
		}

		pexprResolver = GPOS_NEW(mp) CExpression(
			mp,
			GPOS_NEW(mp) CPhysicalPartitionSelector(
				mp, scan_id, mdid, pdrgpdrgpcrKeys, ppartcnstrmap, ppartcnstr,
				phmulexprEqFilter, phmulexprFilter, pexprResidual),
			pexpr);

		pdrgpexpr->Append(pexprResolver);
	}
	pdrgpul->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::PexprFilter
//
//	@doc:
//		Return the filter expression for the given Scan Id
//
//---------------------------------------------------------------------------
CExpression *
CPartitionPropagationSpec::PexprFilter(CMemoryPool *mp, ULONG scan_id)
{
	CExpression *pexprScalar = m_ppfm->Pexpr(scan_id);
	GPOS_ASSERT(NULL != pexprScalar);

	if (CUtils::FScalarIdent(pexprScalar))
	{
		// condition of the form "pkey": translate into pkey = true
		pexprScalar->AddRef();
		pexprScalar = CUtils::PexprScalarEqCmp(
			mp, pexprScalar,
			CUtils::PexprScalarConstBool(mp, true /*value*/,
										 false /*is_null*/));
	}
	else if (CPredicateUtils::FNot(pexprScalar) &&
			 CUtils::FScalarIdent((*pexprScalar)[0]))
	{
		// condition of the form "!pkey": translate into pkey = false
		CExpression *pexprId = (*pexprScalar)[0];
		pexprId->AddRef();

		pexprScalar = CUtils::PexprScalarEqCmp(
			mp, pexprId,
			CUtils::PexprScalarConstBool(mp, false /*value*/,
										 false /*is_null*/));
	}
	else
	{
		pexprScalar->AddRef();
	}

	return pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::FRequiresPartitionPropagation
//
//	@doc:
//		Check if given part index id needs to be enforced on top of the given
//		expression
//
//---------------------------------------------------------------------------
BOOL
CPartitionPropagationSpec::FRequiresPartitionPropagation(
	CMemoryPool *mp, CExpression *pexpr, CExpressionHandle &exprhdl,
	ULONG part_idx_id) const
{
	GPOS_ASSERT(m_ppim->Contains(part_idx_id));

	// construct partition propagation spec with the given id only, and check if it needs to be
	// enforced on top
	CPartIndexMap *ppim = GPOS_NEW(mp) CPartIndexMap(mp);

	IMDId *mdid = m_ppim->GetRelMdId(part_idx_id);
	CPartKeysArray *pdrgppartkeys = m_ppim->Pdrgppartkeys(part_idx_id);
	CPartConstraint *ppartcnstr = m_ppim->PpartcnstrRel(part_idx_id);
	UlongToPartConstraintMap *ppartcnstrmap =
		m_ppim->Ppartcnstrmap(part_idx_id);
	mdid->AddRef();
	pdrgppartkeys->AddRef();
	ppartcnstr->AddRef();
	ppartcnstrmap->AddRef();

	ppim->Insert(part_idx_id, ppartcnstrmap, m_ppim->Epim(part_idx_id),
				 m_ppim->UlExpectedPropagators(part_idx_id), mdid,
				 pdrgppartkeys, ppartcnstr);

	CPartitionPropagationSpec *ppps = GPOS_NEW(mp)
		CPartitionPropagationSpec(ppim, GPOS_NEW(mp) CPartFilterMap(mp));

	CEnfdPartitionPropagation *pepp = GPOS_NEW(mp)
		CEnfdPartitionPropagation(ppps, CEnfdPartitionPropagation::EppmSatisfy,
								  GPOS_NEW(mp) CPartFilterMap(mp));
	CEnfdProp::EPropEnforcingType epetPartitionPropagation =
		pepp->Epet(exprhdl, CPhysical::PopConvert(pexpr->Pop()),
				   true /*fPartitionPropagationRequired*/);

	pepp->Release();

	return CEnfdProp::FEnforce(epetPartitionPropagation);
}

//---------------------------------------------------------------------------
//      @function:
//		CPartitionPropagationSpec::SplitPartPredicates
//
//	@doc:
//		Split the partition elimination predicates over the various levels
//		as well as the residual predicate and add them to the appropriate
//		hashmaps. These are to be used when creating the partition selector
//
//---------------------------------------------------------------------------
void CPartitionPropagationSpec::SplitPartPredicates(CMemoryPool *mp, CExpression *pexprScalar, CColRef2dArray *pdrgpdrgpcrKeys, UlongToExprMap *phmulexprEqFilter, UlongToExprMap *phmulexprFilter, CExpression **ppexprResidual)
{
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != pdrgpdrgpcrKeys);
	GPOS_ASSERT(NULL != phmulexprEqFilter);
	GPOS_ASSERT(NULL != phmulexprFilter);
	GPOS_ASSERT(NULL != ppexprResidual);
	GPOS_ASSERT(NULL == *ppexprResidual);
	CExpressionArray *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
	CBitSet *pbsUsed = GPOS_NEW(mp) CBitSet(mp);
	CColRefSet *pcrsKeys = PcrsKeys(mp, pdrgpdrgpcrKeys);
	const ULONG ulLevels = pdrgpdrgpcrKeys->Size();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CColRef *colref = CUtils::PcrExtractPartKey(pdrgpdrgpcrKeys, ul);
		// find conjuncts for this key and mark their positions
		CExpressionArray *pdrgpexprKey = PdrgpexprPredicatesOnKey(mp, pdrgpexprConjuncts, colref, pcrsKeys, &pbsUsed);
		const ULONG length = pdrgpexprKey->Size();
		if (length == 0)
		{
			// no predicates on this key
			pdrgpexprKey->Release();
			continue;
		}
		if (length == 1 && CPredicateUtils::FIdentCompare((*pdrgpexprKey)[0], IMDType::EcmptEq, colref))
		{
			// EqFilters
			// one equality predicate (key = expr); take out the expression
			// and add it to the equality filters map
			CExpression *pexprPartKey = NULL;
			CExpression *pexprOther = NULL;
			IMDType::ECmpType cmp_type = IMDType::EcmptOther;
			CPredicateUtils::ExtractComponents((*pdrgpexprKey)[0], colref, &pexprPartKey, &pexprOther, &cmp_type);
			GPOS_ASSERT(NULL != pexprOther);
			pexprOther->AddRef();
#ifdef GPOS_DEBUG
			BOOL result =
#endif	// GPOS_DEBUG
				phmulexprEqFilter->Insert(GPOS_NEW(mp) ULONG(ul), pexprOther);
			GPOS_ASSERT(result);
			pdrgpexprKey->Release();
		}
		else
		{
			// Filters
			// more than one predicate on this key or one non-simple-equality predicate
#ifdef GPOS_DEBUG
			BOOL result =
#endif	// GPOS_DEBUG
				phmulexprFilter->Insert(GPOS_NEW(mp) ULONG(ul), CPredicateUtils::PexprConjunction(mp, pdrgpexprKey));
			GPOS_ASSERT(result);
			continue;
		}
	}
	(*ppexprResidual) = PexprResidualFilter(mp, pdrgpexprConjuncts, pbsUsed);
	pcrsKeys->Release();
	pdrgpexprConjuncts->Release();
	pbsUsed->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::PcrsKeys
//
//	@doc:
//		Return a colrefset containing all the part keys
//
//---------------------------------------------------------------------------
CColRefSet* CPartitionPropagationSpec::PcrsKeys(CMemoryPool *mp, CColRef2dArray *pdrgpdrgpcrKeys)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	const ULONG ulLevels = pdrgpdrgpcrKeys->Size();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CColRef *colref = CUtils::PcrExtractPartKey(pdrgpdrgpcrKeys, ul);
		pcrs->Include(colref);
	}
	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::PexprResidualFilter
//
//	@doc:
//		Return a residual filter given an array of predicates and a bitset
//		indicating which predicates have already been used
//
//---------------------------------------------------------------------------
CExpression* CPartitionPropagationSpec::PexprResidualFilter(CMemoryPool *mp, CExpressionArray *pdrgpexpr, CBitSet *pbsUsed)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pbsUsed);
	CExpressionArray *pdrgpexprUnused = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG length = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		if (pbsUsed->Get(ul))
		{
			// predicate already considered
			continue;
		}
		CExpression *pexpr = (*pdrgpexpr)[ul];
		pexpr->AddRef();
		pdrgpexprUnused->Append(pexpr);
	}
	CExpression *pexprResult = CPredicateUtils::PexprConjunction(mp, pdrgpexprUnused);
	if (CUtils::FScalarConstTrue(pexprResult))
	{
		pexprResult->Release();
		pexprResult = NULL;
	}
	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::PdrgpexprPredicatesOnKey
//
//	@doc:
//		Returns an array of predicates on the given partitioning key given
//		an array of predicates on all keys
//
//---------------------------------------------------------------------------
CExpressionArray* CPartitionPropagationSpec::PdrgpexprPredicatesOnKey(CMemoryPool *mp, CExpressionArray *pdrgpexpr, CColRef *colref, CColRefSet *pcrsKeys, CBitSet **ppbs)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != colref);
	GPOS_ASSERT(NULL != ppbs);
	GPOS_ASSERT(NULL != *ppbs);
	CExpressionArray *pdrgpexprResult = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG length = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		if ((*ppbs)->Get(ul))
		{
			// this expression has already been added for another column
			continue;
		}
		CExpression *pexpr = (*pdrgpexpr)[ul];
		GPOS_ASSERT(pexpr->Pop()->FScalar());
		CColRefSet *pcrsUsed = pexpr->DeriveUsedColumns();
		CColRefSet *pcrsUsedKeys = GPOS_NEW(mp) CColRefSet(mp, *pcrsUsed);
		pcrsUsedKeys->Intersection(pcrsKeys);
		if (1 == pcrsUsedKeys->Size() && pcrsUsedKeys->FMember(colref))
		{
			pexpr->AddRef();
			pdrgpexprResult->Append(pexpr);
			(*ppbs)->ExchangeSet(ul);
		}
		pcrsUsedKeys->Release();
	}
	return pdrgpexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream & CPartitionPropagationSpec::OsPrint(IOstream &os) const
{
	os << *m_ppim;
	os << "Filters: [";
	m_ppfm->OsPrint(os);
	os << "]";
	return os;
}