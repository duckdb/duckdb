//---------------------------------------------------------------------------
//	@filename:
//		CPartConstraint.cpp
//
//	@doc:
//		Implementation of part constraints
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/metadata/CPartConstraint.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CConstraint.h"
#include "duckdb/optimizer/cascade/base/CConstraintConjunction.h"
#include "duckdb/optimizer/cascade/base/CConstraintNegation.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::CPartConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartConstraint::CPartConstraint(CMemoryPool *mp, UlongToConstraintMap *phmulcnstr, CBitSet *pbsDefaultParts, BOOL is_unbounded, CColRef2dArray *pdrgpdrgpcr)
	: m_phmulcnstr(phmulcnstr), m_pbsDefaultParts(pbsDefaultParts), m_is_unbounded(is_unbounded), m_fUninterpreted(false), m_pdrgpdrgpcr(pdrgpdrgpcr)
{
	GPOS_ASSERT(NULL != phmulcnstr);
	GPOS_ASSERT(NULL != pbsDefaultParts);
	GPOS_ASSERT(NULL != pdrgpdrgpcr);
	m_num_of_part_levels = pdrgpdrgpcr->Size();
	GPOS_ASSERT_IMP(is_unbounded, FAllDefaultPartsIncluded());

	m_pcnstrCombined = PcnstrBuildCombined(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::CPartConstraint
//
//	@doc:
//		Ctor - shortcut for single-level
//
//---------------------------------------------------------------------------
CPartConstraint::CPartConstraint(CMemoryPool *mp, CConstraint *pcnstr, BOOL fDefaultPartition, BOOL is_unbounded)
	: m_phmulcnstr(NULL), m_pbsDefaultParts(NULL), m_is_unbounded(is_unbounded), m_fUninterpreted(false)
{
	GPOS_ASSERT(NULL != pcnstr);
	GPOS_ASSERT_IMP(is_unbounded, fDefaultPartition);

	m_phmulcnstr = GPOS_NEW(mp) UlongToConstraintMap(mp);
#ifdef GPOS_DEBUG
	BOOL result =
#endif	// GPOS_DEBUG
		m_phmulcnstr->Insert(GPOS_NEW(mp) ULONG(0 /*ulLevel*/), pcnstr);
	GPOS_ASSERT(result);

	CColRefSet *pcrsUsed = pcnstr->PcrsUsed();
	GPOS_ASSERT(1 == pcrsUsed->Size());
	CColRef *pcrPartKey = pcrsUsed->PcrFirst();

	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	colref_array->Append(pcrPartKey);

	m_pdrgpdrgpcr = GPOS_NEW(mp) CColRef2dArray(mp);
	m_pdrgpdrgpcr->Append(colref_array);

	m_num_of_part_levels = 1;
	m_pbsDefaultParts = GPOS_NEW(mp) CBitSet(mp);
	if (fDefaultPartition)
	{
		m_pbsDefaultParts->ExchangeSet(0 /*ulBit*/);
	}

	pcnstr->AddRef();
	m_pcnstrCombined = pcnstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::CPartConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartConstraint::CPartConstraint(BOOL fUninterpreted)
	: m_phmulcnstr(NULL), m_pbsDefaultParts(NULL), m_num_of_part_levels(1), m_is_unbounded(false), m_fUninterpreted(fUninterpreted), m_pdrgpdrgpcr(NULL), m_pcnstrCombined(NULL)
{
	GPOS_ASSERT(fUninterpreted);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::~CPartConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartConstraint::~CPartConstraint()
{
	CRefCount::SafeRelease(m_phmulcnstr);
	CRefCount::SafeRelease(m_pbsDefaultParts);
	CRefCount::SafeRelease(m_pdrgpdrgpcr);
	CRefCount::SafeRelease(m_pcnstrCombined);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PcnstrBuildCombined
//
//	@doc:
//		Construct the combined constraint
//
//---------------------------------------------------------------------------
CConstraint* CPartConstraint::PcnstrBuildCombined(CMemoryPool *mp)
{
	CConstraintArray *pdrgpcnstr = GPOS_NEW(mp) CConstraintArray(mp);
	for (ULONG ul = 0; ul < m_num_of_part_levels; ul++)
	{
		CConstraint *pcnstr = m_phmulcnstr->Find(&ul);
		if (NULL != pcnstr)
		{
			pcnstr->AddRef();
			pdrgpcnstr->Append(pcnstr);
		}
	}

	return CConstraint::PcnstrConjunction(mp, pdrgpcnstr);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FAllDefaultPartsIncluded
//
//	@doc:
//		Are all default partitions on all levels included
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FAllDefaultPartsIncluded()
{
	for (ULONG ul = 0; ul < m_num_of_part_levels; ul++)
	{
		if (!IsDefaultPartition(ul))
		{
			return false;
		}
	}

	return true;
}
#endif	//GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::IsConstraintUnbounded
//
//	@doc:
//		Is part constraint unbounded
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::IsConstraintUnbounded() const
{
	return m_is_unbounded;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FEquivalent
//
//	@doc:
//		Are constraints equivalent
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FEquivalent(const CPartConstraint *ppartcnstr) const
{
	GPOS_ASSERT(NULL != ppartcnstr);

	if (m_fUninterpreted || ppartcnstr->FUninterpreted())
	{
		return m_fUninterpreted && ppartcnstr->FUninterpreted();
	}

	if (IsConstraintUnbounded())
	{
		return ppartcnstr->IsConstraintUnbounded();
	}

	return m_num_of_part_levels == ppartcnstr->m_num_of_part_levels &&
		   m_pbsDefaultParts->Equals(ppartcnstr->m_pbsDefaultParts) &&
		   FEqualConstrMaps(m_phmulcnstr, ppartcnstr->m_phmulcnstr,
							m_num_of_part_levels);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FEqualConstrMaps
//
//	@doc:
//		Check if two constaint maps have the same constraints
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FEqualConstrMaps(UlongToConstraintMap *phmulcnstrFst,
								  UlongToConstraintMap *phmulcnstrSnd,
								  ULONG ulLevels)
{
	if (phmulcnstrFst->Size() != phmulcnstrSnd->Size())
	{
		return false;
	}

	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CConstraint *pcnstrFst = phmulcnstrFst->Find(&ul);
		CConstraint *pcnstrSnd = phmulcnstrSnd->Find(&ul);

		if ((NULL == pcnstrFst || NULL == pcnstrSnd) && pcnstrFst != pcnstrSnd)
		{
			return false;
		}

		if (NULL != pcnstrFst && !pcnstrFst->Equals(pcnstrSnd))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::Pcnstr
//
//	@doc:
//		Constraint at given level
//
//---------------------------------------------------------------------------
CConstraint *
CPartConstraint::Pcnstr(ULONG ulLevel) const
{
	GPOS_ASSERT(!m_fUninterpreted &&
				"Calling Pcnstr on uninterpreted partition constraint");
	return m_phmulcnstr->Find(&ulLevel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FOverlapLevel
//
//	@doc:
//		Does the current constraint overlap with given one at the given level
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FOverlapLevel(CMemoryPool *mp,
							   const CPartConstraint *ppartcnstr,
							   ULONG ulLevel) const
{
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(!IsConstraintUnbounded());
	GPOS_ASSERT(!ppartcnstr->IsConstraintUnbounded());

	CConstraintArray *pdrgpcnstr = GPOS_NEW(mp) CConstraintArray(mp);
	CConstraint *pcnstrCurrent = Pcnstr(ulLevel);
	CConstraint *pcnstrOther = ppartcnstr->Pcnstr(ulLevel);
	GPOS_ASSERT(NULL != pcnstrCurrent);
	GPOS_ASSERT(NULL != pcnstrOther);

	pcnstrCurrent->AddRef();
	pcnstrOther->AddRef();
	pdrgpcnstr->Append(pcnstrCurrent);
	pdrgpcnstr->Append(pcnstrOther);

	CConstraint *pcnstrIntersect =
		CConstraint::PcnstrConjunction(mp, pdrgpcnstr);

	BOOL fOverlap = !pcnstrIntersect->FContradiction();
	pcnstrIntersect->Release();

	return fOverlap || (IsDefaultPartition(ulLevel) &&
						ppartcnstr->IsDefaultPartition(ulLevel));
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FOverlap
//
//	@doc:
//		Does constraint overlap with given one
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FOverlap(CMemoryPool *mp,
						  const CPartConstraint *ppartcnstr) const
{
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(!m_fUninterpreted &&
				"Calling FOverlap on uninterpreted partition constraint");

	if (IsConstraintUnbounded() || ppartcnstr->IsConstraintUnbounded())
	{
		return true;
	}

	for (ULONG ul = 0; ul < m_num_of_part_levels; ul++)
	{
		if (!FOverlapLevel(mp, ppartcnstr, ul))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FSubsume
//
//	@doc:
//		Does constraint subsume given one
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FSubsume(const CPartConstraint *ppartcnstr) const
{
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(!m_fUninterpreted &&
				"Calling FSubsume on uninterpreted partition constraint");

	if (IsConstraintUnbounded())
	{
		return true;
	}

	if (ppartcnstr->IsConstraintUnbounded())
	{
		return false;
	}

	BOOL fSubsumeLevel = true;
	for (ULONG ul = 0; ul < m_num_of_part_levels && fSubsumeLevel; ul++)
	{
		CConstraint *pcnstrCurrent = Pcnstr(ul);
		CConstraint *pcnstrOther = ppartcnstr->Pcnstr(ul);
		GPOS_ASSERT(NULL != pcnstrCurrent);
		GPOS_ASSERT(NULL != pcnstrOther);

		fSubsumeLevel =
			pcnstrCurrent->Contains(pcnstrOther) &&
			(IsDefaultPartition(ul) || !ppartcnstr->IsDefaultPartition(ul));
	}

	return fSubsumeLevel;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FCanNegate
//
//	@doc:
//		Check whether or not the current part constraint can be negated. A part
//		constraint can be negated only if it has constraints on the first level
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FCanNegate() const
{
	// first level cannot be NULL
	if (NULL == Pcnstr(0))
	{
		return false;
	}

	// all levels after the first must be unconstrained
	for (ULONG ul = 1; ul < m_num_of_part_levels; ul++)
	{
		CConstraint *pcnstr = Pcnstr(ul);
		if (NULL == pcnstr || !pcnstr->IsConstraintUnbounded())
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PpartcnstrRemaining
//
//	@doc:
//		Return what remains of the current part constraint after taking out
//		the given part constraint. Returns NULL is the difference cannot be
//		performed
//
//---------------------------------------------------------------------------
CPartConstraint *
CPartConstraint::PpartcnstrRemaining(CMemoryPool *mp,
									 CPartConstraint *ppartcnstr)
{
	GPOS_ASSERT(
		!m_fUninterpreted &&
		"Calling PpartcnstrRemaining on uninterpreted partition constraint");
	GPOS_ASSERT(NULL != ppartcnstr);

	if (m_num_of_part_levels != ppartcnstr->m_num_of_part_levels ||
		!ppartcnstr->FCanNegate())
	{
		return NULL;
	}

	UlongToConstraintMap *phmulcnstr = GPOS_NEW(mp) UlongToConstraintMap(mp);
	CBitSet *pbsDefaultParts = GPOS_NEW(mp) CBitSet(mp);

	// constraint on first level
	CConstraint *pcnstrCurrent = Pcnstr(0 /*ulLevel*/);
	CConstraint *pcnstrOther = ppartcnstr->Pcnstr(0 /*ulLevel*/);

	CConstraint *pcnstrRemaining =
		PcnstrRemaining(mp, pcnstrCurrent, pcnstrOther);

#ifdef GPOS_DEBUG
	BOOL result =
#endif	// GPOS_DEBUG
		phmulcnstr->Insert(GPOS_NEW(mp) ULONG(0), pcnstrRemaining);
	GPOS_ASSERT(result);

	if (IsDefaultPartition(0 /*ulLevel*/) &&
		!ppartcnstr->IsDefaultPartition(0 /*ulLevel*/))
	{
		pbsDefaultParts->ExchangeSet(0 /*ulBit*/);
	}

	// copy the remaining constraints and default partition flags
	for (ULONG ul = 1; ul < m_num_of_part_levels; ul++)
	{
		CConstraint *pcnstrLevel = Pcnstr(ul);
		if (NULL != pcnstrLevel)
		{
			pcnstrLevel->AddRef();
#ifdef GPOS_DEBUG
			BOOL result =
#endif	// GPOS_DEBUG
				phmulcnstr->Insert(GPOS_NEW(mp) ULONG(ul), pcnstrLevel);
			GPOS_ASSERT(result);
		}

		if (IsDefaultPartition(ul))
		{
			pbsDefaultParts->ExchangeSet(ul);
		}
	}

	m_pdrgpdrgpcr->AddRef();
	return GPOS_NEW(mp) CPartConstraint(mp, phmulcnstr, pbsDefaultParts,
										false /*is_unbounded*/, m_pdrgpdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PcnstrRemaining
//
//	@doc:
//		Return the remaining part of the first constraint that is not covered by
//		the second constraint
//
//---------------------------------------------------------------------------
CConstraint *
CPartConstraint::PcnstrRemaining(CMemoryPool *mp, CConstraint *pcnstrFst,
								 CConstraint *pcnstrSnd)
{
	GPOS_ASSERT(NULL != pcnstrSnd);

	pcnstrSnd->AddRef();
	CConstraint *pcnstrNegation =
		GPOS_NEW(mp) CConstraintNegation(mp, pcnstrSnd);

	if (NULL == pcnstrFst || pcnstrFst->IsConstraintUnbounded())
	{
		return pcnstrNegation;
	}

	CConstraintArray *pdrgpcnstr = GPOS_NEW(mp) CConstraintArray(mp);
	pcnstrFst->AddRef();
	pdrgpcnstr->Append(pcnstrFst);
	pdrgpcnstr->Append(pcnstrNegation);

	return GPOS_NEW(mp) CConstraintConjunction(mp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PpartcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the part constraint with remapped columns
//
//---------------------------------------------------------------------------
CPartConstraint *
CPartConstraint::PpartcnstrCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	if (m_fUninterpreted)
	{
		return GPOS_NEW(mp) CPartConstraint(true /*m_fUninterpreted*/);
	}

	UlongToConstraintMap *phmulcnstr = GPOS_NEW(mp) UlongToConstraintMap(mp);
	CColRef2dArray *pdrgpdrgpcr = GPOS_NEW(mp) CColRef2dArray(mp);

	for (ULONG ul = 0; ul < m_num_of_part_levels; ul++)
	{
		CColRefArray *colref_array = (*m_pdrgpdrgpcr)[ul];
		CColRefArray *pdrgpcrMapped =
			CUtils::PdrgpcrRemap(mp, colref_array, colref_mapping, must_exist);
		pdrgpdrgpcr->Append(pdrgpcrMapped);

		CConstraint *pcnstr = Pcnstr(ul);
		if (NULL != pcnstr)
		{
			CConstraint *pcnstrRemapped = pcnstr->PcnstrCopyWithRemappedColumns(
				mp, colref_mapping, must_exist);
#ifdef GPOS_DEBUG
			BOOL result =
#endif	// GPOS_DEBUG
				phmulcnstr->Insert(GPOS_NEW(mp) ULONG(ul), pcnstrRemapped);
			GPOS_ASSERT(result);
		}
	}

	m_pbsDefaultParts->AddRef();
	return GPOS_NEW(mp) CPartConstraint(mp, phmulcnstr, m_pbsDefaultParts,
										m_is_unbounded, pdrgpdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPartConstraint::OsPrint(IOstream &os) const
{
	os << "Part constraint: (";
	if (m_fUninterpreted)
	{
		os << "uninterpreted)";
		return os;
	}

	for (ULONG ul = 0; ul < m_num_of_part_levels; ul++)
	{
		if (ul > 0)
		{
			os << ", ";
		}
		CConstraint *pcnstr = Pcnstr(ul);
		if (NULL != pcnstr)
		{
			pcnstr->OsPrint(os);
		}
		else
		{
			os << "-";
		}
	}

	os << ", default partitions on levels: " << *m_pbsDefaultParts
	   << ", unbounded: " << m_is_unbounded;
	os << ")";
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FDisjunctionPossible
//
//	@doc:
//		Check if it is possible to produce a disjunction of the two given part
//		constraints. This is possible if the first ulLevels-1 have the same
//		constraints and default flags for both part constraints
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FDisjunctionPossible(CPartConstraint *ppartcnstrFst,
									  CPartConstraint *ppartcnstrSnd)
{
	GPOS_ASSERT(NULL != ppartcnstrFst);
	GPOS_ASSERT(NULL != ppartcnstrSnd);
	GPOS_ASSERT(ppartcnstrFst->m_num_of_part_levels ==
				ppartcnstrSnd->m_num_of_part_levels);

	const ULONG ulLevels = ppartcnstrFst->m_num_of_part_levels;
	BOOL fSuccess = true;

	for (ULONG ul = 0; fSuccess && ul < ulLevels - 1; ul++)
	{
		CConstraint *pcnstrFst = ppartcnstrFst->Pcnstr(ul);
		CConstraint *pcnstrSnd = ppartcnstrSnd->Pcnstr(ul);
		fSuccess = (NULL != pcnstrFst && NULL != pcnstrSnd &&
					pcnstrFst->Equals(pcnstrSnd) &&
					ppartcnstrFst->IsDefaultPartition(ul) ==
						ppartcnstrSnd->IsDefaultPartition(ul));
	}

	// last level constraints cannot be NULL as well
	fSuccess = (fSuccess && NULL != ppartcnstrFst->Pcnstr(ulLevels - 1) &&
				NULL != ppartcnstrSnd->Pcnstr(ulLevels - 1));

	return fSuccess;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PpartcnstrDisjunction
//
//	@doc:
//		Construct a disjunction of the two part constraints. We can only
//		construct this disjunction if they differ only on the last level
//
//---------------------------------------------------------------------------
CPartConstraint *
CPartConstraint::PpartcnstrDisjunction(CMemoryPool *mp,
									   CPartConstraint *ppartcnstrFst,
									   CPartConstraint *ppartcnstrSnd)
{
	GPOS_ASSERT(NULL != ppartcnstrFst);
	GPOS_ASSERT(NULL != ppartcnstrSnd);

	if (ppartcnstrFst->IsConstraintUnbounded())
	{
		ppartcnstrFst->AddRef();
		return ppartcnstrFst;
	}

	if (ppartcnstrSnd->IsConstraintUnbounded())
	{
		ppartcnstrSnd->AddRef();
		return ppartcnstrSnd;
	}

	if (!FDisjunctionPossible(ppartcnstrFst, ppartcnstrSnd))
	{
		return NULL;
	}

	UlongToConstraintMap *phmulcnstr = GPOS_NEW(mp) UlongToConstraintMap(mp);
	CBitSet *pbsCombined = GPOS_NEW(mp) CBitSet(mp);

	const ULONG ulLevels = ppartcnstrFst->m_num_of_part_levels;
	for (ULONG ul = 0; ul < ulLevels - 1; ul++)
	{
		CConstraint *pcnstrFst = ppartcnstrFst->Pcnstr(ul);

		pcnstrFst->AddRef();
#ifdef GPOS_DEBUG
		BOOL result =
#endif	// GPOS_DEBUG
			phmulcnstr->Insert(GPOS_NEW(mp) ULONG(ul), pcnstrFst);
		GPOS_ASSERT(result);

		if (ppartcnstrFst->IsDefaultPartition(ul))
		{
			pbsCombined->ExchangeSet(ul);
		}
	}

	// create the disjunction between the constraints of the last level
	CConstraint *pcnstrFst = ppartcnstrFst->Pcnstr(ulLevels - 1);
	CConstraint *pcnstrSnd = ppartcnstrSnd->Pcnstr(ulLevels - 1);

	pcnstrFst->AddRef();
	pcnstrSnd->AddRef();
	CConstraintArray *pdrgpcnstrCombined = GPOS_NEW(mp) CConstraintArray(mp);

	pdrgpcnstrCombined->Append(pcnstrFst);
	pdrgpcnstrCombined->Append(pcnstrSnd);

	CConstraint *pcnstrDisj =
		CConstraint::PcnstrDisjunction(mp, pdrgpcnstrCombined);
	GPOS_ASSERT(NULL != pcnstrDisj);
#ifdef GPOS_DEBUG
	BOOL result =
#endif	// GPOS_DEBUG
		phmulcnstr->Insert(GPOS_NEW(mp) ULONG(ulLevels - 1), pcnstrDisj);
	GPOS_ASSERT(result);

	if (ppartcnstrFst->IsDefaultPartition(ulLevels - 1) ||
		ppartcnstrSnd->IsDefaultPartition(ulLevels - 1))
	{
		pbsCombined->ExchangeSet(ulLevels - 1);
	}

	CColRef2dArray *pdrgpdrgpcr = ppartcnstrFst->Pdrgpdrgpcr();
	pdrgpdrgpcr->AddRef();
	return GPOS_NEW(mp) CPartConstraint(mp, phmulcnstr, pbsCombined,
										false /*is_unbounded*/, pdrgpdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::CopyPartConstraints
//
//	@doc:
//		Copy the part constraints to the given destination part constraint map
//
//---------------------------------------------------------------------------
void
CPartConstraint::CopyPartConstraints(
	CMemoryPool *mp, UlongToPartConstraintMap *ppartcnstrmapDest,
	UlongToPartConstraintMap *ppartcnstrmapSource)
{
	GPOS_ASSERT(NULL != ppartcnstrmapDest);
	GPOS_ASSERT(NULL != ppartcnstrmapSource);

	UlongToPartConstraintMapIter pcmi(ppartcnstrmapSource);

	while (pcmi.Advance())
	{
		ULONG ulKey = *(pcmi.Key());
		CPartConstraint *ppartcnstrSource =
			const_cast<CPartConstraint *>(pcmi.Value());

		CPartConstraint *ppartcnstrDest = ppartcnstrmapDest->Find(&ulKey);
		GPOS_ASSERT_IMP(NULL != ppartcnstrDest,
						ppartcnstrDest->FEquivalent(ppartcnstrSource));

		if (NULL == ppartcnstrDest)
		{
			ppartcnstrSource->AddRef();

#ifdef GPOS_DEBUG
			BOOL result =
#endif	// GPOS_DEBUG
				ppartcnstrmapDest->Insert(GPOS_NEW(mp) ULONG(ulKey),
										  ppartcnstrSource);

			GPOS_ASSERT(result && "Duplicate part constraints");
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PpartcnstrmapCombine
//
//	@doc:
//		Combine the two given part constraint maps and return the result
//
//---------------------------------------------------------------------------
UlongToPartConstraintMap *
CPartConstraint::PpartcnstrmapCombine(
	CMemoryPool *mp, UlongToPartConstraintMap *ppartcnstrmapFst,
	UlongToPartConstraintMap *ppartcnstrmapSnd)
{
	if (NULL == ppartcnstrmapFst && NULL == ppartcnstrmapSnd)
	{
		return NULL;
	}

	if (NULL == ppartcnstrmapFst)
	{
		ppartcnstrmapSnd->AddRef();
		return ppartcnstrmapSnd;
	}

	if (NULL == ppartcnstrmapSnd)
	{
		ppartcnstrmapFst->AddRef();
		return ppartcnstrmapFst;
	}

	GPOS_ASSERT(NULL != ppartcnstrmapFst);
	GPOS_ASSERT(NULL != ppartcnstrmapSnd);

	UlongToPartConstraintMap *ppartcnstrmap =
		GPOS_NEW(mp) UlongToPartConstraintMap(mp);

	CopyPartConstraints(mp, ppartcnstrmap, ppartcnstrmapFst);
	CopyPartConstraints(mp, ppartcnstrmap, ppartcnstrmapSnd);

	return ppartcnstrmap;
}