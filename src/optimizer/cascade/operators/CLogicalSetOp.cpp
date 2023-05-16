//---------------------------------------------------------------------------
//	@filename:
//		CLogicalSetOp.cpp
//
//	@doc:
//		Implementation of setops
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalSetOp.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/base/CConstraintInterval.h"
#include "duckdb/optimizer/cascade/base/CConstraintNegation.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::CLogicalSetOp
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalSetOp::CLogicalSetOp(CMemoryPool *mp)
	: CLogical(mp),
	  m_pdrgpcrOutput(NULL),
	  m_pdrgpdrgpcrInput(NULL),
	  m_pcrsOutput(NULL),
	  m_pdrgpcrsInput(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::CLogicalSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSetOp::CLogicalSetOp(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
							 CColRef2dArray *pdrgpdrgpcrInput)
	: CLogical(mp),
	  m_pdrgpcrOutput(pdrgpcrOutput),
	  m_pdrgpdrgpcrInput(pdrgpdrgpcrInput),
	  m_pcrsOutput(NULL),
	  m_pdrgpcrsInput(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpdrgpcrInput);

	BuildColumnSets(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::CLogicalSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSetOp::CLogicalSetOp(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
							 CColRefArray *pdrgpcrLeft,
							 CColRefArray *pdrgpcrRight)
	: CLogical(mp), m_pdrgpcrOutput(pdrgpcrOutput), m_pdrgpdrgpcrInput(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpcrLeft);
	GPOS_ASSERT(NULL != pdrgpcrRight);

	m_pdrgpdrgpcrInput = GPOS_NEW(mp) CColRef2dArray(mp, 2);

	m_pdrgpdrgpcrInput->Append(pdrgpcrLeft);
	m_pdrgpdrgpcrInput->Append(pdrgpcrRight);

	BuildColumnSets(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::~CLogicalSetOp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalSetOp::~CLogicalSetOp()
{
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pdrgpdrgpcrInput);
	CRefCount::SafeRelease(m_pcrsOutput);
	CRefCount::SafeRelease(m_pdrgpcrsInput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::BuildColumnSets
//
//	@doc:
//		Build set representation of input/output columns for faster
//		set operations
//
//---------------------------------------------------------------------------
void
CLogicalSetOp::BuildColumnSets(CMemoryPool *mp)
{
	GPOS_ASSERT(NULL != m_pdrgpcrOutput);
	GPOS_ASSERT(NULL != m_pdrgpdrgpcrInput);
	GPOS_ASSERT(NULL == m_pcrsOutput);
	GPOS_ASSERT(NULL == m_pdrgpcrsInput);

	m_pcrsOutput = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcrOutput);
	m_pdrgpcrsInput = GPOS_NEW(mp) CColRefSetArray(mp);
	const ULONG ulChildren = m_pdrgpdrgpcrInput->Size();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CColRefSet *pcrsInput =
			GPOS_NEW(mp) CColRefSet(mp, (*m_pdrgpdrgpcrInput)[ul]);
		m_pdrgpcrsInput->Append(pcrsInput);

		m_pcrsLocalUsed->Include(pcrsInput);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSetOp::DeriveOutputColumns(CMemoryPool *,  // mp
								   CExpressionHandle &
#ifdef GPOS_DEBUG
									   exprhdl
#endif	// GPOS_DEBUG
)
{
#ifdef GPOS_DEBUG
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CColRefSet *pcrsChildOutput = exprhdl.DeriveOutputColumns(ul);
		CColRefSet *pcrsInput = (*m_pdrgpcrsInput)[ul];
		GPOS_ASSERT(pcrsChildOutput->ContainsAll(pcrsInput) &&
					"Unexpected outer references in SetOp input");
	}
#endif	// GPOS_DEBUG

	m_pcrsOutput->AddRef();

	return m_pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalSetOp::DeriveKeyCollection(CMemoryPool *mp,
								   CExpressionHandle &	// exprhdl
) const
{
	// TODO: 3/3/2012 - ; we can do better by remapping the keys between
	// all children and check if they align

	// True set ops return sets, hence, all output columns are keys
	m_pdrgpcrOutput->AddRef();
	return GPOS_NEW(mp) CKeyCollection(mp, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::DerivePartitionInfo
//
//	@doc:
//		Derive partition consumer info
//
//---------------------------------------------------------------------------
CPartInfo *
CLogicalSetOp::DerivePartitionInfo(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const
{
	const ULONG arity = exprhdl.Arity();
	GPOS_ASSERT(0 < arity);

	// start with the part info of the first child
	CPartInfo *ppartinfo = exprhdl.DerivePartitionInfo(0);
	ppartinfo->AddRef();

	for (ULONG ul = 1; ul < arity; ul++)
	{
		CPartInfo *ppartinfoChild = exprhdl.DerivePartitionInfo(ul);
		GPOS_ASSERT(NULL != ppartinfoChild);

		CColRefArray *pdrgpcrInput = (*m_pdrgpdrgpcrInput)[ul];
		GPOS_ASSERT(pdrgpcrInput->Size() == m_pdrgpcrOutput->Size());

		CPartInfo *ppartinfoRemapped =
			ppartinfoChild->PpartinfoWithRemappedKeys(mp, pdrgpcrInput,
													  m_pdrgpcrOutput);
		CPartInfo *ppartinfoCombined =
			CPartInfo::PpartinfoCombine(mp, ppartinfo, ppartinfoRemapped);
		ppartinfoRemapped->Release();

		ppartinfo->Release();
		ppartinfo = ppartinfoCombined;
	}

	return ppartinfo;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalSetOp::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalSetOp *popSetOp = CLogicalSetOp::PopConvert(pop);
	CColRef2dArray *pdrgpdrgpcrInput = popSetOp->PdrgpdrgpcrInput();
	const ULONG arity = pdrgpdrgpcrInput->Size();

	if (arity != m_pdrgpdrgpcrInput->Size() ||
		!m_pdrgpcrOutput->Equals(popSetOp->PdrgpcrOutput()))
	{
		return false;
	}

	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (!(*m_pdrgpdrgpcrInput)[ul]->Equals((*pdrgpdrgpcrInput)[ul]))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PdrgpcrsOutputEquivClasses
//
//	@doc:
//		Get output equivalence classes
//
//---------------------------------------------------------------------------
CColRefSetArray *
CLogicalSetOp::PdrgpcrsOutputEquivClasses(CMemoryPool *mp,
										  CExpressionHandle &exprhdl,
										  BOOL fIntersect) const
{
	const ULONG ulChildren = exprhdl.Arity();
	CColRefSetArray *pdrgpcrs = PdrgpcrsInputMapped(mp, exprhdl, 0 /*ulChild*/);

	for (ULONG ul = 1; ul < ulChildren; ul++)
	{
		CColRefSetArray *pdrgpcrsChild = PdrgpcrsInputMapped(mp, exprhdl, ul);
		CColRefSetArray *pdrgpcrsMerged = NULL;

		if (fIntersect)
		{
			// merge with the equivalence classes we have so far
			pdrgpcrsMerged =
				CUtils::PdrgpcrsMergeEquivClasses(mp, pdrgpcrs, pdrgpcrsChild);
		}
		else
		{
			// in case of a union, an equivalence class must be coming from all
			// children to be part of the output
			pdrgpcrsMerged = CUtils::PdrgpcrsIntersectEquivClasses(
				mp, pdrgpcrs, pdrgpcrsChild);
		}
		pdrgpcrsChild->Release();
		pdrgpcrs->Release();
		pdrgpcrs = pdrgpcrsMerged;
	}

	return pdrgpcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PdrgpcrsInputMapped
//
//	@doc:
//		Get equivalence classes from one input child, mapped to output columns
//
//---------------------------------------------------------------------------
CColRefSetArray *
CLogicalSetOp::PdrgpcrsInputMapped(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   ULONG ulChild) const
{
	CColRefSetArray *pdrgpcrsInput =
		exprhdl.DerivePropertyConstraint(ulChild)->PdrgpcrsEquivClasses();
	const ULONG length = pdrgpcrsInput->Size();

	CColRefSet *pcrsChildInput = (*m_pdrgpcrsInput)[ulChild];
	CColRefSetArray *pdrgpcrs = GPOS_NEW(mp) CColRefSetArray(mp);
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
		pcrs->Include((*pdrgpcrsInput)[ul]);
		pcrs->Intersection(pcrsChildInput);

		if (0 == pcrs->Size())
		{
			pcrs->Release();
			continue;
		}

		// replace each input column with its corresponding output column
		pcrs->Replace((*m_pdrgpdrgpcrInput)[ulChild], m_pdrgpcrOutput);

		pdrgpcrs->Append(pcrs);
	}

	return pdrgpcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PdrgpcnstrColumn
//
//	@doc:
//		Get constraints for a given output column from all children
//
//---------------------------------------------------------------------------
CConstraintArray *
CLogicalSetOp::PdrgpcnstrColumn(CMemoryPool *mp, CExpressionHandle &exprhdl,
								ULONG ulColIndex, ULONG ulStart) const
{
	CConstraintArray *pdrgpcnstr = GPOS_NEW(mp) CConstraintArray(mp);

	CColRef *colref = (*m_pdrgpcrOutput)[ulColIndex];
	if (!CUtils::FConstrainableType(colref->RetrieveType()->MDId()))
	{
		return pdrgpcnstr;
	}

	const ULONG ulChildren = exprhdl.Arity();
	for (ULONG ul = ulStart; ul < ulChildren; ul++)
	{
		CConstraint *pcnstr = PcnstrColumn(mp, exprhdl, ulColIndex, ul);
		if (NULL == pcnstr)
		{
			pcnstr =
				CConstraintInterval::PciUnbounded(mp, colref, true /*is_null*/);
		}
		GPOS_ASSERT(NULL != pcnstr);
		pdrgpcnstr->Append(pcnstr);
	}

	return pdrgpcnstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PcnstrColumn
//
//	@doc:
//		Get constraint for a given output column from a given children
//
//---------------------------------------------------------------------------
CConstraint *
CLogicalSetOp::PcnstrColumn(CMemoryPool *mp, CExpressionHandle &exprhdl,
							ULONG ulColIndex, ULONG ulChild) const
{
	GPOS_ASSERT(ulChild < exprhdl.Arity());

	// constraint from child
	CConstraint *pcnstrChild =
		exprhdl.DerivePropertyConstraint(ulChild)->Pcnstr();
	if (NULL == pcnstrChild)
	{
		return NULL;
	}

	// part of constraint on the current input column
	CConstraint *pcnstrCol =
		pcnstrChild->Pcnstr(mp, (*(*m_pdrgpdrgpcrInput)[ulChild])[ulColIndex]);
	if (NULL == pcnstrCol)
	{
		return NULL;
	}

	// make a copy of this constraint but for the output column instead
	CConstraint *pcnstrOutput =
		pcnstrCol->PcnstrRemapForColumn(mp, (*m_pdrgpcrOutput)[ulColIndex]);
	pcnstrCol->Release();
	return pcnstrOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PpcDeriveConstraintIntersectUnion
//
//	@doc:
//		Derive constraint property for intersect and union operators
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalSetOp::PpcDeriveConstraintIntersectUnion(CMemoryPool *mp,
												 CExpressionHandle &exprhdl,
												 BOOL fIntersect) const
{
	const ULONG num_cols = m_pdrgpcrOutput->Size();

	CConstraintArray *pdrgpcnstr = GPOS_NEW(mp) CConstraintArray(mp);
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		// get constraints for this column from all children
		CConstraintArray *pdrgpcnstrCol =
			PdrgpcnstrColumn(mp, exprhdl, ul, 0 /*ulStart*/);

		CConstraint *pcnstrCol = NULL;
		if (fIntersect)
		{
			pcnstrCol = CConstraint::PcnstrConjunction(mp, pdrgpcnstrCol);
		}
		else
		{
			pcnstrCol = CConstraint::PcnstrDisjunction(mp, pdrgpcnstrCol);
		}

		if (NULL != pcnstrCol)
		{
			pdrgpcnstr->Append(pcnstrCol);
		}
	}

	CConstraint *pcnstrAll = CConstraint::PcnstrConjunction(mp, pdrgpcnstr);

	CColRefSetArray *pdrgpcrs =
		PdrgpcrsOutputEquivClasses(mp, exprhdl, fIntersect);

	return GPOS_NEW(mp) CPropConstraint(mp, pdrgpcrs, pcnstrAll);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::PcrsStat
//
//	@doc:
//		Compute required stats columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSetOp::PcrsStat(CMemoryPool *,		  // mp
						CExpressionHandle &,  //exprhdl,
						CColRefSet *,		  //pcrsInput
						ULONG child_index) const
{
	CColRefSet *pcrs = (*m_pdrgpcrsInput)[child_index];
	pcrs->AddRef();

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSetOp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalSetOp::OsPrint(IOstream &os) const
{
	os << SzId() << " Output: (";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << ")";

	os << ", Input: [";
	const ULONG ulChildren = m_pdrgpdrgpcrInput->Size();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		os << "(";
		CUtils::OsPrintDrgPcr(os, (*m_pdrgpdrgpcrInput)[ul]);
		os << ")";

		if (ul < ulChildren - 1)
		{
			os << ", ";
		}
	}
	os << "]";

	return os;
}