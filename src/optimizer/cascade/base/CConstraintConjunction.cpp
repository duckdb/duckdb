//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintConjunction.cpp
//
//	@doc:
//		Implementation of conjunction constraints
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CConstraintConjunction.h"

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CConstraintInterval.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::CConstraintConjunction
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstraintConjunction::CConstraintConjunction(CMemoryPool *mp, CConstraintArray *pdrgpcnstr)
	: CConstraint(mp), m_pdrgpcnstr(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcnstr);
	m_pdrgpcnstr = PdrgpcnstrFlatten(mp, pdrgpcnstr, EctConjunction);

	const ULONG length = m_pdrgpcnstr->Size();
	GPOS_ASSERT(0 < length);
	m_pcrsUsed = GPOS_NEW(mp) CColRefSet(mp);

	for (ULONG ul = 0; ul < length; ul++)
	{
		CConstraint *pcnstr = (*m_pdrgpcnstr)[ul];
		m_pcrsUsed->Include(pcnstr->PcrsUsed());
	}

	m_phmcolconstr = Phmcolconstr(mp, m_pcrsUsed, m_pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::~CConstraintConjunction
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstraintConjunction::~CConstraintConjunction()
{
	m_pdrgpcnstr->Release();
	m_pcrsUsed->Release();
	m_phmcolconstr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::FContradiction
//
//	@doc:
//		Check if this constraint is a contradiction
//
//---------------------------------------------------------------------------
BOOL
CConstraintConjunction::FContradiction() const
{
	const ULONG length = m_pdrgpcnstr->Size();

	BOOL fContradiction = false;
	for (ULONG ul = 0; !fContradiction && ul < length; ul++)
	{
		fContradiction = (*m_pdrgpcnstr)[ul]->FContradiction();
	}

	return fContradiction;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::FConstraint
//
//	@doc:
//		Check if there is a constraint on the given column
//
//---------------------------------------------------------------------------
BOOL
CConstraintConjunction::FConstraint(const CColRef *colref) const
{
	CConstraintArray *pdrgpcnstrCol = m_phmcolconstr->Find(colref);
	return (NULL != pdrgpcnstrCol && 0 < pdrgpcnstrCol->Size());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::PcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the constraint with remapped columns. If must_exist is
//		set to true, then every column reference in this constraint must be in
//		the hashmap in order to be replace. Otherwise, some columns may not be
//		in the mapping, and hence will not be replaced
//
//---------------------------------------------------------------------------
CConstraint* CConstraintConjunction::PcnstrCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CConstraintArray *pdrgpcnstr = GPOS_NEW(mp) CConstraintArray(mp);
	const ULONG length = m_pdrgpcnstr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CConstraint *pcnstr = (*m_pdrgpcnstr)[ul];
		CConstraint *pcnstrCopy = pcnstr->PcnstrCopyWithRemappedColumns(mp, colref_mapping, must_exist);
		pdrgpcnstr->Append(pcnstrCopy);
	}
	return GPOS_NEW(mp) CConstraintConjunction(mp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::Pcnstr
//
//	@doc:
//		Return constraint on a given column
//
//---------------------------------------------------------------------------
CConstraint* CConstraintConjunction::Pcnstr(CMemoryPool *mp, const CColRef *colref)
{
	// all children referencing given column
	CConstraintArray *pdrgpcnstrCol = m_phmcolconstr->Find(colref);
	if (NULL == pdrgpcnstrCol)
	{
		return NULL;
	}

	CConstraintArray *pdrgpcnstr = GPOS_NEW(mp) CConstraintArray(mp);

	const ULONG length = pdrgpcnstrCol->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		// the part of the child that references this column
		CConstraint *pcnstrCol = (*pdrgpcnstrCol)[ul]->Pcnstr(mp, colref);
		if (NULL == pcnstrCol || pcnstrCol->IsConstraintUnbounded())
		{
			CRefCount::SafeRelease(pcnstrCol);
			continue;
		}
		pdrgpcnstr->Append(pcnstrCol);
	}

	return CConstraint::PcnstrConjunction(mp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::Pcnstr
//
//	@doc:
//		Return constraint on a given column set
//
//---------------------------------------------------------------------------
CConstraint* CConstraintConjunction::Pcnstr(CMemoryPool *mp, CColRefSet *pcrs)
{
	const ULONG length = m_pdrgpcnstr->Size();
	CConstraintArray *pdrgpcnstr = GPOS_NEW(mp) CConstraintArray(mp);

	for (ULONG ul = 0; ul < length; ul++)
	{
		CConstraint *pcnstr = (*m_pdrgpcnstr)[ul];
		if (pcnstr->PcrsUsed()->IsDisjoint(pcrs))
		{
			continue;
		}

		// the part of the child that references these columns
		CConstraint *pcnstrCol = pcnstr->Pcnstr(mp, pcrs);
		if (NULL != pcnstrCol)
		{
			pdrgpcnstr->Append(pcnstrCol);
		}
	}

	return CConstraint::PcnstrConjunction(mp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::PcnstrRemapForColumn
//
//	@doc:
//		Return a copy of the constraint for a different column
//
//---------------------------------------------------------------------------
CConstraint* CConstraintConjunction::PcnstrRemapForColumn(CMemoryPool *mp, CColRef *colref) const
{
	return PcnstrConjDisjRemapForColumn(mp, colref, m_pdrgpcnstr, true /*fConj*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintConjunction::PexprScalar
//
//	@doc:
//		Scalar expression
//
//---------------------------------------------------------------------------
CExpression* CConstraintConjunction::PexprScalar(CMemoryPool *mp)
{
	if (NULL == m_pexprScalar)
	{
		if (FContradiction())
		{
			m_pexprScalar = CUtils::PexprScalarConstBool(mp, false /*fval*/,
														 false /*is_null*/);
		}
		else
		{
			m_pexprScalar = PexprScalarConjDisj(mp, m_pdrgpcnstr, true /*fConj*/);
		}
	}
	return m_pexprScalar;
}