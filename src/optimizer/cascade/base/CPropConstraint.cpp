//---------------------------------------------------------------------------
//	@filename:
//		CPropConstraint.cpp
//
//	@doc:
//		Implementation of constraint property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CPropConstraint.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CConstraintConjunction.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::CPropConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPropConstraint::CPropConstraint(CMemoryPool *mp, CColRefSetArray *pdrgpcrs, CConstraint *pcnstr)
	: m_pdrgpcrs(pdrgpcrs), m_phmcrcrs(NULL), m_pcnstr(pcnstr)
{
	GPOS_ASSERT(NULL != pdrgpcrs);
	InitHashMap(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::~CPropConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPropConstraint::~CPropConstraint()
{
	m_pdrgpcrs->Release();
	CRefCount::SafeRelease(m_phmcrcrs);
	CRefCount::SafeRelease(m_pcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::InitHashMap
//
//	@doc:
//		Initialize mapping between columns and equivalence classes
//
//---------------------------------------------------------------------------
void
CPropConstraint::InitHashMap(CMemoryPool *mp)
{
	GPOS_ASSERT(NULL == m_phmcrcrs);
	const ULONG ulEquiv = m_pdrgpcrs->Size();

	// m_phmcrcrs is only needed when storing equivalent columns
	if (0 != ulEquiv)
	{
		m_phmcrcrs = GPOS_NEW(mp) ColRefToColRefSetMap(mp);
	}
	for (ULONG ul = 0; ul < ulEquiv; ul++)
	{
		CColRefSet *pcrs = (*m_pdrgpcrs)[ul];

		CColRefSetIter crsi(*pcrs);
		while (crsi.Advance())
		{
			pcrs->AddRef();
#ifdef GPOS_DEBUG
			BOOL fres =
#endif	//GPOS_DEBUG
				m_phmcrcrs->Insert(crsi.Pcr(), pcrs);
			GPOS_ASSERT(fres);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::FContradiction
//
//	@doc:
//		Is this a contradiction
//
//---------------------------------------------------------------------------
BOOL
CPropConstraint::FContradiction() const
{
	return (NULL != m_pcnstr && m_pcnstr->FContradiction());
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::PexprScalarMappedFromEquivCols
//
//	@doc:
//		Return scalar expression on the given column mapped from all constraints
//		on its equivalent columns
//
//---------------------------------------------------------------------------
CExpression *
CPropConstraint::PexprScalarMappedFromEquivCols(
	CMemoryPool *mp, CColRef *colref,
	CPropConstraint *constraintsForOuterRefs) const
{
	if (NULL == m_pcnstr || NULL == m_phmcrcrs)
	{
		return NULL;
	}
	CColRefSet *pcrs = m_phmcrcrs->Find(colref);
	CColRefSet *equivOuterRefs = NULL;

	if (NULL != constraintsForOuterRefs &&
		NULL != constraintsForOuterRefs->m_phmcrcrs)
	{
		equivOuterRefs = constraintsForOuterRefs->m_phmcrcrs->Find(colref);
	}

	if ((NULL == pcrs || 1 == pcrs->Size()) &&
		(NULL == equivOuterRefs || 1 == equivOuterRefs->Size()))
	{
		// we have no columns that are equivalent to 'colref'
		return NULL;
	}

	// get constraints for all other columns in this equivalence class
	// except the current column
	CColRefSet *pcrsEquiv = GPOS_NEW(mp) CColRefSet(mp);
	pcrsEquiv->Include(pcrs);
	if (NULL != equivOuterRefs)
	{
		pcrsEquiv->Include(equivOuterRefs);
	}
	pcrsEquiv->Exclude(colref);

	// local constraints on the equivalent column(s)
	CConstraint *pcnstr = m_pcnstr->Pcnstr(mp, pcrsEquiv);
	CConstraint *pcnstrFromOuterRefs = NULL;

	if (NULL != constraintsForOuterRefs &&
		NULL != constraintsForOuterRefs->m_pcnstr)
	{
		// constraints that exist in the outer scope
		pcnstrFromOuterRefs =
			constraintsForOuterRefs->m_pcnstr->Pcnstr(mp, pcrsEquiv);
	}
	pcrsEquiv->Release();
	CRefCount::SafeRelease(equivOuterRefs);

	// combine local and outer ref constraints, if we have any, into pcnstr
	if (NULL == pcnstr && NULL == pcnstrFromOuterRefs)
	{
		// neither local nor outer ref constraints
		return NULL;
	}
	else if (NULL == pcnstr)
	{
		// only constraints from outer refs, move to pcnstr
		pcnstr = pcnstrFromOuterRefs;
		pcnstrFromOuterRefs = NULL;
	}
	else if (NULL != pcnstr && NULL != pcnstrFromOuterRefs)
	{
		// constraints from both local and outer refs, make a conjunction
		// and store it in pcnstr
		CConstraintArray *conjArray = GPOS_NEW(mp) CConstraintArray(mp);

		conjArray->Append(pcnstr);
		conjArray->Append(pcnstrFromOuterRefs);
		pcnstrFromOuterRefs = NULL;
		pcnstr = GPOS_NEW(mp) CConstraintConjunction(mp, conjArray);
	}

	// Now, pcnstr contains constraints on columns that are equivalent
	// to 'colref'. These constraints may be local or in an outer scope.
	// Generate a copy of all these constraints for the current column.
	CConstraint *pcnstrCol = pcnstr->PcnstrRemapForColumn(mp, colref);
	CExpression *pexprScalar = pcnstrCol->PexprScalar(mp);
	pexprScalar->AddRef();

	pcnstr->Release();
	GPOS_ASSERT(NULL == pcnstrFromOuterRefs);
	pcnstrCol->Release();

	return pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPropConstraint::OsPrint(IOstream &os) const
{
	const ULONG length = m_pdrgpcrs->Size();
	if (0 < length)
	{
		os << "Equivalence Classes: { ";

		for (ULONG ul = 0; ul < length; ul++)
		{
			CColRefSet *pcrs = (*m_pdrgpcrs)[ul];
			os << "(" << *pcrs << ") ";
		}

		os << "} ";
	}

	if (NULL != m_pcnstr)
	{
		os << "Constraint:" << *m_pcnstr;
	}

	return os;
}