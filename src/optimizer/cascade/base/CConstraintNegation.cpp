//---------------------------------------------------------------------------
//	@filename:
//		CConstraintNegation.cpp
//
//	@doc:
//		Implementation of negation constraints
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CConstraintNegation.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CConstraintInterval.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::CConstraintNegation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstraintNegation::CConstraintNegation(CMemoryPool *mp, CConstraint *pcnstr)
	: CConstraint(mp), m_pcnstr(pcnstr)
{
	GPOS_ASSERT(NULL != pcnstr);

	m_pcrsUsed = pcnstr->PcrsUsed();
	m_pcrsUsed->AddRef();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::~CConstraintNegation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstraintNegation::~CConstraintNegation()
{
	m_pcnstr->Release();
	m_pcrsUsed->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::PcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the constraint with remapped columns
//
//---------------------------------------------------------------------------
CConstraint* CConstraintNegation::PcnstrCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CConstraint *pcnstr = m_pcnstr->PcnstrCopyWithRemappedColumns(mp, colref_mapping, must_exist);
	return GPOS_NEW(mp) CConstraintNegation(mp, pcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::Pcnstr
//
//	@doc:
//		Return constraint on a given column
//
//---------------------------------------------------------------------------
CConstraint* CConstraintNegation::Pcnstr(CMemoryPool *mp, const CColRef *colref)
{
	if (!m_pcrsUsed->FMember(colref) || (1 != m_pcrsUsed->Size()))
	{
		// return NULL when the constraint:
		// 1) does not contain the column requested
		// 2) constraint may include other columns as well.
		// for instance, conjunction constraint (NOT a=b) is like:
		//       NOT ({"a" (0), ranges: (-inf, inf) } AND {"b" (1), ranges: (-inf, inf) }))
		// recursing down the constraint will give NOT ({"a" (0), ranges: (-inf, inf) })
		// but that is equivalent to (NOT a) which is not the case.

		return NULL;
	}

	return GPOS_NEW(mp) CConstraintNegation(mp, m_pcnstr->Pcnstr(mp, colref));
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::Pcnstr
//
//	@doc:
//		Return constraint on a given column set
//
//---------------------------------------------------------------------------
CConstraint* CConstraintNegation::Pcnstr(CMemoryPool *mp, CColRefSet *pcrs)
{
	if (!m_pcrsUsed->Equals(pcrs))
	{
		return NULL;
	}

	return GPOS_NEW(mp) CConstraintNegation(mp, m_pcnstr->Pcnstr(mp, pcrs));
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::PcnstrRemapForColumn
//
//	@doc:
//		Return a copy of the constraint for a different column
//
//---------------------------------------------------------------------------
CConstraint* CConstraintNegation::PcnstrRemapForColumn(CMemoryPool *mp, CColRef *colref) const
{
	GPOS_ASSERT(1 == m_pcrsUsed->Size());

	return GPOS_NEW(mp) CConstraintNegation(mp, m_pcnstr->PcnstrRemapForColumn(mp, colref));
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::PexprScalar
//
//	@doc:
//		Scalar expression
//
//---------------------------------------------------------------------------
CExpression* CConstraintNegation::PexprScalar(CMemoryPool *mp)
{
	if (NULL == m_pexprScalar)
	{
		EConstraintType ect = m_pcnstr->Ect();
		if (EctNegation == ect)
		{
			CConstraintNegation *pcn = (CConstraintNegation *) m_pcnstr;
			m_pexprScalar = pcn->PcnstrChild()->PexprScalar(mp);
			m_pexprScalar->AddRef();
		}
		else if (EctInterval == ect)
		{
			CConstraintInterval *pci = (CConstraintInterval *) m_pcnstr;
			CConstraintInterval *pciComp = pci->PciComplement(mp);
			m_pexprScalar = pciComp->PexprScalar(mp);
			m_pexprScalar->AddRef();
			pciComp->Release();
		}
		else
		{
			CExpression *pexpr = m_pcnstr->PexprScalar(mp);
			pexpr->AddRef();
			m_pexprScalar = CUtils::PexprNegate(mp, pexpr);
		}
	}
	return m_pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintNegation::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream& CConstraintNegation::OsPrint(IOstream &os) const
{
	os << "(NOT " << *m_pcnstr << ")";
	return os;
}