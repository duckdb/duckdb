//---------------------------------------------------------------------------
//	@filename:
//		CLogicalApply.cpp
//
//	@doc:
//		Implementation of apply operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalApply.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::CLogicalApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalApply::CLogicalApply(CMemoryPool *mp)
	: CLogical(mp), m_pdrgpcrInner(NULL), m_eopidOriginSubq(COperator::EopSentinel)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::CLogicalApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalApply::CLogicalApply(CMemoryPool *mp, CColRefArray *pdrgpcrInner, EOperatorId eopidOriginSubq)
	: CLogical(mp), m_pdrgpcrInner(pdrgpcrInner), m_eopidOriginSubq(eopidOriginSubq)
{
	GPOS_ASSERT(NULL != pdrgpcrInner);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::~CLogicalApply
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalApply::~CLogicalApply()
{
	CRefCount::SafeRelease(m_pdrgpcrInner);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::PcrsStat
//
//	@doc:
//		Compute required stat columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalApply::PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
						CColRefSet *pcrsInput, ULONG child_index) const
{
	GPOS_ASSERT(3 == exprhdl.Arity());

	CColRefSet *pcrsUsed = GPOS_NEW(mp) CColRefSet(mp);
	// add columns used by scalar child
	pcrsUsed->Union(exprhdl.DeriveUsedColumns(2));

	if (0 == child_index)
	{
		// add outer references coming from inner child
		pcrsUsed->Union(exprhdl.DeriveOuterReferences(1));
	}

	CColRefSet *pcrsStat =
		PcrsReqdChildStats(mp, exprhdl, pcrsInput, pcrsUsed, child_index);
	pcrsUsed->Release();

	return pcrsStat;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalApply::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CColRefArray *pdrgpcrInner =
			CLogicalApply::PopConvert(pop)->PdrgPcrInner();
		if (NULL == m_pdrgpcrInner || NULL == pdrgpcrInner)
		{
			return (NULL == m_pdrgpcrInner && NULL == pdrgpcrInner);
		}

		return m_pdrgpcrInner->Equals(pdrgpcrInner);
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalApply::OsPrint(IOstream &os) const
{
	os << this->SzId();
	if (NULL != m_pdrgpcrInner)
	{
		os << " (Reqd Inner Cols: ";
		(void) CUtils::OsPrintDrgPcr(os, m_pdrgpcrInner);
		os << ")";
	}

	return os;
}