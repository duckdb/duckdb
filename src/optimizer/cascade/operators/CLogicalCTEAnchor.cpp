//---------------------------------------------------------------------------
//	@filename:
//		CLogicalCTEAnchor.cpp
//
//	@doc:
//		Implementation of CTE anchor operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalCTEAnchor.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::CLogicalCTEAnchor
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalCTEAnchor::CLogicalCTEAnchor(CMemoryPool *mp)
    : CLogical(mp), m_id(0)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::CLogicalCTEAnchor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalCTEAnchor::CLogicalCTEAnchor(CMemoryPool *mp, ULONG id)
	: CLogical(mp), m_id(id)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet* CLogicalCTEAnchor::DeriveOutputColumns(CMemoryPool* mp, CExpressionHandle &exprhdl)
{
	return PcrsDeriveOutputPassThru(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection* CLogicalCTEAnchor::DeriveKeyCollection(CMemoryPool* mp, CExpressionHandle &exprhdl) const
{
	return PkcDeriveKeysPassThru(exprhdl, 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::DerivePartitionInfo
//
//	@doc:
//		Derive part consumer
//
//---------------------------------------------------------------------------
CPartInfo* CLogicalCTEAnchor::DerivePartitionInfo(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	CPartInfo *ppartinfoChild = exprhdl.DerivePartitionInfo(0);
	GPOS_ASSERT(NULL != ppartinfoChild);

	CExpression *pexprProducer =
		COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(m_id);
	GPOS_ASSERT(NULL != pexprProducer);
	CPartInfo *ppartinfoCTEProducer = pexprProducer->DerivePartitionInfo();

	return CPartInfo::PpartinfoCombine(mp, ppartinfoChild,
									   ppartinfoCTEProducer);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalCTEAnchor::DeriveMaxCard(CMemoryPool *,	 // mp
								 CExpressionHandle &exprhdl) const
{
	// pass on max card of first child
	return exprhdl.DeriveMaxCard(0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEAnchor::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalCTEAnchor *popCTEAnchor = CLogicalCTEAnchor::PopConvert(pop);

	return m_id == popCTEAnchor->Id();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEAnchor::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(), m_id);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalCTEAnchor::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfCTEAnchor2Sequence);
	(void) xform_set->ExchangeSet(CXform::ExfCTEAnchor2TrivialSelect);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEAnchor::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream& CLogicalCTEAnchor::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_id;
	os << ")";
	return os;
}