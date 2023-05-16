//---------------------------------------------------------------------------
//	@filename:
//		CLogicalBitmapTableGet.cpp
//
//	@doc:
//		Logical operator for table access via bitmap indexes.
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalBitmapTableGet.h"
#include "duckdb/optimizer/cascade/metadata/CTableDescriptor.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::CLogicalBitmapTableGet
//
//	@doc:
//		Ctor
//		Takes ownership of ptabdesc, pnameTableAlias and pdrgpcrOutput.
//
//---------------------------------------------------------------------------
CLogicalBitmapTableGet::CLogicalBitmapTableGet(CMemoryPool *mp, CTableDescriptor *ptabdesc, ULONG ulOriginOpId, const CName *pnameTableAlias, CColRefArray *pdrgpcrOutput)
	: CLogical(mp), m_ptabdesc(ptabdesc), m_ulOriginOpId(ulOriginOpId), m_pnameTableAlias(pnameTableAlias), m_pdrgpcrOutput(pdrgpcrOutput)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameTableAlias);
	GPOS_ASSERT(NULL != pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::CLogicalBitmapTableGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalBitmapTableGet::CLogicalBitmapTableGet(CMemoryPool *mp)
	: CLogical(mp), m_ptabdesc(NULL), m_ulOriginOpId(gpos::ulong_max), m_pnameTableAlias(NULL), m_pdrgpcrOutput(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::~CLogicalBitmapTableGet
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalBitmapTableGet::~CLogicalBitmapTableGet()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	GPOS_DELETE(m_pnameTableAlias);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalBitmapTableGet::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
									   m_ptabdesc->MDId()->HashValue());
	ulHash =
		gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::Matches
//
//	@doc:
//		Match this operator with the given one.
//
//---------------------------------------------------------------------------
BOOL
CLogicalBitmapTableGet::Matches(COperator *pop) const
{
	return CUtils::FMatchBitmapScan(this, pop);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalBitmapTableGet::DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &
#ifdef GPOS_DEBUG
																 exprhdl
#endif
)
{
	GPOS_ASSERT(exprhdl.Pop() == this);

	return GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::DeriveOuterReferences
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalBitmapTableGet::DeriveOuterReferences(CMemoryPool *mp,
											  CExpressionHandle &exprhdl)
{
	return PcrsDeriveOuterIndexGet(mp, exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::DerivePropertyConstraint
//
//	@doc:
//		Derive the constraint property.
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalBitmapTableGet::DerivePropertyConstraint(
	CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	return PpcDeriveConstraintFromTableWithPredicates(mp, exprhdl, m_ptabdesc,
													  m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalBitmapTableGet::PstatsDerive(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 IStatisticsArray *stats_ctxt) const
{
	return CStatisticsUtils::DeriveStatsForBitmapTableGet(mp, exprhdl,
														  stats_ctxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::OsPrint
//
//	@doc:
//		Debug print of this operator
//
//---------------------------------------------------------------------------
IOstream &
CLogicalBitmapTableGet::OsPrint(IOstream &os) const
{
	os << SzId() << " ";
	os << ", Table Name: (";
	m_ptabdesc->Name().OsPrint(os);
	os << ")";
	os << ", Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "]";

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalBitmapTableGet::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRefArray *pdrgpcrOutput = NULL;
	if (must_exist)
	{
		pdrgpcrOutput =
			CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcrOutput, colref_mapping);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput,
											 colref_mapping, must_exist);
	}
	CName *pnameAlias = GPOS_NEW(mp) CName(mp, *m_pnameTableAlias);

	m_ptabdesc->AddRef();

	return GPOS_NEW(mp) CLogicalBitmapTableGet(mp, m_ptabdesc, m_ulOriginOpId,
											   pnameAlias, pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalBitmapTableGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet* CLogicalBitmapTableGet::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementBitmapTableGet);
	return xform_set;
}