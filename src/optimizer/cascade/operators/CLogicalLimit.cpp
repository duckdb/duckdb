//---------------------------------------------------------------------------
//	@filename:
//		CLogicalLimit.cpp
//
//	@doc:
//		Implementation of logical limit operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalLimit.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/metadata/CName.h"
#include "duckdb/optimizer/cascade/metadata/CTableDescriptor.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/base/IDatumInt8.h"
#include "duckdb/optimizer/cascade/md/IMDTypeInt8.h"
#include "duckdb/optimizer/cascade/statistics/CLimitStatsProcessor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::CLogicalLimit
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalLimit::CLogicalLimit(CMemoryPool *mp)
	: CLogical(mp),
	  m_pos(NULL),
	  m_fGlobal(true),
	  m_fHasCount(false),
	  m_top_limit_under_dml(false)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::CLogicalLimit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalLimit::CLogicalLimit(CMemoryPool *mp, COrderSpec *pos, BOOL fGlobal,
							 BOOL fHasCount, BOOL fTopLimitUnderDML)
	: CLogical(mp),
	  m_pos(pos),
	  m_fGlobal(fGlobal),
	  m_fHasCount(fHasCount),
	  m_top_limit_under_dml(fTopLimitUnderDML)
{
	GPOS_ASSERT(NULL != pos);
	CColRefSet *pcrsSort = m_pos->PcrsUsed(mp);
	m_pcrsLocalUsed->Include(pcrsSort);
	pcrsSort->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::~CLogicalLimit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalLimit::~CLogicalLimit()
{
	CRefCount::SafeRelease(m_pos);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalLimit::HashValue() const
{
	return gpos::CombineHashes(
		gpos::CombineHashes(COperator::HashValue(), m_pos->HashValue()),
		gpos::CombineHashes(gpos::HashValue<BOOL>(&m_fGlobal),
							gpos::HashValue<BOOL>(&m_fHasCount)));
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalLimit::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pop);

		if (popLimit->FGlobal() == m_fGlobal &&
			popLimit->FHasCount() == m_fHasCount)
		{
			// match if order specs match
			return m_pos->Matches(popLimit->m_pos);
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLimit::PopCopyWithRemappedColumns(CMemoryPool *mp,
										  UlongToColRefMap *colref_mapping,
										  BOOL must_exist)
{
	COrderSpec *pos =
		m_pos->PosCopyWithRemappedColumns(mp, colref_mapping, must_exist);

	return GPOS_NEW(mp)
		CLogicalLimit(mp, pos, m_fGlobal, m_fHasCount, m_top_limit_under_dml);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalLimit::DeriveOutputColumns(CMemoryPool *,  // mp
								   CExpressionHandle &exprhdl)
{
	return PcrsDeriveOutputPassThru(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::DeriveOuterReferences
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalLimit::DeriveOuterReferences(CMemoryPool *mp,
									 CExpressionHandle &exprhdl)
{
	CColRefSet *pcrsSort = m_pos->PcrsUsed(mp);
	CColRefSet *outer_refs =
		CLogical::DeriveOuterReferences(mp, exprhdl, pcrsSort);
	pcrsSort->Release();

	return outer_refs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalLimit::DeriveMaxCard(CMemoryPool *,	 // mp
							 CExpressionHandle &exprhdl) const
{
	// max card is a precise property, so we need an exact scalar expr to derive it
	CExpression *pexprCount = exprhdl.PexprScalarExactChild(2 /*child_index*/);

	if (NULL != pexprCount &&
		CUtils::FScalarConstInt<IMDTypeInt8>(pexprCount) &&
		!pexprCount->DeriveHasSubquery())
	{
		CScalarConst *popScalarConst =
			CScalarConst::PopConvert(pexprCount->Pop());
		IDatumInt8 *pdatumInt8 =
			dynamic_cast<IDatumInt8 *>(popScalarConst->GetDatum());

		return CMaxCard(pdatumInt8->Value());
	}

	// pass on max card of first child
	return exprhdl.DeriveMaxCard(0);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLimit::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfImplementLimit);
	(void) xform_set->ExchangeSet(CXform::ExfSplitLimit);

	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PcrsStat
//
//	@doc:
//		Compute required stat columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalLimit::PcrsStat(CMemoryPool *mp, CExpressionHandle &exprhdl,
						CColRefSet *pcrsInput, ULONG child_index) const
{
	GPOS_ASSERT(0 == child_index);

	CColRefSet *pcrsUsed = GPOS_NEW(mp) CColRefSet(mp);
	// add columns used by number of rows and offset scalar children
	pcrsUsed->Union(exprhdl.DeriveUsedColumns(1));
	pcrsUsed->Union(exprhdl.DeriveUsedColumns(2));

	CColRefSet *pcrsStat =
		PcrsReqdChildStats(mp, exprhdl, pcrsInput, pcrsUsed, child_index);
	pcrsUsed->Release();

	return pcrsStat;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalLimit::DeriveKeyCollection(CMemoryPool *,  // mp
								   CExpressionHandle &exprhdl) const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalLimit::OsPrint(IOstream &os) const
{
	os << SzId() << " " << (*m_pos) << " " << (m_fGlobal ? "global" : "local")
	   << (m_top_limit_under_dml ? " NonRemovableLimit" : "");

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PstatsDerive
//
//	@doc:
//		Derive statistics based on limit
//
//---------------------------------------------------------------------------
IStatistics* CLogicalLimit::PstatsDerive(CMemoryPool* mp, CExpressionHandle &exprhdl, IStatisticsArray* stats_ctxt) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *child_stats = exprhdl.Pstats(0);
	CMaxCard maxcard = this->DeriveMaxCard(mp, exprhdl);
	CDouble dRowsMax = CDouble(maxcard.Ull());

	if (child_stats->Rows() <= dRowsMax)
	{
		child_stats->AddRef();
		return child_stats;
	}

	return CLimitStatsProcessor::CalcLimitStats(mp, dynamic_cast<CStatistics *>(child_stats), dRowsMax);
}