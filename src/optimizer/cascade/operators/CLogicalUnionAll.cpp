//---------------------------------------------------------------------------
//	@filename:
//		CLogicalUnionAll.cpp
//
//	@doc:
//		Implementation of UnionAll operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalUnionAll.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/statistics/CUnionAllStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::CLogicalUnionAll
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalUnionAll::CLogicalUnionAll(CMemoryPool *mp)
	: CLogicalUnion(mp), m_ulScanIdPartialIndex(0)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::CLogicalUnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalUnionAll::CLogicalUnionAll(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
								   CColRef2dArray *pdrgpdrgpcrInput,
								   ULONG ulScanIdPartialIndex)
	: CLogicalUnion(mp, pdrgpcrOutput, pdrgpdrgpcrInput),
	  m_ulScanIdPartialIndex(ulScanIdPartialIndex)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::~CLogicalUnionAll
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalUnionAll::~CLogicalUnionAll()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalUnionAll::DeriveMaxCard(CMemoryPool *,	// mp
								CExpressionHandle &exprhdl) const
{
	const ULONG arity = exprhdl.Arity();

	CMaxCard maxcard = exprhdl.DeriveMaxCard(0);
	for (ULONG ul = 1; ul < arity; ul++)
	{
		maxcard += exprhdl.DeriveMaxCard(ul);
	}

	return maxcard;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalUnionAll::PopCopyWithRemappedColumns(CMemoryPool *mp,
											 UlongToColRefMap *colref_mapping,
											 BOOL must_exist)
{
	CColRefArray *pdrgpcrOutput =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput, colref_mapping, must_exist);
	CColRef2dArray *pdrgpdrgpcrInput = CUtils::PdrgpdrgpcrRemap(
		mp, m_pdrgpdrgpcrInput, colref_mapping, must_exist);

	return GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgpdrgpcrInput,
										 m_ulScanIdPartialIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalUnionAll::DeriveKeyCollection(CMemoryPool *,	   //mp,
									  CExpressionHandle &  // exprhdl
) const
{
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalUnionAll::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementUnionAll);

	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PstatsDeriveUnionAll
//
//	@doc:
//		Derive statistics based on union all semantics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalUnionAll::PstatsDeriveUnionAll(CMemoryPool *mp,
									   CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(COperator::EopLogicalUnionAll == exprhdl.Pop()->Eopid() ||
				COperator::EopLogicalUnion == exprhdl.Pop()->Eopid());

	CColRefArray *pdrgpcrOutput =
		CLogicalSetOp::PopConvert(exprhdl.Pop())->PdrgpcrOutput();
	CColRef2dArray *pdrgpdrgpcrInput =
		CLogicalSetOp::PopConvert(exprhdl.Pop())->PdrgpdrgpcrInput();
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpdrgpcrInput);

	IStatistics *result_stats = exprhdl.Pstats(0);
	result_stats->AddRef();
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 1; ul < arity; ul++)
	{
		IStatistics *child_stats = exprhdl.Pstats(ul);
		CStatistics *stats = CUnionAllStatsProcessor::CreateStatsForUnionAll(
			mp, dynamic_cast<CStatistics *>(result_stats),
			dynamic_cast<CStatistics *>(child_stats),
			CColRef::Pdrgpul(mp, pdrgpcrOutput),
			CColRef::Pdrgpul(mp, (*pdrgpdrgpcrInput)[0]),
			CColRef::Pdrgpul(mp, (*pdrgpdrgpcrInput)[ul]));
		result_stats->Release();
		result_stats = stats;
	}

	return result_stats;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PstatsDerive
//
//	@doc:
//		Derive statistics based on union all semantics
//
//---------------------------------------------------------------------------
IStatistics* CLogicalUnionAll::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl, IStatisticsArray* stats_ctxt) const
{
	GPOS_ASSERT(EspNone < Esp(exprhdl));

	return PstatsDeriveUnionAll(mp, exprhdl);
}