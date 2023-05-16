//---------------------------------------------------------------------------
//	@filename:
//		CLogicalUnion.cpp
//
//	@doc:
//		Implementation of union operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalUnion.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogicalGbAgg.h"
#include "duckdb/optimizer/cascade/operators/CLogicalUnionAll.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::CLogicalUnion
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalUnion::CLogicalUnion(CMemoryPool *mp) : CLogicalSetOp(mp)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::CLogicalUnion
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalUnion::CLogicalUnion(CMemoryPool *mp, CColRefArray *pdrgpcrOutput,
							 CColRef2dArray *pdrgpdrgpcrInput)
	: CLogicalSetOp(mp, pdrgpcrOutput, pdrgpdrgpcrInput)
{
#ifdef GPOS_DEBUG
	CColRefArray *pdrgpcrInput = (*pdrgpdrgpcrInput)[0];
	const ULONG num_cols = pdrgpcrOutput->Size();
	GPOS_ASSERT(num_cols == pdrgpcrInput->Size());

	// Ensure that the output columns are the same as first input
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		GPOS_ASSERT((*pdrgpcrOutput)[ul] == (*pdrgpcrInput)[ul]);
	}

#endif	// GPOS_DEBUG
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::~CLogicalUnion
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalUnion::~CLogicalUnion()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalUnion::PopCopyWithRemappedColumns(CMemoryPool *mp,
										  UlongToColRefMap *colref_mapping,
										  BOOL must_exist)
{
	CColRefArray *pdrgpcrOutput =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput, colref_mapping, must_exist);
	CColRef2dArray *pdrgpdrgpcrInput = CUtils::PdrgpdrgpcrRemap(
		mp, m_pdrgpdrgpcrInput, colref_mapping, must_exist);

	return GPOS_NEW(mp) CLogicalUnion(mp, pdrgpcrOutput, pdrgpdrgpcrInput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalUnion::DeriveMaxCard(CMemoryPool *,	 // mp
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
//		CLogicalUnion::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalUnion::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfUnion2UnionAll);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalUnion::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							IStatisticsArray *	// not used
) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);

	// union is transformed into a group by over an union all
	// we follow the same route to compute statistics
	IStatistics *pstatsUnionAll =
		CLogicalUnionAll::PstatsDeriveUnionAll(mp, exprhdl);

	// computed columns
	ULongPtrArray *pdrgpulComputedCols = GPOS_NEW(mp) ULongPtrArray(mp);

	IStatistics *stats = CLogicalGbAgg::PstatsDerive(
		mp, pstatsUnionAll,
		m_pdrgpcrOutput,	  // we group by the output columns
		pdrgpulComputedCols,  // no computed columns for set ops
		NULL				  // no keys, use all grouping cols
	);

	// clean up
	pdrgpulComputedCols->Release();
	pstatsUnionAll->Release();

	return stats;
}