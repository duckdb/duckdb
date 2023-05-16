//---------------------------------------------------------------------------
//	@filename:
//		CLogicalUnary.cpp
//
//	@doc:
//		Implementation of logical unary operators
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalUnary.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/xforms/CXformUtils.h"
#include "duckdb/optimizer/cascade/statistics/CProjectStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL CLogicalUnary::Matches(COperator *pop) const
{
	return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::Esp
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CLogical::EStatPromise
CLogicalUnary::Esp(CExpressionHandle &exprhdl) const
{
	// low promise for stat derivation if scalar predicate has subqueries, or logical
	// expression has outer-refs or is part of an Apply expression
	if (exprhdl.DeriveHasSubquery(1) || exprhdl.HasOuterRefs() || (NULL != exprhdl.Pgexpr() && CXformUtils::FGenerateApply(exprhdl.Pgexpr()->ExfidOrigin())))
	{
		return EspLow;
	}

	return EspHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::PstatsDeriveProject
//
//	@doc:
//		Derive statistics for projection operators
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalUnary::PstatsDeriveProject(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   UlongToIDatumMap *phmuldatum) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *child_stats = exprhdl.Pstats(0);
	CReqdPropRelational *prprel =
		CReqdPropRelational::GetReqdRelationalProps(exprhdl.Prp());
	CColRefSet *pcrs = prprel->PcrsStat();
	ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);
	pcrs->ExtractColIds(mp, colids);

	IStatistics *stats = CProjectStatsProcessor::CalcProjStats(
		mp, dynamic_cast<CStatistics *>(child_stats), colids, phmuldatum);

	// clean up
	colids->Release();

	return stats;
}