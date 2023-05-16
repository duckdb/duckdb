//---------------------------------------------------------------------------
//	@filename:
//		CLeftSemiJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Left Semi Joins
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CLeftSemiJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/statistics/CGroupByStatsProcessor.h"

using namespace gpopt;

// return statistics object after performing LSJ operation
CStatistics *
CLeftSemiJoinStatsProcessor::CalcLSJoinStatsStatic(
	CMemoryPool *mp, const IStatistics *outer_stats_input,
	const IStatistics *inner_stats_input, CStatsPredJoinArray *join_preds_stats)
{
	GPOS_ASSERT(NULL != outer_stats_input);
	GPOS_ASSERT(NULL != inner_stats_input);
	GPOS_ASSERT(NULL != join_preds_stats);

	const ULONG length = join_preds_stats->Size();

	// iterate over all inner columns and perform a group by to remove duplicates
	ULongPtrArray *inner_colids = GPOS_NEW(mp) ULongPtrArray(mp);
	for (ULONG ul = 0; ul < length; ul++)
	{
		if ((*join_preds_stats)[ul]->HasValidColIdInner())
		{
			ULONG colid = ((*join_preds_stats)[ul])->ColIdInner();
			inner_colids->Append(GPOS_NEW(mp) ULONG(colid));
		}
	}

	// dummy agg columns required for group by derivation
	ULongPtrArray *aggs = GPOS_NEW(mp) ULongPtrArray(mp);
	IStatistics *inner_stats = CGroupByStatsProcessor::CalcGroupByStats(
		mp, dynamic_cast<const CStatistics *>(inner_stats_input), inner_colids,
		aggs,
		NULL  // keys: no keys, use all grouping cols
	);

	const CStatistics *outer_stats =
		dynamic_cast<const CStatistics *>(outer_stats_input);
	CStatistics *semi_join_stats = CJoinStatsProcessor::SetResultingJoinStats(
		mp, outer_stats->GetStatsConfig(), outer_stats, inner_stats,
		join_preds_stats, IStatistics::EsjtLeftSemiJoin /* esjt */,
		true /* DoIgnoreLASJHistComputation */
	);

	// clean up
	inner_colids->Release();
	aggs->Release();
	inner_stats->Release();

	return semi_join_stats;
}