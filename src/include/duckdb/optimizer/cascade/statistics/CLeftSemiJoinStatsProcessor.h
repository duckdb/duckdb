//---------------------------------------------------------------------------
//	@filename:
//		CLeftSemiJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Left Semi Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftSemiJoinStatsProcessor_H
#define GPNAUCRATES_CLeftSemiJoinStatsProcessor_H

#include "duckdb/optimizer/cascade/statistics/CJoinStatsProcessor.h"

namespace gpnaucrates
{
class CLeftSemiJoinStatsProcessor : public CJoinStatsProcessor
{
public:
	static CStatistics *CalcLSJoinStatsStatic(
		CMemoryPool *mp, const IStatistics *outer_stats,
		const IStatistics *inner_side_stats,
		CStatsPredJoinArray *join_preds_stats);
};
}  // namespace gpnaucrates

#endif