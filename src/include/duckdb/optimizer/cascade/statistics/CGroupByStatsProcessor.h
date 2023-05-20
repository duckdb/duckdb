//---------------------------------------------------------------------------
//	@filename:
//		CGroupByStatsProcessor.h
//
//	@doc:
//		Compute statistics for group by operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CGroupByStatsProcessor_H
#define GPNAUCRATES_CGroupByStatsProcessor_H

#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"

namespace gpnaucrates
{
class CGroupByStatsProcessor
{
public:
	// group by
	static CStatistics *CalcGroupByStats(CMemoryPool *mp, const CStatistics *input_stats, ULongPtrArray *GCs, ULongPtrArray *aggs, CBitSet *keys);
};
}  // namespace gpnaucrates

#endif