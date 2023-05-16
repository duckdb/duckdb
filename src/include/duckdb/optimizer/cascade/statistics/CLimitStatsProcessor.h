//---------------------------------------------------------------------------
//	Greenplum Database
//	@filename:
//		CLimitStatsProcessor.h
//
//	@doc:
//		Compute statistics for limit operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLimitStatsProcessor_H
#define GPNAUCRATES_CLimitStatsProcessor_H

#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

namespace gpnaucrates
{
using namespace gpos;

class CLimitStatsProcessor
{
public:
	// limit
	static CStatistics *CalcLimitStats(CMemoryPool *mp, const CStatistics *input_stats, CDouble input_limit_rows);
};
}  // namespace gpnaucrates

#endif