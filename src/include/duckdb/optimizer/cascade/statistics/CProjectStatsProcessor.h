//---------------------------------------------------------------------------
//	@filename:
//		CProjectStatsProcessor.h
//
//	@doc:
//		Compute statistics for project operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CProjectStatsProcessor_H
#define GPNAUCRATES_CProjectStatsProcessor_H

#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"

namespace gpnaucrates
{
class CProjectStatsProcessor
{
public:
	// project
	static CStatistics *CalcProjStats(CMemoryPool *mp, const CStatistics *input_stats, ULongPtrArray *projection_colids, UlongToIDatumMap *datum_map);
};
}  // namespace gpnaucrates

#endif