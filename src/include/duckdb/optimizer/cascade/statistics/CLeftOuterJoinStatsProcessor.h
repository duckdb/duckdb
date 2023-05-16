//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CLeftOuterJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Left Outer Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftOuterJoinStatsProcessor_H
#define GPNAUCRATES_CLeftOuterJoinStatsProcessor_H

#include "duckdb/optimizer/cascade/statistics/CJoinStatsProcessor.h"

namespace gpnaucrates
{
class CLeftOuterJoinStatsProcessor : public CJoinStatsProcessor
{
private:
	// create a new hash map of histograms from the results of the inner join and the histograms of the outer child
	static UlongToHistogramMap *MakeLOJHistogram(
		CMemoryPool *mp, const CStatistics *outer_stats,
		const CStatistics *inner_side_stats, CStatistics *inner_join_stats,
		CStatsPredJoinArray *join_preds_stats, CDouble num_rows_inner_join,
		CDouble *result_rows_LASJ);
	// helper method to add histograms of the inner side of a LOJ
	static void AddHistogramsLOJInner(CMemoryPool *mp,
									  const CStatistics *inner_join_stats,
									  ULongPtrArray *inner_colids_with_stats,
									  CDouble num_rows_LASJ,
									  CDouble num_rows_inner_join,
									  UlongToHistogramMap *LOJ_histograms);

public:
	static CStatistics *CalcLOJoinStatsStatic(
		CMemoryPool *mp, const IStatistics *outer_stats,
		const IStatistics *inner_side_stats,
		CStatsPredJoinArray *join_preds_stats);
};
}  // namespace gpnaucrates

#endif