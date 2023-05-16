//---------------------------------------------------------------------------
//	@filename:
//		CLimitStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing limit operations
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CLimitStatsProcessor.h"

using namespace gpopt;

//	compute the statistics of a limit operation
CStatistics* CLimitStatsProcessor::CalcLimitStats(CMemoryPool *mp, const CStatistics *input_stats, CDouble input_limit_rows)
{
	GPOS_ASSERT(NULL != input_stats);

	// copy the hash map from colid -> histogram for resultant structure
	UlongToHistogramMap *colid_histogram = input_stats->CopyHistograms(mp);

	CDouble limit_rows = CStatistics::MinRows;
	if (!input_stats->IsEmpty())
	{
		limit_rows = std::max(CStatistics::MinRows, input_limit_rows);
	}
	// create an output stats object
	CStatistics *pstatsLimit = GPOS_NEW(mp) CStatistics(
		mp, colid_histogram, input_stats->CopyWidths(mp), limit_rows,
		input_stats->IsEmpty(), input_stats->GetNumberOfPredicates());

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated limit cardinality.

	// modify source id to upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(
		mp, input_stats, pstatsLimit, limit_rows,
		CStatistics::EcbmMin /* card_bounding_method */);

	return pstatsLimit;
}