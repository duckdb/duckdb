//---------------------------------------------------------------------------
//	@filename:
//		CGroupByStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing group by operations
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CGroupByStatsProcessor.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpopt;

// return statistics object after Group by computation
CStatistics *
CGroupByStatsProcessor::CalcGroupByStats(CMemoryPool *mp,
										 const CStatistics *input_stats,
										 ULongPtrArray *GCs,
										 ULongPtrArray *aggs, CBitSet *keys)
{
	// create hash map from colid -> histogram for resultant structure
	UlongToHistogramMap *col_histogram_mapping =
		GPOS_NEW(mp) UlongToHistogramMap(mp);

	// hash map colid -> width
	UlongToDoubleMap *col_width_mapping = GPOS_NEW(mp) UlongToDoubleMap(mp);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	CStatistics *agg_stats = NULL;
	CDouble agg_rows = CStatistics::MinRows;
	if (input_stats->IsEmpty())
	{
		// add dummy histograms for the aggregates and grouping columns
		CHistogram::AddDummyHistogramAndWidthInfo(
			mp, col_factory, col_histogram_mapping, col_width_mapping, aggs,
			true /* is_empty */);
		CHistogram::AddDummyHistogramAndWidthInfo(
			mp, col_factory, col_histogram_mapping, col_width_mapping, GCs,
			true /* is_empty */);

		agg_stats = GPOS_NEW(mp)
			CStatistics(mp, col_histogram_mapping, col_width_mapping, agg_rows,
						true /* is_empty */);
	}
	else
	{
		// for computed aggregates, we're not going to be very smart right now
		CHistogram::AddDummyHistogramAndWidthInfo(
			mp, col_factory, col_histogram_mapping, col_width_mapping, aggs,
			false /* is_empty */);

		CColRefSet *computed_groupby_cols = GPOS_NEW(mp) CColRefSet(mp);
		CColRefSet *groupby_cols_for_stats =
			CStatisticsUtils::MakeGroupByColsForStats(mp, GCs,
													  computed_groupby_cols);

		// add statistical information of columns (1) used to compute the cardinality of the aggregate
		// and (2) the grouping columns that are computed
		CStatisticsUtils::AddGrpColStats(
			mp, input_stats, groupby_cols_for_stats, col_histogram_mapping,
			col_width_mapping);
		CStatisticsUtils::AddGrpColStats(mp, input_stats, computed_groupby_cols,
										 col_histogram_mapping,
										 col_width_mapping);

		const CStatisticsConfig *stats_config = input_stats->GetStatsConfig();

		CDoubleArray *NDVs = CStatisticsUtils::ExtractNDVForGrpCols(
			mp, stats_config, input_stats, groupby_cols_for_stats, keys);
		CDouble groups =
			CStatisticsUtils::GetCumulativeNDVs(stats_config, NDVs);

		// clean up
		groupby_cols_for_stats->Release();
		computed_groupby_cols->Release();
		NDVs->Release();

		agg_rows = std::min(std::max(CStatistics::MinRows.Get(), groups.Get()),
							input_stats->Rows().Get());

		// create a new stats object for the output
		agg_stats = GPOS_NEW(mp)
			CStatistics(mp, col_histogram_mapping, col_width_mapping, agg_rows,
						input_stats->IsEmpty());
	}

	// In the output statistics object, the upper bound source cardinality of the grouping column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated group by cardinality.

	// modify source id to upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(
		mp, input_stats, agg_stats, agg_rows,
		CStatistics::EcbmMin /* card_bounding_method */);

	return agg_stats;
}