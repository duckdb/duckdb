//---------------------------------------------------------------------------
//	@filename:
//		CLeftOuterJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Left Outer Joins
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CLeftOuterJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpmd;

// return statistics object after performing LOJ operation with another statistics structure
CStatistics* CLeftOuterJoinStatsProcessor::CalcLOJoinStatsStatic(CMemoryPool *mp, const IStatistics *outer_side_stats, const IStatistics *inner_side_stats, CStatsPredJoinArray *join_preds_stats)
{
	GPOS_ASSERT(NULL != outer_side_stats);
	GPOS_ASSERT(NULL != inner_side_stats);
	GPOS_ASSERT(NULL != join_preds_stats);

	const CStatistics *result_stats_outer_side =
		dynamic_cast<const CStatistics *>(outer_side_stats);
	const CStatistics *result_stats_inner_side =
		dynamic_cast<const CStatistics *>(inner_side_stats);

	CStatistics *inner_join_stats = result_stats_outer_side->CalcInnerJoinStats(
		mp, inner_side_stats, join_preds_stats);
	CDouble num_rows_inner_join = inner_join_stats->Rows();
	CDouble num_rows_LASJ(1.0);

	// create a new hash map of histograms, for each column from the outer child
	// add the buckets that do not contribute to the inner join
	UlongToHistogramMap *LOJ_histograms =
		CLeftOuterJoinStatsProcessor::MakeLOJHistogram(
			mp, result_stats_outer_side, result_stats_inner_side,
			inner_join_stats, join_preds_stats, num_rows_inner_join,
			&num_rows_LASJ);

	// cardinality of LOJ is at least the cardinality of the outer child
	CDouble num_rows_LOJ =
		std::max(outer_side_stats->Rows(), num_rows_inner_join + num_rows_LASJ);

	// create an output stats object
	CStatistics *result_stats_LOJ = GPOS_NEW(mp) CStatistics(
		mp, LOJ_histograms, inner_join_stats->CopyWidths(mp), num_rows_LOJ,
		outer_side_stats->IsEmpty(), outer_side_stats->GetNumberOfPredicates());

	inner_join_stats->Release();

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated join cardinality.

	// modify source id to upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(
		mp, result_stats_outer_side, result_stats_LOJ, num_rows_LOJ,
		CStatistics::EcbmMin /* card_bounding_method */);
	CStatisticsUtils::ComputeCardUpperBounds(
		mp, result_stats_inner_side, result_stats_LOJ, num_rows_LOJ,
		CStatistics::EcbmMin /* card_bounding_method */);

	return result_stats_LOJ;
}

// create a new hash map of histograms for LOJ from the histograms
// of the outer child and the histograms of the inner join
UlongToHistogramMap *
CLeftOuterJoinStatsProcessor::MakeLOJHistogram(
	CMemoryPool *mp, const CStatistics *outer_side_stats,
	const CStatistics *inner_side_stats, CStatistics *inner_join_stats,
	CStatsPredJoinArray *join_preds_stats, CDouble num_rows_inner_join,
	CDouble *result_rows_LASJ)
{
	GPOS_ASSERT(NULL != outer_side_stats);
	GPOS_ASSERT(NULL != inner_side_stats);
	GPOS_ASSERT(NULL != join_preds_stats);
	GPOS_ASSERT(NULL != inner_join_stats);

	// build a bitset with all outer child columns contributing to the join
	CBitSet *outer_side_join_cols = GPOS_NEW(mp) CBitSet(mp);
	for (ULONG j = 0; j < join_preds_stats->Size(); j++)
	{
		CStatsPredJoin *join_stats = (*join_preds_stats)[j];
		if (join_stats->HasValidColIdOuter())
		{
			(void) outer_side_join_cols->ExchangeSet(join_stats->ColIdOuter());
		}
	}

	// for the columns in the outer child, compute the buckets that do not contribute to the inner join
	CStatistics *LASJ_stats = outer_side_stats->CalcLASJoinStats(
		mp, inner_side_stats, join_preds_stats,
		false /* DoIgnoreLASJHistComputation */
	);
	CDouble num_rows_LASJ(0.0);
	if (!LASJ_stats->IsEmpty())
	{
		num_rows_LASJ = LASJ_stats->Rows();
	}

	UlongToHistogramMap *LOJ_histograms = GPOS_NEW(mp) UlongToHistogramMap(mp);

	ULongPtrArray *outer_colids_with_stats =
		outer_side_stats->GetColIdsWithStats(mp);
	const ULONG num_outer_cols = outer_colids_with_stats->Size();

	for (ULONG i = 0; i < num_outer_cols; i++)
	{
		ULONG colid = *(*outer_colids_with_stats)[i];
		const CHistogram *inner_join_histogram =
			inner_join_stats->GetHistogram(colid);
		GPOS_ASSERT(NULL != inner_join_histogram);

		if (outer_side_join_cols->Get(colid))
		{
			// add buckets from the outer histogram that do not contribute to the inner join
			const CHistogram *LASJ_histogram = LASJ_stats->GetHistogram(colid);
			GPOS_ASSERT(NULL != LASJ_histogram);

			if (LASJ_histogram->IsWellDefined() && !LASJ_histogram->IsEmpty())
			{
				// union the buckets from the inner join and LASJ to get the LOJ buckets
				CHistogram *LOJ_histogram =
					LASJ_histogram->MakeUnionAllHistogramNormalize(
						num_rows_LASJ, inner_join_histogram,
						num_rows_inner_join);
				CStatisticsUtils::AddHistogram(mp, colid, LOJ_histogram,
											   LOJ_histograms);
				GPOS_DELETE(LOJ_histogram);
			}
			else
			{
				CStatisticsUtils::AddHistogram(mp, colid, inner_join_histogram,
											   LOJ_histograms);
			}
		}
		else
		{
			// if column from the outer side that is not a join then just add it
			CStatisticsUtils::AddHistogram(mp, colid, inner_join_histogram,
										   LOJ_histograms);
		}
	}

	LASJ_stats->Release();

	// extract all columns from the inner child of the join
	ULongPtrArray *inner_colids_with_stats =
		inner_side_stats->GetColIdsWithStats(mp);

	// add its corresponding statistics
	AddHistogramsLOJInner(mp, inner_join_stats, inner_colids_with_stats,
						  num_rows_LASJ, num_rows_inner_join, LOJ_histograms);

	*result_rows_LASJ = num_rows_LASJ;

	// clean up
	inner_colids_with_stats->Release();
	outer_colids_with_stats->Release();
	outer_side_join_cols->Release();

	return LOJ_histograms;
}


// helper function to add histograms of the inner side of a LOJ
void
CLeftOuterJoinStatsProcessor::AddHistogramsLOJInner(
	CMemoryPool *mp, const CStatistics *inner_join_stats,
	ULongPtrArray *inner_colids_with_stats, CDouble num_rows_LASJ,
	CDouble num_rows_inner_join, UlongToHistogramMap *LOJ_histograms)
{
	GPOS_ASSERT(NULL != inner_join_stats);
	GPOS_ASSERT(NULL != inner_colids_with_stats);
	GPOS_ASSERT(NULL != LOJ_histograms);

	const ULONG num_inner_cols = inner_colids_with_stats->Size();

	for (ULONG ul = 0; ul < num_inner_cols; ul++)
	{
		ULONG colid = *(*inner_colids_with_stats)[ul];

		const CHistogram *inner_join_histogram =
			inner_join_stats->GetHistogram(colid);
		GPOS_ASSERT(NULL != inner_join_histogram);

		// the number of nulls added to the inner side should be the number of rows of the LASJ on the outer side.
		CHistogram *null_histogram = GPOS_NEW(mp) CHistogram(
			mp, GPOS_NEW(mp) CBucketArray(mp), true /*is_well_defined*/,
			1.0 /*null_freq*/, CHistogram::DefaultNDVRemain,
			CHistogram::DefaultNDVFreqRemain);
		CHistogram *LOJ_histogram =
			inner_join_histogram->MakeUnionAllHistogramNormalize(
				num_rows_inner_join, null_histogram, num_rows_LASJ);
		CStatisticsUtils::AddHistogram(mp, colid, LOJ_histogram,
									   LOJ_histograms);

		GPOS_DELETE(null_histogram);
		GPOS_DELETE(LOJ_histogram);
	}
}