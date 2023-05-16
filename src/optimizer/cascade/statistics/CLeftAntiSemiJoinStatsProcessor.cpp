//---------------------------------------------------------------------------
//	@filename:
//		CLeftAntiSemiJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Left Anti-Semi Join
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CScaleFactorUtils.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpmd;

// helper for LAS-joining histograms
void
CLeftAntiSemiJoinStatsProcessor::JoinHistogramsLASJ(
	const CHistogram *histogram1, const CHistogram *histogram2,
	CStatsPredJoin *join_stats, CDouble num_rows1,
	CDouble,					//num_rows2,
	CHistogram **result_hist1,	// output: histogram 1 after join
	CHistogram **result_hist2,	// output: histogram 2 after join
	CDouble *scale_factor,		// output: scale factor based on the join
	BOOL is_input_empty, IStatistics::EStatsJoinType,
	BOOL DoIgnoreLASJHistComputation)
{
	GPOS_ASSERT(NULL != histogram1);
	GPOS_ASSERT(NULL != histogram2);
	GPOS_ASSERT(NULL != join_stats);
	GPOS_ASSERT(NULL != result_hist1);
	GPOS_ASSERT(NULL != result_hist2);
	GPOS_ASSERT(NULL != scale_factor);

	// anti-semi join should give the full outer side.
	// use 1.0 as scale factor if anti semi join
	*scale_factor = 1.0;

	CStatsPred::EStatsCmpType stats_cmp_type = join_stats->GetCmpType();

	if (is_input_empty)
	{
		*result_hist1 = histogram1->CopyHistogram();
		*result_hist2 = NULL;

		return;
	}

	BOOL empty_histograms = histogram1->IsEmpty() || histogram2->IsEmpty();
	if (!empty_histograms &&
		CHistogram::JoinPredCmpTypeIsSupported(stats_cmp_type))
	{
		*result_hist1 = histogram1->MakeLASJHistogramNormalize(
			stats_cmp_type, num_rows1, histogram2, scale_factor,
			DoIgnoreLASJHistComputation);
		*result_hist2 = NULL;

		if ((*result_hist1)->IsEmpty())
		{
			// if the LASJ histograms is empty then all tuples of the outer join column
			// joined with those on the inner side. That is, LASJ will produce no rows
			*scale_factor = num_rows1;
		}

		return;
	}

	// for an unsupported join predicate operator or in the case of missing stats,
	// copy input histograms and use default scale factor
	*scale_factor = CDouble(CScaleFactorUtils::DefaultJoinPredScaleFactor);
	*result_hist1 = histogram1->CopyHistogram();
	*result_hist2 = NULL;
}

//	Return statistics object after performing LASJ
CStatistics *
CLeftAntiSemiJoinStatsProcessor::CalcLASJoinStatsStatic(
	CMemoryPool *mp, const IStatistics *outer_stats_input,
	const IStatistics *inner_stats_input, CStatsPredJoinArray *join_preds_stats,
	BOOL DoIgnoreLASJHistComputation)
{
	GPOS_ASSERT(NULL != inner_stats_input);
	GPOS_ASSERT(NULL != outer_stats_input);
	GPOS_ASSERT(NULL != join_preds_stats);
	const CStatistics *outer_stats =
		dynamic_cast<const CStatistics *>(outer_stats_input);

	return CJoinStatsProcessor::SetResultingJoinStats(
		mp, outer_stats->GetStatsConfig(), outer_stats_input, inner_stats_input,
		join_preds_stats, IStatistics::EsjtLeftAntiSemiJoin /* esjt */,
		DoIgnoreLASJHistComputation);
}

// Compute the null frequency for LASJ
CDouble
CLeftAntiSemiJoinStatsProcessor::NullFreqLASJ(
	CStatsPred::EStatsCmpType stats_cmp_type, const CHistogram *outer_histogram,
	const CHistogram *inner_histogram)
{
	GPOS_ASSERT(NULL != outer_histogram);
	GPOS_ASSERT(NULL != inner_histogram);

	if (CStatsPred::EstatscmptINDF != stats_cmp_type)
	{
		// for equality predicate NULLs on the outer side of the join
		// will not join with those in the inner side
		return outer_histogram->GetNullFreq();
	}

	if (CStatistics::Epsilon < inner_histogram->GetNullFreq())
	{
		// for INDF predicate NULLs on the outer side of the join
		// will join with those in the inner side if they are present
		return CDouble(0.0);
	}

	return outer_histogram->GetNullFreq();
}