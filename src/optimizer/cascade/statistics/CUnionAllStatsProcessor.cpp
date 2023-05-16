//---------------------------------------------------------------------------
//	@filename:
//		CUnionAllStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing union all operations
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CUnionAllStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpopt;

// return statistics object after union all operation with input statistics object
CStatistics* CUnionAllStatsProcessor::CreateStatsForUnionAll(CMemoryPool *mp, const CStatistics *stats_first_child, const CStatistics *stats_second_child, ULongPtrArray *output_colids, ULongPtrArray *first_child_colids, ULongPtrArray *second_child_colids)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != stats_second_child);

	// lengths must match
	GPOS_ASSERT(output_colids->Size() == first_child_colids->Size());
	GPOS_ASSERT(output_colids->Size() == second_child_colids->Size());

	// create hash map from colid -> histogram for resultant structure
	UlongToHistogramMap *histograms_new = GPOS_NEW(mp) UlongToHistogramMap(mp);

	// column ids on which widths are to be computed
	UlongToDoubleMap *column_to_width_map = GPOS_NEW(mp) UlongToDoubleMap(mp);

	BOOL is_empty_unionall =
		stats_first_child->IsEmpty() && stats_second_child->IsEmpty();
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CDouble unionall_rows = CStatistics::MinRows;
	if (is_empty_unionall)
	{
		CHistogram::AddDummyHistogramAndWidthInfo(
			mp, col_factory, histograms_new, column_to_width_map, output_colids,
			true /*is_empty*/);
	}
	else
	{
		const ULONG len = output_colids->Size();
		for (ULONG ul = 0; ul < len; ul++)
		{
			ULONG output_colid = *(*output_colids)[ul];
			ULONG first_child_colid = *(*first_child_colids)[ul];
			ULONG second_child_colid = *(*second_child_colids)[ul];

			const CHistogram *first_child_histogram =
				stats_first_child->GetHistogram(first_child_colid);
			GPOS_ASSERT(NULL != first_child_histogram);
			const CHistogram *second_child_histogram =
				stats_second_child->GetHistogram(second_child_colid);
			GPOS_ASSERT(NULL != second_child_histogram);

			if (first_child_histogram->IsWellDefined() ||
				second_child_histogram->IsWellDefined())
			{
				CHistogram *output_histogram =
					first_child_histogram->MakeUnionAllHistogramNormalize(
						stats_first_child->Rows(), second_child_histogram,
						stats_second_child->Rows());
				CStatisticsUtils::AddHistogram(
					mp, output_colid, output_histogram, histograms_new);
				GPOS_DELETE(output_histogram);
			}
			else
			{
				CColRef *column_ref = col_factory->LookupColRef(output_colid);
				GPOS_ASSERT(NULL != column_ref);

				CHistogram *dummy_histogram = CHistogram::MakeDefaultHistogram(
					mp, column_ref, false /* is_empty*/);
				histograms_new->Insert(GPOS_NEW(mp) ULONG(output_colid),
									   dummy_histogram);
			}

			// look up width
			const CDouble *col_width =
				stats_first_child->GetWidth(first_child_colid);
			GPOS_ASSERT(NULL != col_width);
			column_to_width_map->Insert(GPOS_NEW(mp) ULONG(output_colid),
										GPOS_NEW(mp) CDouble(*col_width));
		}

		unionall_rows = stats_first_child->Rows() + stats_second_child->Rows();
	}

	// release inputs
	output_colids->Release();
	first_child_colids->Release();
	second_child_colids->Release();

	// create an output stats object
	CStatistics *unionall_stats = GPOS_NEW(mp)
		CStatistics(mp, histograms_new, column_to_width_map, unionall_rows,
					is_empty_unionall, 0 /* m_num_predicates */
		);

	// In the output statistics object, the upper bound source cardinality of the UNION ALL column
	// is the estimate union all cardinality.

	// modify upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(
		mp, stats_first_child, unionall_stats, unionall_rows,
		CStatistics::EcbmOutputCard /* card_bounding_method */);

	return unionall_stats;
}