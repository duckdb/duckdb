//---------------------------------------------------------------------------
//	@filename:
//		CProjectStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing project operations
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CProjectStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpopt;

//  return a statistics object for a project operation
CStatistics* CProjectStatsProcessor::CalcProjStats(CMemoryPool *mp, const CStatistics *input_stats, ULongPtrArray *projection_colids, UlongToIDatumMap *datum_map)
{
	GPOS_ASSERT(NULL != projection_colids);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	// create hash map from colid -> histogram for resultant structure
	UlongToHistogramMap *histograms_new = GPOS_NEW(mp) UlongToHistogramMap(mp);

	// column ids on which widths are to be computed
	UlongToDoubleMap *colid_width_mapping = GPOS_NEW(mp) UlongToDoubleMap(mp);

	const ULONG length = projection_colids->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		ULONG colid = *(*projection_colids)[ul];
		const CHistogram *histogram = input_stats->GetHistogram(colid);

		if (NULL == histogram)
		{
			// create histogram for the new project column
			CBucketArray *proj_col_bucket = GPOS_NEW(mp) CBucketArray(mp);
			CDouble null_freq = 0.0;

			BOOL is_well_defined = false;
			if (NULL != datum_map)
			{
				IDatum *datum = datum_map->Find(&colid);
				if (NULL != datum)
				{
					is_well_defined = true;
					if (!datum->IsNull())
					{
						proj_col_bucket->Append(
							CBucket::MakeBucketSingleton(mp, datum));
					}
					else
					{
						null_freq = 1.0;
					}
				}
			}

			CHistogram *proj_col_histogram = NULL;
			CColRef *colref = col_factory->LookupColRef(colid);
			GPOS_ASSERT(NULL != colref);

			if (0 == proj_col_bucket->Size() &&
				IMDType::EtiBool == colref->RetrieveType()->GetDatumType())
			{
				proj_col_bucket->Release();
				proj_col_histogram = CHistogram::MakeDefaultBoolHistogram(mp);
			}
			else
			{
				proj_col_histogram = GPOS_NEW(mp)
					CHistogram(mp, proj_col_bucket, is_well_defined, null_freq,
							   CHistogram::DefaultNDVRemain,
							   CHistogram::DefaultNDVFreqRemain);
			}

			histograms_new->Insert(GPOS_NEW(mp) ULONG(colid),
								   proj_col_histogram);
		}
		else
		{
			histograms_new->Insert(GPOS_NEW(mp) ULONG(colid),
								   histogram->CopyHistogram());
		}

		// look up width
		const CDouble *width = input_stats->GetWidth(colid);
		if (NULL == width)
		{
			CColRef *colref = col_factory->LookupColRef(colid);
			GPOS_ASSERT(NULL != colref);

			CDouble width =
				CStatisticsUtils::DefaultColumnWidth(colref->RetrieveType());
			colid_width_mapping->Insert(GPOS_NEW(mp) ULONG(colid),
										GPOS_NEW(mp) CDouble(width));
		}
		else
		{
			colid_width_mapping->Insert(GPOS_NEW(mp) ULONG(colid),
										GPOS_NEW(mp) CDouble(*width));
		}
	}

	CDouble input_rows = input_stats->Rows();
	// create an output stats object
	CStatistics *projection_stats = GPOS_NEW(mp) CStatistics(
		mp, histograms_new, colid_width_mapping, input_rows,
		input_stats->IsEmpty(), input_stats->GetNumberOfPredicates());

	// In the output statistics object, the upper bound source cardinality of the project column
	// is equivalent the estimate project cardinality.
	CStatisticsUtils::ComputeCardUpperBounds(
		mp, input_stats, projection_stats, input_rows,
		CStatistics::EcbmInputSourceMaxCard /* card_bounding_method */);

	// add upper bound card information for the project columns
	CStatistics::CreateAndInsertUpperBoundNDVs(mp, projection_stats,
											   projection_colids, input_rows);

	return projection_stats;
}