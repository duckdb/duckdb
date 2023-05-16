//---------------------------------------------------------------------------
//	@filename:
//		CStatistics.cpp
//
//	@doc:
//		Histograms based statistics
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"
#include "duckdb/optimizer/cascade/common/CBitSet.h"
#include "duckdb/optimizer/cascade/memory/CAutoMemoryPool.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CColumnFactory.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/engine/CStatisticsConfig.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/statistics/CInnerJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CLeftOuterJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CLeftSemiJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CScaleFactorUtils.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;

// default number of rows in relation
const CDouble CStatistics::DefaultRelationRows(1000.0);

// epsilon to be used for various computations
const CDouble CStatistics::Epsilon(0.00001);

// minimum number of rows in relation
const CDouble CStatistics::MinRows(1.0);

// default column width
const CDouble CStatistics::DefaultColumnWidth(8.0);

// default number of distinct values
const CDouble CStatistics::DefaultDistinctValues(1000.0);

// the default value for operators that have no cardinality estimation risk
const ULONG CStatistics::no_card_est_risk_default_val = 1;

// ctor
CStatistics::CStatistics(CMemoryPool *mp,
						 UlongToHistogramMap *col_histogram_mapping,
						 UlongToDoubleMap *colid_width_mapping, CDouble rows,
						 BOOL is_empty, ULONG num_predicates)
	: m_colid_histogram_mapping(col_histogram_mapping),
	  m_colid_width_mapping(colid_width_mapping),
	  m_rows(rows),
	  m_stats_estimation_risk(no_card_est_risk_default_val),
	  m_empty(is_empty),
	  m_num_rebinds(
		  1.0),	 // by default, a stats object is rebound to parameters only once
	  m_num_predicates(num_predicates),
	  m_src_upper_bound_NDVs(NULL)
{
	GPOS_ASSERT(NULL != m_colid_histogram_mapping);
	GPOS_ASSERT(NULL != m_colid_width_mapping);
	GPOS_ASSERT(CDouble(0.0) <= m_rows);

	// hash map for source id -> max source cardinality mapping
	m_src_upper_bound_NDVs = GPOS_NEW(mp) CUpperBoundNDVPtrArray(mp);

	m_stats_conf =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig()->GetStatsConf();
}

// Dtor
CStatistics::~CStatistics()
{
	m_colid_histogram_mapping->Release();
	m_colid_width_mapping->Release();
	m_src_upper_bound_NDVs->Release();
}

// look up the width of a particular column
const CDouble *
CStatistics::GetWidth(ULONG colid) const
{
	return m_colid_width_mapping->Find(&colid);
}


//	cap the total number of distinct values (NDVs) in buckets to the number of rows
void
CStatistics::CapNDVs(CDouble rows, UlongToHistogramMap *col_histogram_mapping)
{
	GPOS_ASSERT(NULL != col_histogram_mapping);
	UlongToHistogramMapIter col_hist_mapping(col_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		CHistogram *histogram =
			const_cast<CHistogram *>(col_hist_mapping.Value());
		histogram->CapNDVs(rows);
	}
}

// helper print function
IOstream &
CStatistics::OsPrint(IOstream &os) const
{
	os << "{" << std::endl;
	os << "Rows = " << Rows() << std::endl;
	os << "Rebinds = " << NumRebinds() << std::endl;

	UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG colid = *(col_hist_mapping.Key());
		os << "Col" << colid << ":" << std::endl;
		const CHistogram *histogram = col_hist_mapping.Value();
		histogram->OsPrint(os);
		os << std::endl;
	}

	UlongToDoubleMapIter col_width_map_iterator(m_colid_width_mapping);
	while (col_width_map_iterator.Advance())
	{
		ULONG colid = *(col_width_map_iterator.Key());
		os << "Col" << colid << ":" << std::endl;
		const CDouble *width = col_width_map_iterator.Value();
		os << " width " << (*width) << std::endl;
	}

	const ULONG length = m_src_upper_bound_NDVs->Size();
	for (ULONG i = 0; i < length; i++)
	{
		const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
		upper_bound_NDVs->OsPrint(os);
	}
	os << "StatsEstimationRisk = " << StatsEstimationRisk() << std::endl;
	os << "}" << std::endl;

	return os;
}


//	return the total number of rows for this statistics object
CDouble
CStatistics::Rows() const
{
	return m_rows;
}

// return the estimated skew of the given column
CDouble
CStatistics::GetSkew(ULONG colid) const
{
	CHistogram *histogram = m_colid_histogram_mapping->Find(&colid);
	if (NULL == histogram)
	{
		return CDouble(1.0);
	}

	return histogram->GetSkew();
}

// return total width in bytes
CDouble
CStatistics::Width() const
{
	CDouble total_width(0.0);
	UlongToDoubleMapIter col_width_map_iterator(m_colid_width_mapping);
	while (col_width_map_iterator.Advance())
	{
		const CDouble *width = col_width_map_iterator.Value();
		total_width = total_width + (*width);
	}
	return total_width.Ceil();
}

// return the width in bytes of a set of columns
CDouble
CStatistics::Width(ULongPtrArray *colids) const
{
	GPOS_ASSERT(NULL != colids);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CDouble total_width(0.0);
	const ULONG size = colids->Size();
	for (ULONG idx = 0; idx < size; idx++)
	{
		ULONG colid = *((*colids)[idx]);
		CDouble *width = m_colid_width_mapping->Find(&colid);
		if (NULL != width)
		{
			total_width = total_width + (*width);
		}
		else
		{
			CColRef *colref = col_factory->LookupColRef(colid);
			GPOS_ASSERT(NULL != colref);

			total_width = total_width + CStatisticsUtils::DefaultColumnWidth(
											colref->RetrieveType());
		}
	}
	return total_width.Ceil();
}

// return width in bytes of a set of columns
CDouble
CStatistics::Width(CMemoryPool *mp, CColRefSet *colrefs) const
{
	GPOS_ASSERT(NULL != colrefs);

	ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);
	colrefs->ExtractColIds(mp, colids);

	CDouble width = Width(colids);
	colids->Release();

	return width;
}

// return dummy statistics object
CStatistics *
CStatistics::MakeDummyStats(CMemoryPool *mp, ULongPtrArray *colids,
							CDouble rows)
{
	GPOS_ASSERT(NULL != colids);

	// hash map from colid -> histogram for resultant structure
	UlongToHistogramMap *col_histogram_mapping =
		GPOS_NEW(mp) UlongToHistogramMap(mp);

	// hashmap from colid -> width (double)
	UlongToDoubleMap *colid_width_mapping = GPOS_NEW(mp) UlongToDoubleMap(mp);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	BOOL is_empty = (CStatistics::Epsilon >= rows);
	CHistogram::AddDummyHistogramAndWidthInfo(
		mp, col_factory, col_histogram_mapping, colid_width_mapping, colids,
		is_empty);

	CStatistics *stats = GPOS_NEW(mp) CStatistics(
		mp, col_histogram_mapping, colid_width_mapping, rows, is_empty);
	CreateAndInsertUpperBoundNDVs(mp, stats, colids, rows);

	return stats;
}

// add upper bound ndvs information for a given set of columns
void
CStatistics::CreateAndInsertUpperBoundNDVs(CMemoryPool *mp, CStatistics *stats,
										   ULongPtrArray *colids, CDouble rows)
{
	GPOS_ASSERT(NULL != stats);
	GPOS_ASSERT(NULL != colids);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CColRefSet *colrefs = GPOS_NEW(mp) CColRefSet(mp);
	const ULONG num_cols = colids->Size();
	for (ULONG i = 0; i < num_cols; i++)
	{
		ULONG colid = *(*colids)[i];
		const CColRef *colref = col_factory->LookupColRef(colid);
		if (NULL != colref)
		{
			colrefs->Include(colref);
		}
	}

	if (0 < colrefs->Size())
	{
		stats->AddCardUpperBound(GPOS_NEW(mp) CUpperBoundNDVs(colrefs, rows));
	}
	else
	{
		colrefs->Release();
	}
}

//	return dummy statistics object
CStatistics *
CStatistics::MakeDummyStats(CMemoryPool *mp,
							ULongPtrArray *col_histogram_mapping,
							ULongPtrArray *col_width_mapping, CDouble rows)
{
	GPOS_ASSERT(NULL != col_histogram_mapping);
	GPOS_ASSERT(NULL != col_width_mapping);

	BOOL is_empty = (CStatistics::Epsilon >= rows);
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	// hash map from colid -> histogram for resultant structure
	UlongToHistogramMap *result_col_histogram_mapping =
		GPOS_NEW(mp) UlongToHistogramMap(mp);

	const ULONG num_col_hist = col_histogram_mapping->Size();
	for (ULONG ul = 0; ul < num_col_hist; ul++)
	{
		ULONG colid = *(*col_histogram_mapping)[ul];

		CColRef *colref = col_factory->LookupColRef(colid);
		GPOS_ASSERT(NULL != colref);

		// empty histogram
		CHistogram *histogram =
			CHistogram::MakeDefaultHistogram(mp, colref, is_empty);
		result_col_histogram_mapping->Insert(GPOS_NEW(mp) ULONG(colid),
											 histogram);
	}

	// hashmap from colid -> width (double)
	UlongToDoubleMap *colid_width_mapping = GPOS_NEW(mp) UlongToDoubleMap(mp);

	const ULONG num_col_width = col_width_mapping->Size();
	for (ULONG ul = 0; ul < num_col_width; ul++)
	{
		ULONG colid = *(*col_width_mapping)[ul];

		CColRef *colref = col_factory->LookupColRef(colid);
		GPOS_ASSERT(NULL != colref);

		CDouble width =
			CStatisticsUtils::DefaultColumnWidth(colref->RetrieveType());
		colid_width_mapping->Insert(GPOS_NEW(mp) ULONG(colid),
									GPOS_NEW(mp) CDouble(width));
	}

	CStatistics *stats = GPOS_NEW(mp)
		CStatistics(mp, result_col_histogram_mapping, colid_width_mapping, rows,
					false /* is_empty */);
	CreateAndInsertUpperBoundNDVs(mp, stats, col_histogram_mapping, rows);

	return stats;
}


//	check if the input statistics from join statistics computation empty
BOOL
CStatistics::IsEmptyJoin(const CStatistics *outer_stats,
						 const CStatistics *inner_side_stats, BOOL IsLASJ)
{
	GPOS_ASSERT(NULL != outer_stats);
	GPOS_ASSERT(NULL != inner_side_stats);

	if (IsLASJ)
	{
		return outer_stats->IsEmpty();
	}

	return outer_stats->IsEmpty() || inner_side_stats->IsEmpty();
}

// Currently, Pstats[Join type] are thin wrappers the C[Join type]StatsProcessor class's method
// for deriving the stat objects for the corresponding join operator

//	return statistics object after performing LOJ operation with another statistics structure
CStatistics *
CStatistics::CalcLOJoinStats(CMemoryPool *mp, const IStatistics *other_stats,
							 CStatsPredJoinArray *join_preds_stats) const
{
	return CLeftOuterJoinStatsProcessor::CalcLOJoinStatsStatic(
		mp, this, other_stats, join_preds_stats);
}



//	return statistics object after performing semi-join with another statistics structure
CStatistics *
CStatistics::CalcLSJoinStats(CMemoryPool *mp,
							 const IStatistics *inner_side_stats,
							 CStatsPredJoinArray *join_preds_stats) const
{
	return CLeftSemiJoinStatsProcessor::CalcLSJoinStatsStatic(
		mp, this, inner_side_stats, join_preds_stats);
}



// return statistics object after performing inner join
CStatistics *
CStatistics::CalcInnerJoinStats(CMemoryPool *mp, const IStatistics *other_stats,
								CStatsPredJoinArray *join_preds_stats) const
{
	return CInnerJoinStatsProcessor::CalcInnerJoinStatsStatic(
		mp, this, other_stats, join_preds_stats);
}

// return statistics object after performing LASJ
CStatistics *
CStatistics::CalcLASJoinStats(CMemoryPool *mp, const IStatistics *other_stats,
							  CStatsPredJoinArray *join_preds_stats,
							  BOOL DoIgnoreLASJHistComputation) const
{
	return CLeftAntiSemiJoinStatsProcessor::CalcLASJoinStatsStatic(
		mp, this, other_stats, join_preds_stats, DoIgnoreLASJHistComputation);
}

//	helper method to copy statistics on columns that are not excluded by bitset
void
CStatistics::AddNotExcludedHistograms(
	CMemoryPool *mp, CBitSet *excluded_cols,
	UlongToHistogramMap *col_histogram_mapping) const
{
	GPOS_ASSERT(NULL != excluded_cols);
	GPOS_ASSERT(NULL != col_histogram_mapping);

	UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG colid = *(col_hist_mapping.Key());
		if (!excluded_cols->Get(colid))
		{
			const CHistogram *histogram = col_hist_mapping.Value();
			CStatisticsUtils::AddHistogram(mp, colid, histogram,
										   col_histogram_mapping);
		}

		GPOS_CHECK_ABORT;
	}
}

UlongToDoubleMap *
CStatistics::CopyWidths(CMemoryPool *mp) const
{
	UlongToDoubleMap *widths_copy = GPOS_NEW(mp) UlongToDoubleMap(mp);
	CStatisticsUtils::AddWidthInfo(mp, m_colid_width_mapping, widths_copy);

	return widths_copy;
}

void
CStatistics::CopyWidthsInto(CMemoryPool *mp,
							UlongToDoubleMap *colid_width_mapping) const
{
	CStatisticsUtils::AddWidthInfo(mp, m_colid_width_mapping,
								   colid_width_mapping);
}

UlongToHistogramMap *
CStatistics::CopyHistograms(CMemoryPool *mp) const
{
	// create hash map from colid -> histogram for resultant structure
	UlongToHistogramMap *histograms_copy = GPOS_NEW(mp) UlongToHistogramMap(mp);

	BOOL is_empty = IsEmpty();

	UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG colid = *(col_hist_mapping.Key());
		const CHistogram *histogram = col_hist_mapping.Value();
		CHistogram *histogram_copy = NULL;
		if (is_empty)
		{
			histogram_copy =
				GPOS_NEW(mp) CHistogram(mp, false /* is_well_defined */);
		}
		else
		{
			histogram_copy = histogram->CopyHistogram();
		}

		histograms_copy->Insert(GPOS_NEW(mp) ULONG(colid), histogram_copy);
	}

	return histograms_copy;
}



//	return required props associated with statistics object
CReqdPropRelational *
CStatistics::GetReqdRelationalProps(CMemoryPool *mp) const
{
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	GPOS_ASSERT(NULL != col_factory);

	CColRefSet *colrefs = GPOS_NEW(mp) CColRefSet(mp);

	// add columns from histogram map
	UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG colid = *(col_hist_mapping.Key());
		CColRef *colref = col_factory->LookupColRef(colid);
		GPOS_ASSERT(NULL != colref);

		colrefs->Include(colref);
	}

	return GPOS_NEW(mp) CReqdPropRelational(colrefs);
}

// append given statistics to current object
void
CStatistics::AppendStats(CMemoryPool *mp, IStatistics *input_stats)
{
	CStatistics *stats = CStatistics::CastStats(input_stats);

	CHistogram::AddHistograms(mp, stats->m_colid_histogram_mapping,
							  m_colid_histogram_mapping);
	GPOS_CHECK_ABORT;

	CStatisticsUtils::AddWidthInfo(mp, stats->m_colid_width_mapping,
								   m_colid_width_mapping);
	GPOS_CHECK_ABORT;
}

// copy statistics object
IStatistics *
CStatistics::CopyStats(CMemoryPool *mp) const
{
	return ScaleStats(mp, CDouble(1.0) /*factor*/);
}

// return a copy of this statistics object scaled by a given factor
IStatistics *
CStatistics::ScaleStats(CMemoryPool *mp, CDouble factor) const
{
	UlongToHistogramMap *histograms_new = GPOS_NEW(mp) UlongToHistogramMap(mp);
	UlongToDoubleMap *widths_new = GPOS_NEW(mp) UlongToDoubleMap(mp);

	CHistogram::AddHistograms(mp, m_colid_histogram_mapping, histograms_new);
	GPOS_CHECK_ABORT;

	CStatisticsUtils::AddWidthInfo(mp, m_colid_width_mapping, widths_new);
	GPOS_CHECK_ABORT;

	CDouble scaled_num_rows = m_rows * factor;

	// create a scaled stats object
	CStatistics *scaled_stats =
		GPOS_NEW(mp) CStatistics(mp, histograms_new, widths_new,
								 scaled_num_rows, IsEmpty(), m_num_predicates);

	// In the output statistics object, the upper bound source cardinality of the scaled column
	// cannot be greater than the the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated output cardinality.

	// modify source id to upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(
		mp, this, scaled_stats, scaled_num_rows,
		CStatistics::EcbmMin /* card_bounding_method */);

	return scaled_stats;
}

//	copy statistics object with re-mapped column ids
IStatistics *
CStatistics::CopyStatsWithRemap(CMemoryPool *mp,
								UlongToColRefMap *colref_mapping,
								BOOL must_exist) const
{
	GPOS_ASSERT(NULL != colref_mapping);
	UlongToHistogramMap *histograms_new = GPOS_NEW(mp) UlongToHistogramMap(mp);
	UlongToDoubleMap *widths_new = GPOS_NEW(mp) UlongToDoubleMap(mp);

	AddHistogramsWithRemap(mp, m_colid_histogram_mapping, histograms_new,
						   colref_mapping, must_exist);
	AddWidthInfoWithRemap(mp, m_colid_width_mapping, widths_new, colref_mapping,
						  must_exist);

	// create a copy of the stats object
	CStatistics *stats_copy = GPOS_NEW(mp) CStatistics(
		mp, histograms_new, widths_new, m_rows, IsEmpty(), m_num_predicates);

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the the upper bound source cardinality information maintained in the input
	// statistics object.

	// copy the upper bound ndv information
	const ULONG length = m_src_upper_bound_NDVs->Size();
	for (ULONG i = 0; i < length; i++)
	{
		const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
		CUpperBoundNDVs *upper_bound_NDVs_copy =
			upper_bound_NDVs->CopyUpperBoundNDVWithRemap(mp, colref_mapping);

		if (NULL != upper_bound_NDVs_copy)
		{
			stats_copy->AddCardUpperBound(upper_bound_NDVs_copy);
		}
	}

	return stats_copy;
}

//	return the column identifiers of all columns whose statistics are
//	maintained by the statistics object
ULongPtrArray *
CStatistics::GetColIdsWithStats(CMemoryPool *mp) const
{
	ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);

	UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG colid = *(col_hist_mapping.Key());
		colids->Append(GPOS_NEW(mp) ULONG(colid));
	}

	return colids;
}

// return the set of column references we have statistics for
CColRefSet *
CStatistics::GetColRefSet(CMemoryPool *mp) const
{
	CColRefSet *colrefs = GPOS_NEW(mp) CColRefSet(mp);
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	UlongToHistogramMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG colid = *(col_hist_mapping.Key());
		CColRef *colref = col_factory->LookupColRef(colid);
		GPOS_ASSERT(NULL != colref);

		colrefs->Include(colref);
	}

	return colrefs;
}

//	append given histograms to current object where the column ids have been re-mapped
void
CStatistics::AddHistogramsWithRemap(CMemoryPool *mp,
									UlongToHistogramMap *src_histograms,
									UlongToHistogramMap *dest_histograms,
									UlongToColRefMap *colref_mapping,
									BOOL
#ifdef GPOS_DEBUG
										must_exist
#endif	//GPOS_DEBUG
)
{
	UlongToColRefMapIter colref_iterator(colref_mapping);
	while (colref_iterator.Advance())
	{
		ULONG src_colid = *(colref_iterator.Key());
		const CColRef *dest_colref = colref_iterator.Value();
		GPOS_ASSERT_IMP(must_exist, NULL != dest_colref);

		ULONG dest_colid = dest_colref->Id();

		const CHistogram *src_histogram = src_histograms->Find(&src_colid);
		if (NULL != src_histogram)
		{
			CStatisticsUtils::AddHistogram(mp, dest_colid, src_histogram,
										   dest_histograms);
		}
	}
}

// add width information where the column ids have been re-mapped
void
CStatistics::AddWidthInfoWithRemap(CMemoryPool *mp, UlongToDoubleMap *src_width,
								   UlongToDoubleMap *dest_width,
								   UlongToColRefMap *colref_mapping,
								   BOOL must_exist)
{
	UlongToDoubleMapIter col_width_map_iterator(src_width);
	while (col_width_map_iterator.Advance())
	{
		ULONG colid = *(col_width_map_iterator.Key());
		CColRef *new_colref = colref_mapping->Find(&colid);
		if (must_exist && NULL == new_colref)
		{
			continue;
		}

		if (NULL != new_colref)
		{
			colid = new_colref->Id();
		}

		if (NULL == dest_width->Find(&colid))
		{
			const CDouble *width = col_width_map_iterator.Value();
			CDouble *width_copy = GPOS_NEW(mp) CDouble(*width);
#ifdef GPOS_DEBUG
			BOOL result =
#endif	// GPOS_DEBUG
				dest_width->Insert(GPOS_NEW(mp) ULONG(colid), width_copy);
			GPOS_ASSERT(result);
		}
	}
}

// return the index of the array of upper bound ndvs to which column reference belongs
ULONG
CStatistics::GetIndexUpperBoundNDVs(const CColRef *colref)
{
	GPOS_ASSERT(NULL != colref);

	const ULONG length = m_src_upper_bound_NDVs->Size();
	for (ULONG i = 0; i < length; i++)
	{
		const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
		if (upper_bound_NDVs->IsPresent(colref))
		{
			return i;
		}
	}

	return gpos::ulong_max;
}

// add upper bound of source cardinality
void
CStatistics::AddCardUpperBound(CUpperBoundNDVs *upper_bound_NDVs)
{
	GPOS_ASSERT(NULL != upper_bound_NDVs);

	m_src_upper_bound_NDVs->Append(upper_bound_NDVs);
}

// return the upper bound of ndvs for a column reference
CDouble
CStatistics::GetColUpperBoundNDVs(const CColRef *colref)
{
	GPOS_ASSERT(NULL != colref);

	const ULONG length = m_src_upper_bound_NDVs->Size();
	for (ULONG i = 0; i < length; i++)
	{
		const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
		if (upper_bound_NDVs->IsPresent(colref))
		{
			return upper_bound_NDVs->UpperBoundNDVs();
		}
	}

	return DefaultDistinctValues;
}


// look up the number of distinct values of a particular column
CDouble
CStatistics::GetNDVs(const CColRef *colref)
{
	ULONG colid = colref->Id();
	CHistogram *col_histogram = m_colid_histogram_mapping->Find(&colid);
	if (NULL != col_histogram)
	{
		return std::min(col_histogram->GetNumDistinct(),
						GetColUpperBoundNDVs(colref));
	}

#ifdef GPOS_DEBUG
	{
		// the case of no histogram available for requested column signals
		// something wrong with computation of required statistics columns,
		// we print a debug message to log this case

		CAutoMemoryPool amp;
		CAutoTrace at(amp.Pmp());

		at.Os() << "\nREQUESTED NDVs FOR COL (" << colref->Id()
				<< ") WITH A MISSING HISTOGRAM";
	}
#endif	//GPOS_DEBUG

	// if no histogram is available for required column, we use
	// the number of rows as NDVs estimate
	return std::min(m_rows, GetColUpperBoundNDVs(colref));
}