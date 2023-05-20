//---------------------------------------------------------------------------
//	@filename:
//		CStatisticsUtils.h
//
//	@doc:
//		Utility functions for statistics
//---------------------------------------------------------------------------
#ifndef GPOPT_CStatisticsUtils_H
#define GPOPT_CStatisticsUtils_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CPartFilterMap.h"
#include "duckdb/optimizer/cascade/engine/CStatisticsConfig.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"
#include "duckdb/optimizer/cascade/operators/CScalarBoolOp.h"
#include "duckdb/optimizer/cascade/base/IDatum.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredDisj.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredUnsupported.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredUtils.h"
#include "duckdb/optimizer/cascade/statistics/CFilterStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CScaleFactorUtils.h"

namespace gpopt
{
class CTableDescriptor;
}

namespace gpnaucrates
{
using namespace gpos;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@class:
//		CStatisticsUtils
//
//	@doc:
//		Utility functions for statistics
//
//---------------------------------------------------------------------------
class CStatisticsUtils
{
private:
	// MCV value and its corresponding frequency, used for sorting MCVs
	struct SMcvPair
	{
		// MCV datum
		IDatum *m_datum_mcv;

		// the frequency of this MCV
		CDouble m_mcv_freq;

		// ctor
		SMcvPair(IDatum *datum_mcv, CDouble mcv_freq)
			: m_datum_mcv(datum_mcv), m_mcv_freq(mcv_freq)
		{
		}

		// dtor
		~SMcvPair()
		{
			m_datum_mcv->Release();
		}
	};

	// array of SMcvVPairs
	typedef CDynamicPtrArray<SMcvPair, CleanupDelete> SMcvPairPtrArray;

	// private ctor
	CStatisticsUtils();

	// private dtor
	virtual ~CStatisticsUtils();

	// private copy ctor
	CStatisticsUtils(const CStatisticsUtils &);

	// given MCVs and histogram buckets, merge them into buckets of a single histogram
	static CBucketArray *MergeMcvHistBucket(CMemoryPool *mp, const CBucketArray *mcv_buckets, const CBucketArray *histogram_buckets);

	// split a histogram bucket given an MCV bucket
	static CBucketArray *SplitHistBucketGivenMcvBuckets(CMemoryPool *mp, const CBucket *histogram_bucket, const CBucketArray *mcv_buckets);

	// given lower and upper bound information and their closedness, return a bucket if they can form a valid bucket
	static CBucket *CreateValidBucket(CMemoryPool *mp, CPoint *bucket_lower_bound, CPoint *bucket_upper_bound, BOOL is_lower_closed, BOOL is_upper_closed);

	// given lower and upper bound information and their closedness, test if they can form a valid bucket
	static BOOL IsValidBucket(CPoint *bucket_lower_bound, CPoint *bucket_upper_bound, BOOL is_lower_closed, BOOL is_upper_closed);

	// find the MCVs that fall within the same histogram bucket and perform the split
	static void SplitHistDriver(CMemoryPool *mp, const CBucket *histogram_bucket, const CBucketArray *mcv_buckets, CBucketArray *merged_buckets, ULONG *mcv_index, ULONG mcv);

	// distribute total distinct and frequency of the histogram bucket into the new buckets
	static CBucketArray *DistributeBucketProperties(CMemoryPool *mp, CDouble total_frequency, CDouble total_distinct_values, const CBucketArray *buckets);

	// add the NDVs for all of the grouping columns
	static void AddNdvForAllGrpCols(CMemoryPool *mp, const CStatistics *input_stats, const ULongPtrArray *grouping_columns, CDoubleArray *output_ndvs)
	{
		GPOS_ASSERT(NULL != grouping_columns);
		GPOS_ASSERT(NULL != input_stats);
		GPOS_ASSERT(NULL != output_ndvs);
		const ULONG num_cols = grouping_columns->Size();
		// iterate over grouping columns
		for (ULONG i = 0; i < num_cols; i++)
		{
			ULONG colid = (*(*grouping_columns)[i]);
			CDouble distinct_vals = CStatisticsUtils::DefaultDistinctVals(input_stats->Rows());
			const CHistogram *histogram = input_stats->GetHistogram(colid);
			if (NULL != histogram)
			{
				distinct_vals = histogram->GetNumDistinct();
				if (histogram->IsEmpty())
				{
					distinct_vals = DefaultDistinctVals(input_stats->Rows());
				}
			}
			output_ndvs->Append(GPOS_NEW(mp) CDouble(distinct_vals));
		}
	}

	// compute max number of groups when grouping on columns from the given source
	static CDouble MaxNumGroupsForGivenSrcGprCols(CMemoryPool *mp, const CStatisticsConfig *stats_config, CStatistics *input_stats, const ULongPtrArray *src_grouping_cols)
	{
		GPOS_ASSERT(NULL != input_stats);
		GPOS_ASSERT(NULL != src_grouping_cols);
		GPOS_ASSERT(0 < src_grouping_cols->Size());
		CDouble input_rows = input_stats->Rows();
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
		CColRef *first_colref = col_factory->LookupColRef(*(*src_grouping_cols)[0]);
		CDouble upper_bound_ndvs = input_stats->GetColUpperBoundNDVs(first_colref);
		CDoubleArray *ndvs = GPOS_NEW(mp) CDoubleArray(mp);
		AddNdvForAllGrpCols(mp, input_stats, src_grouping_cols, ndvs);
		// take the minimum of (a) the estimated number of groups from the columns of this source,
		// (b) input rows, and (c) cardinality upper bound for the given source in the
		// input statistics object

		// DNumOfDistVal internally damps the number of columns with our formula.
		// (a) For columns from the same table, they will be damped based on the formula in DNumOfDistVal
		// (b) If the group by has columns from multiple tables, they will again be damped by the formula
		// in DNumOfDistVal when we compute the final group by cardinality
		CDouble groups = std::min(std::max(CStatistics::MinRows.Get(), GetCumulativeNDVs(stats_config, ndvs).Get()), std::min(input_rows.Get(), upper_bound_ndvs.Get()));
		ndvs->Release();
		return groups;
	}

	// check to see if any one of the grouping columns has been capped
	static BOOL CappedGrpColExists(const CStatistics *stats, const ULongPtrArray *grouping_columns)
	{
		GPOS_ASSERT(NULL != stats);
		GPOS_ASSERT(NULL != grouping_columns);
		const ULONG num_cols = grouping_columns->Size();
		for (ULONG i = 0; i < num_cols; i++)
		{
			ULONG colid = (*(*grouping_columns)[i]);
			const CHistogram *histogram = stats->GetHistogram(colid);
			if (NULL != histogram && histogram->WereNDVsScaled())
			{
				return true;
			}
		}
		return false;
	}

	// return the maximum NDV given an array of grouping columns
	static CDouble MaxNdv(const CStatistics *stats, const ULongPtrArray *grouping_columns)
	{
		GPOS_ASSERT(NULL != stats);
		GPOS_ASSERT(NULL != grouping_columns);
		const ULONG num_grp_cols = grouping_columns->Size();
		CDouble ndv_max(1.0);
		for (ULONG i = 0; i < num_grp_cols; i++)
		{
			CDouble ndv = CStatisticsUtils::DefaultDistinctVals(stats->Rows());
			ULONG colid = (*(*grouping_columns)[i]);
			const CHistogram *histogram = stats->GetHistogram(colid);
			if (NULL != histogram)
			{
				ndv = histogram->GetNumDistinct();
				if (histogram->IsEmpty())
				{
					ndv = CStatisticsUtils::DefaultDistinctVals(stats->Rows());
				}
			}
			if (ndv_max < ndv)
			{
				ndv_max = ndv;
			}
		}
		return ndv_max;
	}

public:
	// get the next data point for generating new bucket boundary
	static CPoint *NextPoint(CMemoryPool *mp, CMDAccessor *md_accessor, CPoint *point);

	// transform mcv information to optimizer's histogram structure
	static CHistogram *TransformMCVToHist(CMemoryPool *mp, const IMDType *mdtype, IDatumArray *mcv_datums, CDoubleArray *freq_array, ULONG num_mcv_values);

	// merge MCVs and histogram
	static CHistogram *MergeMCVHist(CMemoryPool *mp,
									const CHistogram *mcv_histogram,
									const CHistogram *histogram);

	// comparison function for sorting MCVs
	static inline INT GetMcvPairCmpFunc(const void *val1, const void *val2);

	// utility function to print column stats before/after applying filter
	static void PrintColStats(CMemoryPool *mp, CStatsPred *pred_stats, ULONG cond_colid, CHistogram *histogram, CDouble last_scale_factor, BOOL is_filter_applied_before);

	// extract all the column identifiers used in the statistics filter
	static void ExtractUsedColIds(CMemoryPool *mp, CBitSet *colids_bitset,
								  CStatsPred *pred_stats,
								  ULongPtrArray *colids);

	// given the previously generated histogram, update the intermediate
	// result of the disjunction
	static void UpdateDisjStatistics(
		CMemoryPool *mp, CBitSet *non_updatable_cols,
		CDouble input_disjunct_rows, CDouble local_rows,
		CHistogram *previous_histogram,
		UlongToHistogramMap *disjunctive_result_histograms, ULONG last_colid);

	// given a disjunction filter, generate a bit set of columns whose
	// histogram buckets cannot be changed by applying the predicates in the
	// disjunction
	static CBitSet *GetColsNonUpdatableHistForDisj(CMemoryPool *mp,
												   CStatsPredDisj *pred_stats);

	// helper method to add a histogram to a map
	static void AddHistogram(CMemoryPool *mp, ULONG colid,
							 const CHistogram *histogram,
							 UlongToHistogramMap *col_histogram_mapping,
							 BOOL replace_old = false);

	// create a new hash map of histograms after merging
	// the histograms generated by the child of disjunctive predicate
	static UlongToHistogramMap *CreateHistHashMapAfterMergingDisjPreds(
		CMemoryPool *mp, CBitSet *non_updatable_cols,
		UlongToHistogramMap *prev_histogram_map,
		UlongToHistogramMap *disj_preds_histogram_map, CDouble cummulative_rows,
		CDouble num_rows_disj_child);

	// helper method to copy the hash map of histograms
	static UlongToHistogramMap *CopyHistHashMap(
		CMemoryPool *mp, UlongToHistogramMap *col_histogram_mapping);

	// return the column identifier of the filter if the predicate is
	// on a single column else	return gpos::ulong_max
	static ULONG GetColId(const CStatsPredPtrArry *stats_preds_array);

	// add remaining buckets from one array of buckets to the other
	static void AddRemainingBuckets(CMemoryPool *mp,
									const CBucketArray *src_buckets,
									CBucketArray *dest_buckets,
									ULONG *start_val);

	// generate a null datum with the type of passed colref
	static IDatum *DatumNull(const CColRef *colref);

#ifdef GPOS_DEBUG
	// helper method to print the hash map of histograms
	static void PrintHistogramMap(IOstream &os,
								  UlongToHistogramMap *col_histogram_mapping);
#endif	// GPOS_DEBUG

	// derive statistics of dynamic scan based on part-selector stats in the given map
	static IStatistics *DeriveStatsForDynamicScan(CMemoryPool *mp, CExpressionHandle &expr_handle, ULONG part_idx_id, CPartFilterMap *part_filter_map);

	// derive statistics of (dynamic) index-get
	static IStatistics *DeriveStatsForIndexGet(CMemoryPool *mp, CExpressionHandle &exprhdl, IStatisticsArray *stats_contexts);

	// derive statistics of bitmap table-get
	static IStatistics* DeriveStatsForBitmapTableGet(CMemoryPool *mp, CExpressionHandle &expr_handle, IStatisticsArray *stats_contexts)
	{
		GPOS_ASSERT(CLogical::EopLogicalBitmapTableGet == expr_handle.Pop()->Eopid() || CLogical::EopLogicalDynamicBitmapTableGet == expr_handle.Pop()->Eopid());
		CTableDescriptor *table_descriptor = expr_handle.DeriveTableDescriptor();
		// the index of the condition
		ULONG child_cond_index = 0;
		// get outer references from expression handle
		CColRefSet *outer_col_refset = expr_handle.DeriveOuterReferences();
		CExpression *local_expr = NULL;
		CExpression *outer_refs_expr = NULL;
		CExpression *scalar_expr = expr_handle.PexprScalarRepChild(child_cond_index);
		CPredicateUtils::SeparateOuterRefs(mp, scalar_expr, outer_col_refset, &local_expr, &outer_refs_expr);
		// collect columns used by the index
		CColRefSet *used_col_refset = GPOS_NEW(mp) CColRefSet(mp);
		used_col_refset->Union(expr_handle.DeriveUsedColumns(child_cond_index));
		used_col_refset->Difference(outer_col_refset);
		IStatistics *base_table_stats = CLogical::PstatsBaseTable(mp, expr_handle, table_descriptor, used_col_refset);
		used_col_refset->Release();
		IStatistics *stats = CFilterStatsProcessor::MakeStatsFilterForScalarExpr(mp, expr_handle, base_table_stats, local_expr, outer_refs_expr, stats_contexts);
		base_table_stats->Release();
		local_expr->Release();
		outer_refs_expr->Release();
		return stats;
	}

	// compute the cumulative number of distinct values (NDV) of the group by operator
	// from the array of NDV of the individual grouping columns
	static CDouble GetCumulativeNDVs(const CStatisticsConfig *stats_config, CDoubleArray *ndvs)
	{
		GPOS_ASSERT(NULL != stats_config);
		GPOS_ASSERT(NULL != ndvs);
		CScaleFactorUtils::SortScalingFactor(ndvs, true);
		const ULONG ndv_size = ndvs->Size();
		if (0 == ndv_size)
		{
			return CDouble(1.0);
		}
		CDouble cumulative_ndv = *(*ndvs)[0];
		for (ULONG idx = 1; idx < ndv_size; idx++)
		{
			CDouble ndv = *(*ndvs)[idx];
			CDouble ndv_damped = std::max(CHistogram::MinDistinct.Get(), (ndv * CScaleFactorUtils::DampedGroupByScaleFactor(stats_config, idx)).Get());
			cumulative_ndv = cumulative_ndv * ndv_damped;
		}
		return cumulative_ndv;
	}

	// return the mapping between the table column used for grouping to the logical operator id where it was defined.
	// If the grouping column is not a table column then the logical op id is initialized to gpos::ulong_max
	static UlongToUlongPtrArrayMap *GetGrpColIdToUpperBoundNDVIdxMap(CMemoryPool *mp, CStatistics *stats, const CColRefSet *grp_cols_refset, CBitSet *keys)
	{
		GPOS_ASSERT(NULL != grp_cols_refset);
		GPOS_ASSERT(NULL != stats);
		UlongToUlongPtrArrayMap *grp_colid_upper_bound_ndv_idx_map = GPOS_NEW(mp) UlongToUlongPtrArrayMap(mp);
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
		// iterate over grouping columns
		CColRefSetIter col_refset_iter(*grp_cols_refset);
		while (col_refset_iter.Advance())
		{
			CColRef *grouping_colref = col_refset_iter.Pcr();
			ULONG colid = grouping_colref->Id();
			if (NULL == keys || keys->Get(colid))
			{
				// if keys are available then only consider grouping columns defined as
				// key columns else consider all grouping columns
				const CColRef *grouping_colref = col_factory->LookupColRef(colid);
				const ULONG upper_bound_ndv_idx = stats->GetIndexUpperBoundNDVs(grouping_colref);
				const ULongPtrArray *ndv_colid = grp_colid_upper_bound_ndv_idx_map->Find(&upper_bound_ndv_idx);
				if (NULL == ndv_colid)
				{
					ULongPtrArray *colids_new = GPOS_NEW(mp) ULongPtrArray(mp);
					colids_new->Append(GPOS_NEW(mp) ULONG(colid));
#ifdef GPOS_DEBUG
					BOOL fres =
#endif	// GPOS_DEBUG
						grp_colid_upper_bound_ndv_idx_map->Insert(GPOS_NEW(mp) ULONG(upper_bound_ndv_idx), colids_new);
					GPOS_ASSERT(fres);
				}
				else
				{
					(const_cast<ULongPtrArray *>(ndv_colid))->Append(GPOS_NEW(mp) ULONG(colid));
				}
			}
		}
		return grp_colid_upper_bound_ndv_idx_map;
	}
	
	// extract NDVs for the given array of grouping columns
	static CDoubleArray *ExtractNDVForGrpCols(CMemoryPool *mp, const CStatisticsConfig *stats_config, const IStatistics *stats, CColRefSet *grp_cols_refset, CBitSet *keys)
	{
		GPOS_ASSERT(NULL != stats);
		GPOS_ASSERT(NULL != grp_cols_refset);
		CStatistics *input_stats = CStatistics::CastStats(const_cast<IStatistics *>(stats));
		CDoubleArray *ndvs = GPOS_NEW(mp) CDoubleArray(mp);
		UlongToUlongPtrArrayMap *grp_colid_upper_bound_ndv_idx_map = GetGrpColIdToUpperBoundNDVIdxMap(mp, input_stats, grp_cols_refset, keys);
		UlongToUlongPtrArrayMapIter map_iter(grp_colid_upper_bound_ndv_idx_map);
		while (map_iter.Advance())
		{
			ULONG source_id = *(map_iter.Key());
			const ULongPtrArray *src_grouping_cols = map_iter.Value();
			if (gpos::ulong_max == source_id)
			{
				// this array of grouping columns represents computed columns.
				// Since we currently do not cap computed columns, we add all of their NDVs as is
				AddNdvForAllGrpCols(mp, input_stats, src_grouping_cols, ndvs);
			}
			else
			{
				// compute the maximum number of groups when aggregated on columns from the given source
				CDouble max_grps_per_src = MaxNumGroupsForGivenSrcGprCols(mp, stats_config, input_stats, src_grouping_cols);
				ndvs->Append(GPOS_NEW(mp) CDouble(max_grps_per_src));
			}
		}
		// clean up
		grp_colid_upper_bound_ndv_idx_map->Release();
		return ndvs;
	}

	// compute the cumulative number of groups for the given set of grouping columns
	static CDouble Groups(CMemoryPool *mp, IStatistics *stats, const CStatisticsConfig *stats_config, ULongPtrArray *grouping_cols, CBitSet *keys)
	{
		GPOS_ASSERT(NULL != stats);
		GPOS_ASSERT(NULL != stats_config);
		GPOS_ASSERT(NULL != grouping_cols);
		CColRefSet *computed_groupby_cols = GPOS_NEW(mp) CColRefSet(mp);
		CColRefSet *grp_col_for_stats = MakeGroupByColsForStats(mp, grouping_cols, computed_groupby_cols);
		CDoubleArray *ndvs = ExtractNDVForGrpCols(mp, stats_config, stats, grp_col_for_stats, keys);
		CDouble groups = std::min(std::max(CStatistics::MinRows.Get(), GetCumulativeNDVs(stats_config, ndvs).Get()), stats->Rows().Get());
		// clean up
		grp_col_for_stats->Release();
		computed_groupby_cols->Release();
		ndvs->Release();
		return groups;
	}

	// return the default number of distinct values
	static CDouble
	DefaultDistinctVals(CDouble rows)
	{
		return std::min(CStatistics::DefaultDistinctValues.Get(), rows.Get());
	}

	// add the statistics (histogram and width) of the grouping columns
	static void AddGrpColStats(CMemoryPool *mp, const CStatistics *input_stats, CColRefSet *grp_cols_refset, UlongToHistogramMap *output_histograms, UlongToDoubleMap *output_col_widths)
	{
		GPOS_ASSERT(NULL != input_stats);
		GPOS_ASSERT(NULL != grp_cols_refset);
		GPOS_ASSERT(NULL != output_histograms);
		GPOS_ASSERT(NULL != output_col_widths);
		// iterate over grouping columns
		CColRefSetIter grp_col_refset_iter(*grp_cols_refset);
		while (grp_col_refset_iter.Advance())
		{
			CColRef *colref = grp_col_refset_iter.Pcr();
			ULONG grp_colid = colref->Id();
			CDouble num_distinct_vals(CHistogram::MinDistinct);
			const CHistogram *histogram = input_stats->GetHistogram(grp_colid);
			if (NULL != histogram)
			{
				CHistogram *result_histogram = histogram->MakeGroupByHistogramNormalize(input_stats->Rows(), &num_distinct_vals);
				if (histogram->WereNDVsScaled())
				{
					result_histogram->SetNDVScaled();
				}
				AddHistogram(mp, grp_colid, result_histogram, output_histograms);
				GPOS_DELETE(result_histogram);
			}
			const CDouble *width = input_stats->GetWidth(grp_colid);
			if (NULL != width)
			{
				output_col_widths->Insert(GPOS_NEW(mp) ULONG(grp_colid), GPOS_NEW(mp) CDouble(*width));
			}
		}
	}

	// return the set of grouping columns for statistics computation;
	static CColRefSet *MakeGroupByColsForStats(CMemoryPool *mp, const ULongPtrArray *grouping_columns, CColRefSet* computed_groupby_cols)
	{
		GPOS_ASSERT(NULL != grouping_columns);
		GPOS_ASSERT(NULL != computed_groupby_cols);
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
		CColRefSet *grp_col_for_stats = GPOS_NEW(mp) CColRefSet(mp);
		const ULONG ulGrpCols = grouping_columns->Size();
		// iterate over grouping columns
		for (ULONG i = 0; i < ulGrpCols; i++)
		{
			ULONG colid = *(*grouping_columns)[i];
			CColRef *grp_col_ref = col_factory->LookupColRef(colid);
			GPOS_ASSERT(NULL != grp_col_ref);
			// check to see if the grouping column is a computed attribute
			const CColRefSet *used_col_refset = col_factory->PcrsUsedInComputedCol(grp_col_ref);
			if (NULL == used_col_refset || 0 == used_col_refset->Size())
			{
				(void) grp_col_for_stats->Include(grp_col_ref);
			}
			else
			{
				(void) grp_col_for_stats->Union(used_col_refset);
				(void) computed_groupby_cols->Include(grp_col_ref);
			}
		}
		return grp_col_for_stats;
	}

	// return the total number of distinct values in the given array of buckets
	static CDouble GetNumDistinct(const CBucketArray *histogram_buckets)
	{
		GPOS_ASSERT(NULL != histogram_buckets);
		CDouble distinct = CDouble(0.0);
		const ULONG num_of_buckets = histogram_buckets->Size();
		for (ULONG bucket_index = 0; bucket_index < num_of_buckets; bucket_index++)
		{
			CBucket *bucket = (*histogram_buckets)[bucket_index];
			distinct = distinct + bucket->GetNumDistinct();
		}
		return distinct;
	}

	// return the cumulative frequency in the given array of buckets
	static CDouble GetFrequency(const CBucketArray *histogram_buckets)
	{
		GPOS_ASSERT(NULL != histogram_buckets);
		CDouble freq = CDouble(0.0);
		const ULONG num_of_buckets = histogram_buckets->Size();
		for (ULONG bucket_index = 0; bucket_index < num_of_buckets; bucket_index++)
		{
			CBucket *bucket = (*histogram_buckets)[bucket_index];
			freq = freq + bucket->GetFrequency();
		}
		return freq;
	}

	// true if the given operator increases risk of cardinality misestimation
	static BOOL IncreasesRisk(CLogical *logical_op)
	{
		if (logical_op->FSelectionOp())
		{
			// operators that perform filters (e.g. joins, select) increase the risk
			return true;
		}
		static COperator::EOperatorId grouping_and_semi_join_opid_array[] = {COperator::EopLogicalGbAgg, COperator::EopLogicalIntersect, COperator::EopLogicalIntersectAll, COperator::EopLogicalUnion, COperator::EopLogicalDifference,   COperator::EopLogicalDifferenceAll};
		COperator::EOperatorId operator_id = logical_op->Eopid();
		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(grouping_and_semi_join_opid_array); i++)
		{
			if (grouping_and_semi_join_opid_array[i] == operator_id)
			{
				return true;
			}
		}
		return false;
	}

	// return the default column width
	static CDouble DefaultColumnWidth(const IMDType *mdtype)
	{
		CDouble width(CStatistics::DefaultColumnWidth);
		if (mdtype->IsFixedLength())
		{
			width = CDouble(mdtype->Length());
		}
		return width;
	}

	// helper method to add width information
	static void AddWidthInfo(CMemoryPool *mp, UlongToDoubleMap *src_width, UlongToDoubleMap *dest_width)
	{
		UlongToDoubleMapIter col_width_map_iterator(src_width);
		while (col_width_map_iterator.Advance())
		{
			ULONG colid = *(col_width_map_iterator.Key());
			BOOL is_present = (NULL != dest_width->Find(&colid));
			if (!is_present)
			{
				const CDouble *width = col_width_map_iterator.Value();
				CDouble *width_copy = GPOS_NEW(mp) CDouble(*width);
				dest_width->Insert(GPOS_NEW(mp) ULONG(colid), width_copy);
			}

			GPOS_CHECK_ABORT;
		}
	}

	// for the output stats object, compute its upper bound cardinality mapping based on the bounding method
	// estimated output cardinality and information maintained in the current stats object
	static void ComputeCardUpperBounds(CMemoryPool *mp, const CStatistics *input_stats, CStatistics* output_stats, CDouble rows_output, CStatistics::ECardBoundingMethod card_bounding_method)
	{
		GPOS_ASSERT(NULL != output_stats);
		GPOS_ASSERT(CStatistics::EcbmSentinel != card_bounding_method);
		const CUpperBoundNDVPtrArray *input_stats_ndv_upper_bound = input_stats->GetUpperBoundNDVs();
		ULONG length = input_stats_ndv_upper_bound->Size();
		for (ULONG i = 0; i < length; i++)
		{
			const CUpperBoundNDVs *upper_bound_NDVs = (*input_stats_ndv_upper_bound)[i];
			CDouble upper_bound_ndv_input = upper_bound_NDVs->UpperBoundNDVs();
			CDouble upper_bound_ndv_output = rows_output;
			if (CStatistics::EcbmInputSourceMaxCard == card_bounding_method)
			{
				upper_bound_ndv_output = upper_bound_ndv_input;
			}
			else if (CStatistics::EcbmMin == card_bounding_method)
			{
				upper_bound_ndv_output = std::min(upper_bound_ndv_input.Get(), rows_output.Get());
			}
			CUpperBoundNDVs *upper_bound_NDVs_copy = upper_bound_NDVs->CopyUpperBoundNDVs(mp, upper_bound_ndv_output);
			output_stats->AddCardUpperBound(upper_bound_NDVs_copy);
		}
	}

	static BOOL IsStatsCmpTypeNdvEq(CStatsPred::EStatsCmpType stats_cmp_type)
	{
		return (CStatsPred::EstatscmptEqNDV == stats_cmp_type);
	}

};	// class CStatisticsUtils

// comparison function for sorting MCVs
INT
CStatisticsUtils::GetMcvPairCmpFunc(const void *val1, const void *val2)
{
	GPOS_ASSERT(NULL != val1);
	GPOS_ASSERT(NULL != val2);
	const SMcvPair *mcv_pair1 = *(const SMcvPair **) (val1);
	const SMcvPair *mcv_pair2 = *(const SMcvPair **) (val2);

	const IDatum *datum1 = mcv_pair1->m_datum_mcv;
	const IDatum *datum2 = mcv_pair2->m_datum_mcv;

	if (datum1->StatsAreEqual(datum2))
	{
		return 0;
	}

	if (datum1->StatsAreComparable(datum2) && datum1->StatsAreLessThan(datum2))
	{
		return -1;
	}

	return 1;
}
}  // namespace gpnaucrates

#endif