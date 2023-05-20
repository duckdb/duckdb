//---------------------------------------------------------------------------
//	@filename:
//		CJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing all join types
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/operators/CScalarNAryJoinPredList.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/statistics/CFilterStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "duckdb/optimizer/cascade/statistics/CScaleFactorUtils.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpopt;

BOOL CJoinStatsProcessor::m_compute_scale_factor_from_histogram_buckets = false;

// helper for joining histograms
void
CJoinStatsProcessor::JoinHistograms(
	CMemoryPool *mp, const CHistogram *histogram1, const CHistogram *histogram2,
	CStatsPredJoin *join_pred_stats, CDouble num_rows1, CDouble num_rows2,
	CHistogram **result_hist1,	// output: histogram 1 after join
	CHistogram **result_hist2,	// output: histogram 2 after join
	CDouble *scale_factor,		// output: scale factor based on the join
	BOOL is_input_empty, IStatistics::EStatsJoinType join_type,
	BOOL DoIgnoreLASJHistComputation)
{
	GPOS_ASSERT(NULL != histogram1);
	GPOS_ASSERT(NULL != histogram2);
	GPOS_ASSERT(NULL != join_pred_stats);
	GPOS_ASSERT(NULL != result_hist1);
	GPOS_ASSERT(NULL != result_hist2);
	GPOS_ASSERT(NULL != scale_factor);

	if (IStatistics::EsjtLeftAntiSemiJoin == join_type)
	{
		CLeftAntiSemiJoinStatsProcessor::JoinHistogramsLASJ(
			histogram1, histogram2, join_pred_stats, num_rows1, num_rows2,
			result_hist1, result_hist2, scale_factor, is_input_empty, join_type,
			DoIgnoreLASJHistComputation);

		return;
	}

	if (is_input_empty)
	{
		// use Cartesian product as scale factor
		*scale_factor = num_rows1 * num_rows2;
		*result_hist1 = GPOS_NEW(mp) CHistogram(mp);
		*result_hist2 = GPOS_NEW(mp) CHistogram(mp);

		return;
	}

	*scale_factor = CScaleFactorUtils::DefaultJoinPredScaleFactor;

	CStatsPred::EStatsCmpType stats_cmp_type = join_pred_stats->GetCmpType();
	BOOL empty_histograms = histogram1->IsEmpty() || histogram2->IsEmpty();

	if (empty_histograms)
	{
		// if one more input has no histograms (due to lack of statistics
		// for table columns or computed columns), we estimate
		// the join cardinality to be the max of the two rows.
		// In other words, the scale factor is equivalent to the
		// min of the two rows.
		*scale_factor = std::min(num_rows1, num_rows2);
	}
	else if (CHistogram::JoinPredCmpTypeIsSupported(stats_cmp_type))
	{
		CHistogram *join_histogram = histogram1->MakeJoinHistogramNormalize(
			stats_cmp_type, num_rows1, histogram2, num_rows2, scale_factor);

		if (CStatsPred::EstatscmptEq == stats_cmp_type ||
			CStatsPred::EstatscmptINDF == stats_cmp_type ||
			CStatisticsUtils::IsStatsCmpTypeNdvEq(stats_cmp_type))
		{
			if (histogram1->WereNDVsScaled())
			{
				join_histogram->SetNDVScaled();
			}
			*result_hist1 = join_histogram;
			*result_hist2 = (*result_hist1)->CopyHistogram();
			if (histogram2->WereNDVsScaled())
			{
				(*result_hist2)->SetNDVScaled();
			}
			return;
		}

		// note that for IDF and Not Equality predicate, we do not generate histograms but
		// just the scale factors.

		GPOS_ASSERT(join_histogram->IsEmpty());
		GPOS_DELETE(join_histogram);

		// TODO:  Feb 21 2014, for all join condition except for "=" join predicate
		// we currently do not compute new histograms for the join columns
	}

	// for an unsupported join predicate operator or in the case of
	// missing histograms, copy input histograms and use default scale factor
	*result_hist1 = histogram1->CopyHistogram();
	*result_hist2 = histogram2->CopyHistogram();
}

//	derive statistics for the given join's predicate(s)
IStatistics* CJoinStatsProcessor::CalcAllJoinStats(CMemoryPool *mp, IStatisticsArray *statistics_array, CExpression *expr, COperator *pop)
{
	GPOS_ASSERT(NULL != expr);
	GPOS_ASSERT(NULL != statistics_array);
	GPOS_ASSERT(0 < statistics_array->Size());
	// Is the operator passed in a 2-way LOJ? We will later refine this to find whether
	// an individual predicate is for an LOJ or not.
	BOOL left_outer_2_way_join = false;

	// create an empty set of outer references for statistics derivation
	CColRefSet *outer_refs = GPOS_NEW(mp) CColRefSet(mp);

	// join statistics objects one by one using relevant predicates in given scalar expression
	const ULONG num_stats = statistics_array->Size();
	IStatistics *stats = (*statistics_array)[0]->CopyStats(mp);
	CDouble num_rows_outer = stats->Rows();
	// predicate indexes, if we have a mix of inner and LOJs
	ULongPtrArray *predIndexes = NULL;
	CExpression *inner_or_simple_2_way_loj_preds = expr;

	switch (pop->Eopid())
	{
		case COperator::EopLogicalLeftOuterJoin:
			left_outer_2_way_join = true;
			break;

		case COperator::EopLogicalNAryJoin:
			predIndexes =
				CLogicalNAryJoin::PopConvert(pop)->GetLojChildPredIndexes();
			if (NULL != predIndexes)
			{
				GPOS_ASSERT(COperator::EopScalarNAryJoinPredList == expr->Pop()->Eopid());
				inner_or_simple_2_way_loj_preds = (*expr)[GPOPT_ZERO_INNER_JOIN_PRED_INDEX];
			}
			break;

		default:
			break;
	}

	for (ULONG i = 1; i < num_stats; i++)
	{
		IStatistics *current_stats = (*statistics_array)[i];

		CColRefSetArray *output_colrefsets = GPOS_NEW(mp) CColRefSetArray(mp);
		output_colrefsets->Append(stats->GetColRefSet(mp));
		output_colrefsets->Append(current_stats->GetColRefSet(mp));

		CStatsPred *unsupported_pred_stats = NULL;
		BOOL is_a_left_join = left_outer_2_way_join;
		CExpression *join_preds_available = NULL;

		if (NULL == predIndexes ||
			GPOPT_ZERO_INNER_JOIN_PRED_INDEX == *(*predIndexes)[i])
		{
			join_preds_available = inner_or_simple_2_way_loj_preds;
		}
		else
		{
			// this is an LOJ that is part of an NAry join, get the corresponding ON predicate
			is_a_left_join = true;
			join_preds_available = (*expr)[*(*predIndexes)[i]];
		}

		CStatsPredJoinArray *join_preds_stats =
			CStatsPredUtils::ExtractJoinStatsFromJoinPredArray(
				mp, join_preds_available, output_colrefsets, outer_refs,
				is_a_left_join,	 // left joins use an anti-semijoin internally
				&unsupported_pred_stats);

		IStatistics *new_stats = NULL;

		if (is_a_left_join)
		{
			new_stats =
				stats->CalcLOJoinStats(mp, current_stats, join_preds_stats);
		}
		else
		{
			new_stats =
				stats->CalcInnerJoinStats(mp, current_stats, join_preds_stats);
		}
		stats->Release();
		stats = new_stats;

		if (NULL != unsupported_pred_stats)
		{
			// apply the unsupported join filters as a filter on top of the join results.
			// TODO,  June 13 2014 we currently only cap NDVs for filters
			// immediately on top of tables.
			IStatistics *stats_after_join_filter =
				CFilterStatsProcessor::MakeStatsFilter(
					mp, dynamic_cast<CStatistics *>(stats),
					unsupported_pred_stats, false /* do_cap_NDVs */);

			// If it is outer join and the cardinality after applying the unsupported join
			// filters is less than the cardinality of outer child, we don't use this stats.
			// Because we need to make sure that Card(LOJ) >= Card(Outer child of LOJ).
			if (is_a_left_join &&
				stats_after_join_filter->Rows() < num_rows_outer)
			{
				stats_after_join_filter->Release();
			}
			else
			{
				stats->Release();
				stats = stats_after_join_filter;
			}

			unsupported_pred_stats->Release();
		}

		num_rows_outer = stats->Rows();

		join_preds_stats->Release();
		output_colrefsets->Release();
	}

	// clean up
	outer_refs->Release();

	return stats;
}


// main driver to generate join stats
CStatistics *
CJoinStatsProcessor::SetResultingJoinStats(
	CMemoryPool *mp, CStatisticsConfig *stats_config,
	const IStatistics *outer_stats_input, const IStatistics *inner_stats_input,
	CStatsPredJoinArray *join_pred_stats_info,
	IStatistics::EStatsJoinType join_type, BOOL DoIgnoreLASJHistComputation)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != inner_stats_input);
	GPOS_ASSERT(NULL != outer_stats_input);

	GPOS_ASSERT(NULL != join_pred_stats_info);

	BOOL IsLASJ = (IStatistics::EsjtLeftAntiSemiJoin == join_type);
	BOOL semi_join = IStatistics::IsSemiJoin(join_type);

	// Extract stat objects for inner and outer child.
	// Historically, IStatistics was meant to have multiple derived classes
	// However, currently only CStatistics implements IStatistics
	// Until this changes, the interfaces have been designed to take IStatistics as parameters
	// In the future, IStatistics should be removed, as it is not being utilized as designed
	const CStatistics *outer_stats =
		dynamic_cast<const CStatistics *>(outer_stats_input);
	const CStatistics *inner_side_stats =
		dynamic_cast<const CStatistics *>(inner_stats_input);

	// create hash map from colid -> histogram for resultant structure
	UlongToHistogramMap *result_col_hist_mapping =
		GPOS_NEW(mp) UlongToHistogramMap(mp);

	// build a bitset with all join columns
	CBitSet *join_colids = GPOS_NEW(mp) CBitSet(mp);
	for (ULONG i = 0; i < join_pred_stats_info->Size(); i++)
	{
		CStatsPredJoin *join_stats = (*join_pred_stats_info)[i];

		if (join_stats->HasValidColIdOuter())
		{
			(void) join_colids->ExchangeSet(join_stats->ColIdOuter());
		}
		if (!semi_join && join_stats->HasValidColIdInner())
		{
			(void) join_colids->ExchangeSet(join_stats->ColIdInner());
		}
	}

	// histograms on columns that do not appear in join condition will
	// be copied over to the result structure
	outer_stats->AddNotExcludedHistograms(mp, join_colids,
										  result_col_hist_mapping);
	if (!semi_join)
	{
		inner_side_stats->AddNotExcludedHistograms(mp, join_colids,
												   result_col_hist_mapping);
	}

	CScaleFactorUtils::SJoinConditionArray *join_conds_scale_factors =
		GPOS_NEW(mp) CScaleFactorUtils::SJoinConditionArray(mp);
	const ULONG num_join_conds = join_pred_stats_info->Size();

	BOOL output_is_empty = false;
	CDouble num_join_rows = 0;
	// iterate over join's predicate(s)
	for (ULONG i = 0; i < num_join_conds; i++)
	{
		CStatsPredJoin *pred_info = (*join_pred_stats_info)[i];
		ULONG colid1 = pred_info->ColIdOuter();
		ULONG colid2 = pred_info->ColIdInner();
		GPOS_ASSERT(colid1 != colid2);
		const CHistogram *outer_histogram = NULL;
		const CHistogram *inner_histogram = NULL;
		BOOL is_input_empty =
			CStatistics::IsEmptyJoin(outer_stats, inner_side_stats, IsLASJ);
		CDouble local_scale_factor(1.0);
		CHistogram *outer_histogram_after = NULL;
		CHistogram *inner_histogram_after = NULL;


		// find the histograms corresponding to the two columns
		// are column id1 and 2 always in the order of outer inner?
		if (pred_info->HasValidColIdOuter())
		{
			outer_histogram = outer_stats->GetHistogram(colid1);
			GPOS_ASSERT(NULL != outer_histogram);
		}
		if (pred_info->HasValidColIdInner())
		{
			inner_histogram = inner_side_stats->GetHistogram(colid2);
			GPOS_ASSERT(NULL != inner_histogram);
		}

		// When we have any form of equi join with join condition of type f(a)=b,
		// we calculate the NDV of such a join as NDV(b) ( from Selinger et al.)
		if (NULL == outer_histogram)
		{
			GPOS_ASSERT(CStatsPred::EstatscmptEqNDV == pred_info->GetCmpType());
			outer_histogram = inner_histogram;
			colid1 = colid2;
		}
		else if (NULL == inner_histogram)
		{
			GPOS_ASSERT(CStatsPred::EstatscmptEqNDV == pred_info->GetCmpType());
			inner_histogram = outer_histogram;
			colid2 = colid1;
		}

		JoinHistograms(mp, outer_histogram, inner_histogram, pred_info,
					   outer_stats->Rows(), inner_side_stats->Rows(),
					   &outer_histogram_after, &inner_histogram_after,
					   &local_scale_factor, is_input_empty, join_type,
					   DoIgnoreLASJHistComputation);


		output_is_empty = JoinStatsAreEmpty(
			outer_stats->IsEmpty(), output_is_empty, outer_histogram,
			inner_histogram, outer_histogram_after, join_type);

		CStatisticsUtils::AddHistogram(mp, colid1, outer_histogram_after,
									   result_col_hist_mapping);
		if (!semi_join && colid1 != colid2)
		{
			CStatisticsUtils::AddHistogram(mp, colid2, inner_histogram_after,
										   result_col_hist_mapping);
		}

		GPOS_DELETE(outer_histogram_after);
		GPOS_DELETE(inner_histogram_after);

		// remember which tables the columns came from, this info is used to combine scale factors
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

		CColRef *colref_outer = col_factory->LookupColRef(colid1);
		CColRef *colref_inner = col_factory->LookupColRef(colid2);

		GPOS_ASSERT(colref_outer != NULL);
		GPOS_ASSERT(colref_inner != NULL);

		IMDId *mdid_outer = colref_outer->GetMdidTable();
		IMDId *mdid_inner = colref_inner->GetMdidTable();
		IMdIdArray *mdid_pair = NULL;
		if ((mdid_outer != NULL) && (mdid_inner != NULL))
		{
			// there should only be two tables involved in a join condition
			// if the predicate is more complex (i.e. more than 2 tables involved in the predicate such as t1.a=t2.a+t3.a),
			// the mdid of the base table will be NULL:
			// Note that we hash on the pointer to the Mdid, not the value of the Mdid,
			// but we know that CColRef::GetMdidTable() will always return the same
			// pointer for a given table.
			mdid_pair = GPOS_NEW(mp) IMdIdArray(mp, 2);
			mdid_outer->AddRef();
			mdid_inner->AddRef();
			mdid_pair->Append(mdid_outer);
			mdid_pair->Append(mdid_inner);
			mdid_pair->Sort();
		}

		join_conds_scale_factors->Append(
			GPOS_NEW(mp) CScaleFactorUtils::SJoinCondition(local_scale_factor,
														   mdid_pair));
	}


	num_join_rows = CStatistics::MinRows;
	if (!output_is_empty)
	{
		num_join_rows = CalcJoinCardinality(
			mp, stats_config, outer_stats->Rows(), inner_side_stats->Rows(),
			join_conds_scale_factors, join_type);
	}

	// clean up
	join_conds_scale_factors->Release();
	join_colids->Release();

	UlongToDoubleMap *col_width_mapping_result = outer_stats->CopyWidths(mp);
	if (!semi_join)
	{
		inner_side_stats->CopyWidthsInto(mp, col_width_mapping_result);
	}

	// create an output stats object
	CStatistics *join_stats = GPOS_NEW(mp) CStatistics(
		mp, result_col_hist_mapping, col_width_mapping_result, num_join_rows,
		output_is_empty, outer_stats->GetNumberOfPredicates());

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated join cardinality.

	CStatisticsUtils::ComputeCardUpperBounds(
		mp, outer_stats, join_stats, num_join_rows,
		CStatistics::EcbmMin /* card_bounding_method */);
	if (!semi_join)
	{
		CStatisticsUtils::ComputeCardUpperBounds(
			mp, inner_side_stats, join_stats, num_join_rows,
			CStatistics::EcbmMin /* card_bounding_method */);
	}

	return join_stats;
}


// return join cardinality based on scaling factor and join type
CDouble
CJoinStatsProcessor::CalcJoinCardinality(
	CMemoryPool *mp, CStatisticsConfig *stats_config, CDouble left_num_rows,
	CDouble right_num_rows,
	CScaleFactorUtils::SJoinConditionArray *join_conds_scale_factors,
	IStatistics::EStatsJoinType join_type)
{
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != join_conds_scale_factors);

	CDouble scale_factor = CScaleFactorUtils::CumulativeJoinScaleFactor(
		mp, stats_config, join_conds_scale_factors);
	CDouble cartesian_product_num_rows = left_num_rows * right_num_rows;

	if (IStatistics::EsjtLeftAntiSemiJoin == join_type ||
		IStatistics::EsjtLeftSemiJoin == join_type)
	{
		CDouble rows = left_num_rows;

		if (IStatistics::EsjtLeftAntiSemiJoin == join_type)
		{
			rows = left_num_rows / scale_factor;
		}
		else
		{
			// semi join results cannot exceed size of outer side
			rows = std::min(left_num_rows.Get(),
							(cartesian_product_num_rows / scale_factor).Get());
		}

		return std::max(DOUBLE(1.0), rows.Get());
	}

	GPOS_ASSERT(CStatistics::MinRows <= scale_factor);

	return std::max(CStatistics::MinRows.Get(),
					(cartesian_product_num_rows / scale_factor).Get());
}



// check if the join statistics object is empty output based on the input
// histograms and the join histograms
BOOL
CJoinStatsProcessor::JoinStatsAreEmpty(BOOL outer_is_empty,
									   BOOL output_is_empty,
									   const CHistogram *outer_histogram,
									   const CHistogram *inner_histogram,
									   CHistogram *join_histogram,
									   IStatistics::EStatsJoinType join_type)
{
	GPOS_ASSERT(NULL != outer_histogram);
	GPOS_ASSERT(NULL != inner_histogram);
	GPOS_ASSERT(NULL != join_histogram);
	BOOL IsLASJ = IStatistics::EsjtLeftAntiSemiJoin == join_type;
	return output_is_empty || (!IsLASJ && outer_is_empty) ||
		   (!outer_histogram->IsEmpty() && !inner_histogram->IsEmpty() &&
			join_histogram->IsEmpty());
}

// Derive statistics for join operation given array of statistics object
IStatistics *
CJoinStatsProcessor::DeriveJoinStats(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 IStatisticsArray *stats_ctxt)
{
	GPOS_ASSERT(CLogical::EspNone <
				CLogical::PopConvert(exprhdl.Pop())->Esp(exprhdl));

	IStatisticsArray *statistics_array = GPOS_NEW(mp) IStatisticsArray(mp);
	const ULONG arity = exprhdl.Arity();
	for (ULONG i = 0; i < arity - 1; i++)
	{
		IStatistics *child_stats = exprhdl.Pstats(i);
		child_stats->AddRef();
		statistics_array->Append(child_stats);
	}

	CExpression *join_pred_expr = exprhdl.PexprScalarRepChild(arity - 1);

	join_pred_expr = CPredicateUtils::PexprRemoveImpliedConjuncts(
		mp, join_pred_expr, exprhdl);

	// split join predicate into local predicate and predicate involving outer references
	CExpression *local_expr = NULL;
	CExpression *expr_with_outer_refs = NULL;

	// get outer references from expression handle
	CColRefSet *outer_refs = exprhdl.DeriveOuterReferences();

	CPredicateUtils::SeparateOuterRefs(mp, join_pred_expr, outer_refs,
									   &local_expr, &expr_with_outer_refs);
	join_pred_expr->Release();

#ifdef GPOS_DEBUG
	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopLogicalLeftOuterJoin == op_id ||
				COperator::EopLogicalInnerJoin == op_id ||
				COperator::EopLogicalNAryJoin == op_id);
#endif

	// derive stats based on local join condition
	IStatistics *join_stats = CJoinStatsProcessor::CalcAllJoinStats(
		mp, statistics_array, local_expr, exprhdl.Pop());

	if (exprhdl.HasOuterRefs() && 0 < stats_ctxt->Size())
	{
		// derive stats based on outer references
		IStatistics *stats = DeriveStatsWithOuterRefs(
			mp, exprhdl, expr_with_outer_refs, join_stats, stats_ctxt);
		join_stats->Release();
		join_stats = stats;
	}

	local_expr->Release();
	expr_with_outer_refs->Release();

	statistics_array->Release();

	return join_stats;
}


// Derives statistics when the scalar expression contains one or more outer references.
// This stats derivation mechanism passes around a context array onto which
// operators append their stats objects as they get derived. The context array is
// filled as we derive stats on the children of a given operator. This gives each
// operator access to the stats objects of its previous siblings as well as to the outer
// operators in higher levels.
// For example, in this expression:
//
// JOIN
//   |--Get(R)
//   +--Select(R.r=S.s)
//       +-- Get(S)
//
// We start by deriving stats on JOIN's left child (Get(R)) and append its
// stats to the context. Then, we call stats derivation on JOIN's right child
// (SELECT), passing it the current context.  This gives SELECT access to the
// histogram on column R.r--which is an outer reference in this example. After
// JOIN's children's stats are computed, JOIN combines them into a parent stats
// object, which is passed upwards to JOIN's parent. This mechanism gives any
// operator access to the histograms of outer references defined anywhere in
// the logical tree. For example, we also support the case where outer
// reference R.r is defined two levels upwards:
//
//    JOIN
//      |---Get(R)
//      +--JOIN
//         |--Get(T)
//         +--Select(R.r=S.s)
//               +--Get(S)
//
//
//
// The next step is to combine the statistics objects of the outer references
// with those of the local columns. You can think of this as a correlated
// expression, where for each outer tuple, we need to extract the outer ref
// value and re-execute the inner expression using the current outer ref value.
// This has the same semantics as a Join from a statistics perspective.
//
// We pull statistics for outer references from the passed statistics context,
// using Join statistics derivation in this case.
//
// For example:
//
// 			Join
// 			 |--Get(R)
// 			 +--Join
// 				|--Get(S)
// 				+--Select(T.t=R.r)
// 					+--Get(T)
//
// when deriving statistics on 'Select(T.t=R.r)', we join T with the cross
// product (R x S) based on the condition (T.t=R.r)
IStatistics *
CJoinStatsProcessor::DeriveStatsWithOuterRefs(
	CMemoryPool *mp,
	CExpressionHandle &
		exprhdl,  // handle attached to the logical expression we want to derive stats for
	CExpression *expr,	 // scalar condition to be used for stats derivation
	IStatistics *stats,	 // statistics object of the attached expression
	IStatisticsArray *
		all_outer_stats	 // array of stats objects where outer references are defined
)
{
	GPOS_ASSERT(exprhdl.HasOuterRefs() &&
				"attached expression does not have outer references");
	GPOS_ASSERT(NULL != expr);
	GPOS_ASSERT(NULL != stats);
	GPOS_ASSERT(NULL != all_outer_stats);
	GPOS_ASSERT(0 < all_outer_stats->Size());

	// join outer stats object based on given scalar expression,
	// we use inner join semantics here to consider all relevant combinations of outer tuples
	IStatistics *outer_stats = CJoinStatsProcessor::CalcAllJoinStats(
		mp, all_outer_stats, expr, exprhdl.Pop());
	CDouble num_rows_outer = outer_stats->Rows();

	// join passed stats object and outer stats based on the passed join type
	IStatisticsArray *statistics_array = GPOS_NEW(mp) IStatisticsArray(mp);
	statistics_array->Append(outer_stats);
	stats->AddRef();
	statistics_array->Append(stats);
	IStatistics *result_join_stats = CJoinStatsProcessor::CalcAllJoinStats(
		mp, statistics_array, expr, exprhdl.Pop());
	statistics_array->Release();

	// scale result using cardinality of outer stats and set number of rebinds of returned stats
	IStatistics *result_stats =
		result_join_stats->ScaleStats(mp, CDouble(1.0 / num_rows_outer));
	result_stats->SetRebinds(num_rows_outer);
	result_join_stats->Release();

	return result_stats;
}