//---------------------------------------------------------------------------
//	@filename:
//		CScaleFactorUtils.h
//
//	@doc:
//		Utility functions for computing scale factors used in stats computation
//---------------------------------------------------------------------------
#ifndef GPOPT_CScaleFactorUtils_H
#define GPOPT_CScaleFactorUtils_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/engine/CStatisticsConfig.h"
#include "duckdb/optimizer/cascade/statistics/CHistogram.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

namespace gpnaucrates
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScaleFactorUtils
//
//	@doc:
//		Utility functions for computing scale factors used in stats computation
//
//---------------------------------------------------------------------------
class CScaleFactorUtils
{
public:
	struct SJoinCondition
	{
		// scale factor
		CDouble m_scale_factor;

		// mdid pair for the predicate
		IMdIdArray *m_oid_pair;

		//ctor
		SJoinCondition(CDouble scale_factor, IMdIdArray *mdid_pair)
			: m_scale_factor(scale_factor), m_oid_pair(mdid_pair)
		{
		}

		~SJoinCondition()
		{
			CRefCount::SafeRelease(m_oid_pair);
		}

		// We hash/compare the pointer rather than the contents of the IMdId, as we want to discern between different instances of IMdIds.
		// For example, when performing a self join, the underlying tables will have different IMdId pointers, but the same contents.
		// We treat them as different instances and assume independence to calculate the correct join cardinality.
		static ULONG
		HashValue(const IMdIdArray *oid_pair)
		{
			return CombineHashes(gpos::HashPtr<IMDId>((*oid_pair)[0]),
								 gpos::HashPtr<IMDId>((*oid_pair)[1]));
		}

		static BOOL
		Equals(const IMdIdArray *first, const IMdIdArray *second)
		{
			return ((*first)[0] == (*second)[0]) &&
				   ((*first)[1] == (*second)[1]);
		}
	};

	typedef CDynamicPtrArray<SJoinCondition, CleanupDelete> SJoinConditionArray;

	typedef CHashMap<IMdIdArray, CDoubleArray, SJoinCondition::HashValue,
					 SJoinCondition::Equals, CleanupRelease<IMdIdArray>,
					 CleanupRelease<CDoubleArray> >
		OIDPairToScaleFactorArrayMap;

	typedef CHashMapIter<IMdIdArray, CDoubleArray, SJoinCondition::HashValue,
						 SJoinCondition::Equals, CleanupRelease<IMdIdArray>,
						 CleanupRelease<CDoubleArray> >
		OIDPairToScaleFactorArrayMapIter;

	// generate the hashmap of scale factors grouped by pred tables, also produces array of complex join preds
	static OIDPairToScaleFactorArrayMap *GenerateScaleFactorMap(
		CMemoryPool *mp, SJoinConditionArray *join_conds_scale_factors,
		CDoubleArray *complex_join_preds);

	// generate a cumulative scale factor using a modified sqrt algorithm
	static CDouble CalcCumulativeScaleFactorSqrtAlg(
		OIDPairToScaleFactorArrayMap *scale_factor_hashmap,
		CDoubleArray *complex_join_preds);

	// calculate the cumulative join scaling factor
	static CDouble CumulativeJoinScaleFactor(
		CMemoryPool *mp, const CStatisticsConfig *stats_config,
		SJoinConditionArray *join_conds_scale_factors);

	// return scaling factor of the join predicate after apply damping
	static CDouble DampedJoinScaleFactor(const CStatisticsConfig *stats_config,
										 ULONG num_columns);

	// return scaling factor of the filter after apply damping
	static CDouble DampedFilterScaleFactor(
		const CStatisticsConfig *stats_config, ULONG num_columns);

	// return scaling factor of the group by predicate after apply damping
	static CDouble DampedGroupByScaleFactor(
		const CStatisticsConfig *stats_config, ULONG num_columns);

	// sort the array of scaling factor
	static void SortScalingFactor(CDoubleArray *scale_factors,
								  BOOL is_descending);

	// calculate the cumulative scaling factor for conjunction after applying damping multiplier
	static CDouble CalcScaleFactorCumulativeConj(
		const CStatisticsConfig *stats_config, CDoubleArray *scale_factors);

	// calculate the cumulative scaling factor for disjunction after applying damping multiplier
	static CDouble CalcScaleFactorCumulativeDisj(
		const CStatisticsConfig *stats_config, CDoubleArray *scale_factors,
		CDouble tota_rows);

	// comparison function in descending order
	static INT DescendingOrderCmpFunc(const void *val1, const void *val2);

	// comparison function for joins in descending order
	static INT DescendingOrderCmpJoinFunc(const void *val1, const void *val2);

	// comparison function in ascending order
	static INT AscendingOrderCmpFunc(const void *val1, const void *val2);

	// helper function for double comparison
	static INT DoubleCmpFunc(const CDouble *double_data1,
							 const CDouble *double_data2, BOOL is_descending);

	// default scaling factor of LIKE predicate
	static const CDouble DDefaultScaleFactorLike;

	// default scaling factor of join predicate
	static const CDouble DefaultJoinPredScaleFactor;

	// default scaling factor of non-equality (<, <=, >=, <=) join predicate
	// Note: scale factor of InEquality (!= also denoted as <>) is computed from scale factor of equi-join
	static const CDouble DefaultInequalityJoinPredScaleFactor;

	// invalid scale factor
	static const CDouble InvalidScaleFactor;
};	// class CScaleFactorUtils
}  // namespace gpnaucrates

#endif