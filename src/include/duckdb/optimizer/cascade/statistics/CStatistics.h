//---------------------------------------------------------------------------
//	@filename:
//		CStatistics.h
//
//	@doc:
//		Statistics implementation over 1D histograms
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatistics_H
#define GPNAUCRATES_CStatistics_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CBitSet.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"

#include "duckdb/optimizer/cascade/statistics/CHistogram.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredArrayCmp.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredConj.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredDisj.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredLike.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredUnsupported.h"
#include "duckdb/optimizer/cascade/statistics/CUpperBoundNDVs.h"
#include "duckdb/optimizer/cascade/statistics/IStatistics.h"

namespace gpopt
{
class CStatisticsConfig;
class CColumnFactory;
}  // namespace gpopt

namespace gpnaucrates
{
using namespace gpos;
using namespace gpdxl;
using namespace gpmd;
using namespace gpopt;

// hash maps ULONG -> array of ULONGs
typedef CHashMap<ULONG, ULongPtrArray, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
				 CleanupRelease<ULongPtrArray> >
	UlongToUlongPtrArrayMap;

// iterator
typedef CHashMapIter<ULONG, ULongPtrArray, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupRelease<ULongPtrArray> >
	UlongToUlongPtrArrayMapIter;

//---------------------------------------------------------------------------
//	@class:
//		CStatistics
//
//	@doc:
//		Abstract statistics API
//---------------------------------------------------------------------------
class CStatistics : public IStatistics
{
public:
	// method used to compute for columns of each source it corresponding
	// the cardinality upper bound
	enum ECardBoundingMethod
	{
		EcbmOutputCard =
			0,	// use the estimated output cardinality as the upper bound cardinality of the source
		EcbmInputSourceMaxCard,	 // use the upper bound cardinality of the source in the input statistics object
		EcbmMin,  // use the minimum of the above two cardinality estimates

		EcbmSentinel
	};

	// helper method to copy stats on columns that are not excluded by bitset
	void AddNotExcludedHistograms(
		CMemoryPool *mp, CBitSet *excluded_cols,
		UlongToHistogramMap *col_histogram_mapping) const;

private:
	// private copy ctor
	CStatistics(const CStatistics &);

	// private assignment operator
	CStatistics &operator=(CStatistics &);

	// hashmap from column ids to histograms
	UlongToHistogramMap *m_colid_histogram_mapping;

	// hashmap from column id to width
	UlongToDoubleMap *m_colid_width_mapping;

	// number of rows
	CDouble m_rows;

	// the risk to have errors in cardinality estimation; it goes from 1 to infinity,
	// where 1 is no risk
	// when going from the leaves to the root of the plan, operators that generate joins,
	// selections and groups increment the risk
	ULONG m_stats_estimation_risk;

	// flag to indicate if input relation is empty
	BOOL m_empty;

	// statistics could be computed using predicates with external parameters (outer references),
	// this is the total number of external parameters' values
	CDouble m_num_rebinds;

	// number of predicates applied
	ULONG m_num_predicates;

	// statistics configuration
	CStatisticsConfig *m_stats_conf;

	// array of upper bound of ndv per source;
	// source can be one of the following operators: like Get, Group By, and Project
	CUpperBoundNDVPtrArray *m_src_upper_bound_NDVs;

	// the default value for operators that have no cardinality estimation risk
	static const ULONG no_card_est_risk_default_val;

	// helper method to add histograms where the column ids have been remapped
	static void AddHistogramsWithRemap(CMemoryPool *mp,
									   UlongToHistogramMap *src_histograms,
									   UlongToHistogramMap *dest_histograms,
									   UlongToColRefMap *colref_mapping,
									   BOOL must_exist);

	// helper method to add width information where the column ids have been remapped
	static void AddWidthInfoWithRemap(CMemoryPool *mp,
									  UlongToDoubleMap *src_width,
									  UlongToDoubleMap *dest_width,
									  UlongToColRefMap *colref_mapping,
									  BOOL must_exist);

public:
	// ctor
	CStatistics(CMemoryPool *mp, UlongToHistogramMap *col_histogram_mapping,
				UlongToDoubleMap *colid_width_mapping, CDouble rows,
				BOOL is_empty, ULONG num_predicates = 0);

	// dtor
	virtual ~CStatistics();

	virtual UlongToDoubleMap *CopyWidths(CMemoryPool *mp) const;

	virtual void CopyWidthsInto(CMemoryPool *mp,
								UlongToDoubleMap *colid_width_mapping) const;

	virtual UlongToHistogramMap *CopyHistograms(CMemoryPool *mp) const;

	// actual number of rows
	virtual CDouble Rows() const;

	// number of rebinds
	virtual CDouble
	NumRebinds() const
	{
		return m_num_rebinds;
	}

	// skew estimate for given column
	virtual CDouble GetSkew(ULONG colid) const;

	// what is the width in bytes of set of column id's
	virtual CDouble Width(ULongPtrArray *colids) const;

	// what is the width in bytes of set of column references
	virtual CDouble Width(CMemoryPool *mp, CColRefSet *colrefs) const;

	// what is the width in bytes
	virtual CDouble Width() const;

	// is statistics on an empty input
	virtual BOOL
	IsEmpty() const
	{
		return m_empty;
	}

	// look up the histogram of a particular column
	virtual const CHistogram *
	GetHistogram(ULONG colid) const
	{
		return m_colid_histogram_mapping->Find(&colid);
	}

	// look up the number of distinct values of a particular column
	virtual CDouble GetNDVs(const CColRef *colref);

	// look up the width of a particular column
	virtual const CDouble *GetWidth(ULONG colid) const;

	// the risk of errors in cardinality estimation
	virtual ULONG
	StatsEstimationRisk() const
	{
		return m_stats_estimation_risk;
	}

	// update the risk of errors in cardinality estimation
	virtual void
	SetStatsEstimationRisk(ULONG risk)
	{
		m_stats_estimation_risk = risk;
	}

	// inner join with another stats structure
	virtual CStatistics *CalcInnerJoinStats(
		CMemoryPool *mp, const IStatistics *other_stats,
		CStatsPredJoinArray *join_preds_stats) const;

	// LOJ with another stats structure
	virtual CStatistics *CalcLOJoinStats(
		CMemoryPool *mp, const IStatistics *other_stats,
		CStatsPredJoinArray *join_preds_stats) const;

	// left anti semi join with another stats structure
	virtual CStatistics *CalcLASJoinStats(
		CMemoryPool *mp, const IStatistics *other_stats,
		CStatsPredJoinArray *join_preds_stats,
		BOOL
			DoIgnoreLASJHistComputation	 // except for the case of LOJ cardinality estimation this flag is always
		// "true" since LASJ stats computation is very aggressive
	) const;

	// semi join stats computation
	virtual CStatistics *CalcLSJoinStats(
		CMemoryPool *mp, const IStatistics *inner_side_stats,
		CStatsPredJoinArray *join_preds_stats) const;

	// return required props associated with stats object
	virtual CReqdPropRelational *GetReqdRelationalProps(CMemoryPool *mp) const;

	// append given stats to current object
	virtual void AppendStats(CMemoryPool *mp, IStatistics *stats);

	// set number of rebinds
	virtual void
	SetRebinds(CDouble num_rebinds)
	{
		GPOS_ASSERT(0.0 < num_rebinds);

		m_num_rebinds = num_rebinds;
	}

	// copy stats
	virtual IStatistics *CopyStats(CMemoryPool *mp) const;

	// return a copy of this stats object scaled by a given factor
	virtual IStatistics *ScaleStats(CMemoryPool *mp, CDouble factor) const;

	// copy stats with remapped column id
	virtual IStatistics *CopyStatsWithRemap(CMemoryPool *mp,
											UlongToColRefMap *colref_mapping,
											BOOL must_exist) const;

	// return the set of column references we have stats for
	virtual CColRefSet *GetColRefSet(CMemoryPool *mp) const;

	// generate the DXL representation of the statistics object
	virtual CDXLStatsDerivedRelation *GetDxlStatsDrvdRelation(
		CMemoryPool *mp, CMDAccessor *md_accessor) const;

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

	// add upper bound of source cardinality
	virtual void AddCardUpperBound(CUpperBoundNDVs *upper_bound_NDVs);

	// return the upper bound of the number of distinct values for a given column
	virtual CDouble GetColUpperBoundNDVs(const CColRef *colref);

	// return the index of the array of upper bound ndvs to which column reference belongs
	virtual ULONG GetIndexUpperBoundNDVs(const CColRef *colref);

	// return the column identifiers of all columns statistics maintained
	virtual ULongPtrArray *GetColIdsWithStats(CMemoryPool *mp) const;

	virtual ULONG
	GetNumberOfPredicates() const
	{
		return m_num_predicates;
	}

	CStatisticsConfig *
	GetStatsConfig() const
	{
		return m_stats_conf;
	}

	CUpperBoundNDVPtrArray *
	GetUpperBoundNDVs() const
	{
		return m_src_upper_bound_NDVs;
	}
	// create an empty statistics object
	static CStatistics *
	MakeEmptyStats(CMemoryPool *mp)
	{
		ULongPtrArray *colids = GPOS_NEW(mp) ULongPtrArray(mp);
		CStatistics *stats = MakeDummyStats(mp, colids, DefaultRelationRows);

		// clean up
		colids->Release();

		return stats;
	}

	// conversion function
	static CStatistics *
	CastStats(IStatistics *pstats)
	{
		GPOS_ASSERT(NULL != pstats);
		return dynamic_cast<CStatistics *>(pstats);
	}

	// create a dummy statistics object
	static CStatistics *MakeDummyStats(CMemoryPool *mp, ULongPtrArray *colids,
									   CDouble rows);

	// create a dummy statistics object
	static CStatistics *MakeDummyStats(CMemoryPool *mp,
									   ULongPtrArray *col_histogram_mapping,
									   ULongPtrArray *colid_width_mapping,
									   CDouble rows);

	// default column width
	static const CDouble DefaultColumnWidth;

	// default number of rows in relation
	static const CDouble DefaultRelationRows;

	// minimum number of rows in relation
	static const CDouble MinRows;

	// epsilon
	static const CDouble Epsilon;

	// default number of distinct values
	static const CDouble DefaultDistinctValues;

	// check if the input statistics from join statistics computation empty
	static BOOL IsEmptyJoin(const CStatistics *outer_stats,
							const CStatistics *inner_side_stats, BOOL IsLASJ);

	// add upper bound ndvs information for a given set of columns
	static void CreateAndInsertUpperBoundNDVs(CMemoryPool *mp,
											  CStatistics *stats,
											  ULongPtrArray *colids,
											  CDouble rows);

	// cap the total number of distinct values (NDV) in buckets to the number of rows
	static void CapNDVs(CDouble rows,
						UlongToHistogramMap *col_histogram_mapping);
};	// class CStatistics

}  // namespace gpnaucrates

#endif
