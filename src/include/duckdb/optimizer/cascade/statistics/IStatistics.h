//---------------------------------------------------------------------------
//	@filename:
//		IStatistics.h
//
//	@doc:
//		Abstract statistics API
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IStatistics_H
#define GPNAUCRATES_IStatistics_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CBitSet.h"
#include "duckdb/optimizer/cascade/common/CHashMapIter.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/statistics/CHistogram.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPred.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredJoin.h"
#include "duckdb/optimizer/cascade/statistics/CStatsPredPoint.h"

namespace gpopt
{
class CMDAccessor;
class CReqdPropRelational;
class CColRefSet;
}  // namespace gpopt

namespace gpnaucrates
{
using namespace gpos;
using namespace gpmd;
using namespace gpopt;

// fwd declarations
class IStatistics;

// hash map from column id to a histogram
typedef CHashMap<ULONG, CHistogram, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
				 CleanupDelete<ULONG>, CleanupDelete<CHistogram> >
	UlongToHistogramMap;

// iterator
typedef CHashMapIter<ULONG, CHistogram, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupDelete<CHistogram> >
	UlongToHistogramMapIter;

// hash map from column ULONG to CDouble
typedef CHashMap<ULONG, CDouble, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
				 CleanupDelete<ULONG>, CleanupDelete<CDouble> >
	UlongToDoubleMap;

// iterator
typedef CHashMapIter<ULONG, CDouble, gpos::HashValue<ULONG>,
					 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
					 CleanupDelete<CDouble> >
	UlongToDoubleMapIter;

typedef CHashMap<ULONG, ULONG, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
				 CleanupDelete<ULONG>, CleanupDelete<ULONG> >
	UlongToUlongMap;

// hash maps mapping INT -> ULONG
typedef CHashMap<INT, ULONG, gpos::HashValue<INT>, gpos::Equals<INT>,
				 CleanupDelete<INT>, CleanupDelete<ULONG> >
	IntToUlongMap;

//---------------------------------------------------------------------------
//	@class:
//		IStatistics
//
//	@doc:
//		Abstract statistics API
//
//---------------------------------------------------------------------------
class IStatistics : public CRefCount
{
private:
	// private copy ctor
	IStatistics(const IStatistics &);

	// private assignment operator
	IStatistics &operator=(IStatistics &);

public:
	enum EStatsJoinType
	{
		EsjtInnerJoin,
		EsjtLeftOuterJoin,
		EsjtLeftSemiJoin,
		EsjtLeftAntiSemiJoin,
		EstiSentinel  // should be the last in this enum
	};

	// ctor
	IStatistics()
	{
	}

	// dtor
	virtual ~IStatistics()
	{
	}

	// how many rows
	virtual CDouble Rows() const = 0;

	// is statistics on an empty input
	virtual BOOL IsEmpty() const = 0;

	// statistics could be computed using predicates with external parameters (outer references),
	// this is the total number of external parameters' values
	virtual CDouble NumRebinds() const = 0;

	// skew estimate for given column
	virtual CDouble GetSkew(ULONG colid) const = 0;

	// what is the width in bytes
	virtual CDouble Width() const = 0;

	// what is the width in bytes of set of column id's
	virtual CDouble Width(ULongPtrArray *colids) const = 0;

	// what is the width in bytes of set of column references
	virtual CDouble Width(CMemoryPool *mp, CColRefSet *colrefs) const = 0;

	// the risk of errors in cardinality estimation
	virtual ULONG StatsEstimationRisk() const = 0;

	// update the risk of errors in cardinality estimation
	virtual void SetStatsEstimationRisk(ULONG risk) = 0;

	// look up the number of distinct values of a particular column
	virtual CDouble GetNDVs(const CColRef *colref) = 0;

	virtual ULONG GetNumberOfPredicates() const = 0;

	// inner join with another stats structure
	virtual IStatistics *CalcInnerJoinStats(
		CMemoryPool *mp, const IStatistics *other_stats,
		CStatsPredJoinArray *join_preds_stats) const = 0;

	// LOJ with another stats structure
	virtual IStatistics *CalcLOJoinStats(
		CMemoryPool *mp, const IStatistics *other_stats,
		CStatsPredJoinArray *join_preds_stats) const = 0;

	// semi join stats computation
	virtual IStatistics *CalcLSJoinStats(
		CMemoryPool *mp, const IStatistics *inner_side_stats,
		CStatsPredJoinArray *join_preds_stats) const = 0;

	// anti semi join
	virtual IStatistics *CalcLASJoinStats(
		CMemoryPool *mp, const IStatistics *other_stats,
		CStatsPredJoinArray *join_preds_stats,
		BOOL DoIgnoreLASJHistComputation) const = 0;

	// return required props associated with stats object
	virtual CReqdPropRelational *GetReqdRelationalProps(
		CMemoryPool *mp) const = 0;

	// append given stats to current object
	virtual void AppendStats(CMemoryPool *mp, IStatistics *stats) = 0;

	// set number of rebinds
	virtual void SetRebinds(CDouble num_rebinds) = 0;

	// copy stats
	virtual IStatistics *CopyStats(CMemoryPool *mp) const = 0;

	// return a copy of this stats object scaled by a given factor
	virtual IStatistics *ScaleStats(CMemoryPool *mp, CDouble factor) const = 0;

	// copy stats with remapped column ids
	virtual IStatistics *CopyStatsWithRemap(CMemoryPool *mp,
											UlongToColRefMap *colref_mapping,
											BOOL must_exist = true) const = 0;

	// return a set of column references we have stats for
	virtual CColRefSet *GetColRefSet(CMemoryPool *mp) const = 0;

	// print function
	virtual IOstream &OsPrint(IOstream &os) const = 0;

	// is the join type either a left semi join or left anti-semi join
	static BOOL
	IsSemiJoin(IStatistics::EStatsJoinType join_type)
	{
		return (IStatistics::EsjtLeftAntiSemiJoin == join_type) ||
			   (IStatistics::EsjtLeftSemiJoin == join_type);
	}
};	// class IStatistics

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, IStatistics &stats)
{
	return stats.OsPrint(os);
}
// release istats
inline void
CleanupStats(IStatistics *stats)
{
	if (NULL != stats)
	{
		(dynamic_cast<CRefCount *>(stats))->Release();
	}
}

// dynamic array for derived stats
typedef CDynamicPtrArray<IStatistics, CleanupStats> IStatisticsArray;
}  // namespace gpnaucrates

#endif
