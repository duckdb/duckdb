//---------------------------------------------------------------------------
//	@filename:
//		CBucket.h
//
//	@doc:
//		Bucket in a histogram
//---------------------------------------------------------------------------

#ifndef GPNAUCRATES_CBucket_H
#define GPNAUCRATES_CBucket_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/task/CTask.h"

#include "duckdb/optimizer/cascade/statistics/CPoint.h"
#include "duckdb/optimizer/cascade/statistics/IBucket.h"

#define GPOPT_BUCKET_DEFAULT_FREQ 1.0
#define GPOPT_BUCKET_DEFAULT_DISTINCT 1.0

namespace gpnaucrates
{
using namespace gpos;
using namespace gpmd;

// forward decl
class CBucket;

// dynamic array of buckets
typedef CDynamicPtrArray<CBucket, CleanupDelete> CBucketArray;

//---------------------------------------------------------------------------
//	@class:
//		CBucket
//
//	@doc:
//		Represents a bucket in a histogram
//
//---------------------------------------------------------------------------

class CBucket : public IBucket
{
private:
	// lower bound of bucket
	CPoint *m_bucket_lower_bound;

	// upper bound of bucket
	CPoint *m_bucket_upper_bound;

	// is lower bound closed (does bucket include boundary value)
	BOOL m_is_lower_closed;

	// is upper bound closed (does bucket includes boundary value)
	BOOL m_is_upper_closed;

	// frequency corresponding to bucket
	CDouble m_frequency;

	// number of distinct elements in bucket
	CDouble m_distinct;

	// private copy constructor
	CBucket(const CBucket &);

	// private assignment operator
	CBucket &operator=(const CBucket &);

public:
	// ctor
	CBucket(CPoint *bucket_lower_bound, CPoint *bucket_upper_bound,
			BOOL is_lower_closed, BOOL is_upper_closed, CDouble frequency,
			CDouble distinct);

	// dtor
	virtual ~CBucket();

	// does bucket contain point
	BOOL Contains(const CPoint *point) const;

	// is the point before the lower bound of the bucket
	BOOL IsBefore(const CPoint *point) const;

	// is the point after the upper bound of the bucket
	BOOL IsAfter(const CPoint *point) const;

	// what percentage of bucket is covered by [lb,pp]
	CDouble GetOverlapPercentage(const CPoint *point) const;

	// frequency associated with bucket
	CDouble
	GetFrequency() const
	{
		return m_frequency;
	}

	// width of bucket
	CDouble Width() const;

	// set frequency
	void
	SetFrequency(CDouble frequency)
	{
		GPOS_ASSERT(CDouble(0) <= frequency);
		GPOS_ASSERT(CDouble(1.0) >= frequency);
		m_frequency = frequency;
	}

	// set number of distinct values
	void
	SetDistinct(CDouble distinct)
	{
		GPOS_ASSERT(CDouble(0) <= distinct);
		m_distinct = distinct;
	}

	// number of distinct values in bucket
	CDouble
	GetNumDistinct() const
	{
		return m_distinct;
	}

	// lower point
	CPoint *
	GetLowerBound() const
	{
		return m_bucket_lower_bound;
	}

	// upper point
	CPoint *
	GetUpperBound() const
	{
		return m_bucket_upper_bound;
	}

	// is lower bound closed (does bucket includes boundary value)
	BOOL
	IsLowerClosed() const
	{
		return m_is_lower_closed;
	}

	// is upper bound closed (does bucket includes boundary value)
	BOOL
	IsUpperClosed() const
	{
		return m_is_upper_closed;
	}

	// does bucket's range intersect another's
	BOOL Intersects(const CBucket *bucket) const;

	// does bucket's range subsume another's
	BOOL Subsumes(const CBucket *bucket) const;

	// does bucket occur before another
	BOOL IsBefore(const CBucket *bucket) const;

	// does bucket occur after another
	BOOL IsAfter(const CBucket *bucket) const;

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

#ifdef GPOS_DEBUG
	void DbgPrint() const;
#endif

	// construct new bucket with lower bound greater than given point
	CBucket *MakeBucketGreaterThan(CMemoryPool *mp, CPoint *point) const;

	// scale down version of bucket adjusting upper boundary
	CBucket *MakeBucketScaleUpper(CMemoryPool *mp, CPoint *bucket_upper_bound,
								  BOOL include_upper) const;

	// scale down version of bucket adjusting lower boundary
	CBucket *MakeBucketScaleLower(CMemoryPool *mp, CPoint *bucket_lower_bound,
								  BOOL include_lower) const;

	// extract singleton bucket at given point
	CBucket *MakeBucketSingleton(CMemoryPool *mp,
								 CPoint *point_singleton) const;

	// create a new bucket by intersecting with another and return the percentage of each of the buckets that intersect
	CBucket *MakeBucketIntersect(CMemoryPool *mp, CBucket *bucket,
								 CDouble *result_freq_intersect1,
								 CDouble *result_freq_intersect2) const;

	// Remove a bucket range. This produces lower and upper split
	void Difference(CMemoryPool *mp, CBucket *bucket_other,
					CBucket **result_bucket_lower,
					CBucket **result_bucket_upper);

	// return copy of bucket
	CBucket *MakeBucketCopy(CMemoryPool *mp);

	// return a copy of the bucket with updated frequency based on the new total number of rows
	CBucket *MakeBucketUpdateFrequency(CMemoryPool *mp, CDouble rows_old,
									   CDouble rows_new);

	// Merge with another bucket and return leftovers
	CBucket *MakeBucketMerged(CMemoryPool *mp, CBucket *bucket_other,
							  CDouble rows, CDouble rows_other,
							  CBucket **result_bucket1_new,
							  CBucket **result_bucket2_new,
							  BOOL is_union_all = true);

	// does bucket support sampling
	BOOL
	CanSample() const
	{
		return m_bucket_lower_bound->GetDatum()->StatsMappable();
	}

	// generate a random data point within bucket boundaries
	CDouble GetSample(ULONG *seed) const;

	// compare lower bucket boundaries
	static INT CompareLowerBounds(const CBucket *bucket1,
								  const CBucket *bucket2);

	// compare upper bucket boundaries
	static INT CompareUpperBounds(const CBucket *bucket1,
								  const CBucket *bucket2);

	// compare lower bound of first bucket to upper bound of second bucket
	static INT CompareLowerBoundToUpperBound(const CBucket *bucket1,
											 const CBucket *bucket2);

	// create a new singleton bucket with the given datum as it lower and upper bounds
	static CBucket *MakeBucketSingleton(CMemoryPool *mp, IDatum *datum);
};
}  // namespace gpnaucrates

#endif
