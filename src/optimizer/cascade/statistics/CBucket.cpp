//---------------------------------------------------------------------------
//	@filename:
//		CBucket.cpp
//
//	@doc:
//		Implementation of histogram bucket
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CBucket.h"
#include <stdlib.h>
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/IDatum.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"
#include "duckdb/optimizer/cascade/statistics/CStatisticsUtils.h"

using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CBucket::CBucket
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CBucket::CBucket(CPoint *bucket_lower_bound, CPoint *bucket_upper_bound,
				 BOOL is_lower_closed, BOOL is_upper_closed, CDouble frequency,
				 CDouble distinct)
	: m_bucket_lower_bound(bucket_lower_bound),
	  m_bucket_upper_bound(bucket_upper_bound),
	  m_is_lower_closed(is_lower_closed),
	  m_is_upper_closed(is_upper_closed),
	  m_frequency(frequency),
	  m_distinct(distinct)
{
	GPOS_ASSERT(NULL != m_bucket_lower_bound);
	GPOS_ASSERT(NULL != m_bucket_upper_bound);
	GPOS_ASSERT(0.0 <= m_frequency && 1.0 >= m_frequency);
	GPOS_ASSERT(0.0 <= m_distinct);

	// singleton bucket lower and upper bound are closed
	GPOS_ASSERT_IMP(IsSingleton(), is_lower_closed && is_upper_closed);

	// null values should be in null fraction of the histogram
	GPOS_ASSERT(!m_bucket_lower_bound->GetDatum()->IsNull());
	GPOS_ASSERT(!m_bucket_upper_bound->GetDatum()->IsNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::~CBucket
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CBucket::~CBucket()
{
	m_bucket_lower_bound->Release();
	m_bucket_lower_bound = NULL;
	m_bucket_upper_bound->Release();
	m_bucket_upper_bound = NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::Contains
//
//	@doc:
//		Does bucket contain the point?
//
//---------------------------------------------------------------------------
BOOL
CBucket::Contains(const CPoint *point) const
{
	// special case for singleton bucket
	if (IsSingleton())
	{
		return m_bucket_lower_bound->Equals(point);
	}

	// special case if point equal to lower bound
	if (m_is_lower_closed && m_bucket_lower_bound->Equals(point))
	{
		return true;
	}

	// special case if point equal to upper bound
	if (m_is_upper_closed && m_bucket_upper_bound->Equals(point))
	{
		return true;
	}

	return m_bucket_lower_bound->IsLessThan(point) &&
		   m_bucket_upper_bound->IsGreaterThan(point);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::IsBefore
//
//	@doc:
//		Is the point before the lower bound of the bucket
//
//---------------------------------------------------------------------------
BOOL
CBucket::IsBefore(const CPoint *point) const
{
	GPOS_ASSERT(NULL != point);

	return (m_is_lower_closed && m_bucket_lower_bound->IsGreaterThan(point)) ||
		   (!m_is_lower_closed &&
			m_bucket_lower_bound->IsGreaterThanOrEqual(point));
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::IsAfter
//
//	@doc:
//		Is the point after the upper bound of the bucket
//
//---------------------------------------------------------------------------
BOOL
CBucket::IsAfter(const CPoint *point) const
{
	GPOS_ASSERT(NULL != point);

	return (
		(m_is_upper_closed && m_bucket_upper_bound->IsLessThan(point)) ||
		(!m_is_upper_closed && m_bucket_upper_bound->IsLessThanOrEqual(point)));
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::GetOverlapPercentage
//
//	@doc:
//		What percentage of the bucket is covered by [lower bound, point]
//
//---------------------------------------------------------------------------
CDouble
CBucket::GetOverlapPercentage(const CPoint *point) const
{
	// special case of upper bound equal to point
	if (this->GetUpperBound()->IsLessThanOrEqual(point))
	{
		return CDouble(1.0);
	}
	// if point is not contained, then no overlap
	if (!this->Contains(point))
	{
		return CDouble(0.0);
	}

	// special case for singleton bucket
	if (IsSingleton())
	{
		GPOS_ASSERT(this->m_bucket_lower_bound->Equals(point));

		return CDouble(1.0);
	}

	// general case, compute distance ratio
	CDouble distance_upper =
		m_bucket_upper_bound->Distance(m_bucket_lower_bound);
	GPOS_ASSERT(distance_upper > 0.0);
	CDouble distance_middle = point->Distance(m_bucket_lower_bound);
	GPOS_ASSERT(distance_middle >= 0.0);

	CDouble res = 1 / distance_upper;
	if (distance_middle > 0.0)
	{
		res = res * distance_middle;
	}

	return CDouble(std::min(res.Get(), DOUBLE(1.0)));
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CBucket::OsPrint(IOstream &os) const
{
	os << "CBucket(";

	if (m_is_lower_closed)
	{
		os << " [";
	}
	else
	{
		os << " (";
	}

	m_bucket_lower_bound->OsPrint(os);
	os << ", ";
	m_bucket_upper_bound->OsPrint(os);

	if (m_is_upper_closed)
	{
		os << "]";
	}
	else
	{
		os << ")";
	}

	os << " ";
	os << m_frequency << ", " << m_distinct;
	os << ")";

	return os;
}

#ifdef GPOS_DEBUG
void
CBucket::DbgPrint() const
{
	CAutoTrace at(CTask::Self()->Pmp());
	OsPrint(at.Os());
}
#endif

//---------------------------------------------------------------------------
//	@function:
//		CBucket::MakeBucketGreaterThan
//
//	@doc:
//		Construct new bucket with lower bound greater than given point and
//		the new bucket's upper bound is the upper bound of the current bucket
//---------------------------------------------------------------------------
CBucket *
CBucket::MakeBucketGreaterThan(CMemoryPool *mp, CPoint *point) const
{
	GPOS_ASSERT(Contains(point));

	if (IsSingleton() || GetUpperBound()->Equals(point))
	{
		return NULL;
	}

	CBucket *result_bucket = NULL;
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	CPoint *point_new = CStatisticsUtils::NextPoint(mp, md_accessor, point);

	if (NULL != point_new)
	{
		if (Contains(point_new))
		{
			result_bucket =
				MakeBucketScaleLower(mp, point_new, true /* include_lower */);
		}
		point_new->Release();
	}
	else
	{
		result_bucket =
			MakeBucketScaleLower(mp, point, false /* include_lower */);
	}

	return result_bucket;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::MakeBucketScaleUpper
//
//	@doc:
//		Create a new bucket that is a scaled down version
//		of this bucket with the upper boundary adjusted.
//
//---------------------------------------------------------------------------
CBucket *
CBucket::MakeBucketScaleUpper(CMemoryPool *mp, CPoint *point_upper_new,
							  BOOL include_upper) const
{
	GPOS_ASSERT(mp);
	GPOS_ASSERT(point_upper_new);

	GPOS_ASSERT(this->Contains(point_upper_new));

	// scaling upper to be same as lower is identical to producing a singleton bucket
	if (this->m_bucket_lower_bound->Equals(point_upper_new))
	{
		// invalid bucket, e.g. if bucket is [5,10) and
		// point_upper_new is 5 open, null should be returned
		if (false == include_upper)
		{
			return NULL;
		}
		return MakeBucketSingleton(mp, point_upper_new);
	}

	CDouble frequency_new = this->GetFrequency();
	CDouble distinct_new = this->GetNumDistinct();

	if (!this->m_bucket_upper_bound->Equals(point_upper_new))
	{
		CDouble overlap = this->GetOverlapPercentage(point_upper_new);
		frequency_new = frequency_new * overlap;
		distinct_new = distinct_new * overlap;
	}


	// reuse the lower from this bucket
	this->m_bucket_lower_bound->AddRef();
	point_upper_new->AddRef();

	return GPOS_NEW(mp) CBucket(this->m_bucket_lower_bound, point_upper_new,
								this->m_is_lower_closed, include_upper,
								frequency_new, distinct_new);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::MakeBucketScaleLower
//
//	@doc:
//		Create a new bucket that is a scaled down version
//		of this bucket with the Lower boundary adjusted
//
//---------------------------------------------------------------------------
CBucket *
CBucket::MakeBucketScaleLower(CMemoryPool *mp, CPoint *point_lower_new,
							  BOOL include_lower) const
{
	GPOS_ASSERT(mp);
	GPOS_ASSERT(point_lower_new);

	GPOS_ASSERT(this->Contains(point_lower_new));

	// scaling lower to be same as upper is identical to producing a singleton bucket
	if (this->m_bucket_upper_bound->Equals(point_lower_new))
	{
		return MakeBucketSingleton(mp, point_lower_new);
	}

	CDouble frequency_new = this->GetFrequency();
	CDouble distinct_new = this->GetNumDistinct();

	if (!this->GetLowerBound()->Equals(point_lower_new))
	{
		CDouble overlap =
			CDouble(1.0) - this->GetOverlapPercentage(point_lower_new);
		frequency_new = this->GetFrequency() * overlap;
		distinct_new = this->GetNumDistinct() * overlap;
	}

	// reuse the lower from this bucket
	this->m_bucket_upper_bound->AddRef();
	point_lower_new->AddRef();

	return GPOS_NEW(mp)
		CBucket(point_lower_new, this->m_bucket_upper_bound, include_lower,
				this->m_is_upper_closed, frequency_new, distinct_new);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::MakeBucketSingleton
//
//	@doc:
//		Create a new bucket that is a scaled down version
//		singleton
//
//---------------------------------------------------------------------------
CBucket *
CBucket::MakeBucketSingleton(CMemoryPool *mp, CPoint *point_singleton) const
{
	GPOS_ASSERT(mp);
	GPOS_ASSERT(point_singleton);
	GPOS_ASSERT(this->Contains(point_singleton));

	// assume that this point is one of the ndistinct values
	// in the bucket
	CDouble distinct_ratio = CDouble(1.0) / this->m_distinct;

	CDouble frequency_new =
		std::min(DOUBLE(1.0), (this->m_frequency * distinct_ratio).Get());
	CDouble distinct_new = CDouble(1.0);

	// singleton point is both lower and upper
	point_singleton->AddRef();
	point_singleton->AddRef();

	return GPOS_NEW(mp)
		CBucket(point_singleton, point_singleton, true /* is_lower_closed */,
				true /* is_upper_closed */, frequency_new, distinct_new);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::MakeBucketCopy
//
//	@doc:
//		Copy of bucket. Points are shared.
//
//---------------------------------------------------------------------------
CBucket *
CBucket::MakeBucketCopy(CMemoryPool *mp)
{
	// reuse the points
	m_bucket_lower_bound->AddRef();
	m_bucket_upper_bound->AddRef();

	return GPOS_NEW(mp)
		CBucket(m_bucket_lower_bound, m_bucket_upper_bound, m_is_lower_closed,
				m_is_upper_closed, m_frequency, m_distinct);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::MakeBucketUpdateFrequency
//
//	@doc:
//		Create copy of bucket with a copy of the bucket with updated frequency
//		based on the new total number of rows
//---------------------------------------------------------------------------
CBucket *
CBucket::MakeBucketUpdateFrequency(CMemoryPool *mp, CDouble rows_old,
								   CDouble rows_new)
{
	// reuse the points
	m_bucket_lower_bound->AddRef();
	m_bucket_upper_bound->AddRef();

	CDouble frequency_new = (this->m_frequency * rows_old) / rows_new;

	return GPOS_NEW(mp)
		CBucket(m_bucket_lower_bound, m_bucket_upper_bound, m_is_lower_closed,
				m_is_upper_closed, frequency_new, m_distinct);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::CompareLowerBounds
//
//	@doc:
//		Compare lower bounds of the buckets, return 0 if they match, 1 if
//		lb of bucket1 is greater than lb of bucket2 and -1 otherwise.
//
//---------------------------------------------------------------------------
INT
CBucket::CompareLowerBounds(const CBucket *bucket1, const CBucket *bucket2)
{
	GPOS_ASSERT(NULL != bucket1);
	GPOS_ASSERT(NULL != bucket2);

	CPoint *point1 = bucket1->GetLowerBound();
	CPoint *point2 = bucket2->GetLowerBound();

	BOOL is_closed_point1 = bucket1->IsLowerClosed();
	BOOL is_closed_point2 = bucket2->IsLowerClosed();

	if (point1->Equals(point2))
	{
		if (is_closed_point1 == is_closed_point2)
		{
			return 0;
		}

		if (is_closed_point1)
		{
			// bucket1 contains the lower bound (lb), while bucket2 contain all
			// values between (lb + delta) and upper bound (ub)
			return -1;
		}

		return 1;
	}

	if (point1->IsLessThan(point2))
	{
		return -1;
	}

	return 1;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::CompareLowerBoundToUpperBound
//
//	@doc:
//		Compare lb of the first bucket to the ub of the second bucket,
//		return 0 if they match, 1 if lb of bucket1 is greater
//		than ub of bucket2 and -1 otherwise.
//---------------------------------------------------------------------------
INT
CBucket::CompareLowerBoundToUpperBound(const CBucket *bucket1,
									   const CBucket *bucket2)
{
	CPoint *lower_bound_first = bucket1->GetLowerBound();
	CPoint *upper_bound_second = bucket2->GetUpperBound();

	if (lower_bound_first->IsGreaterThan(upper_bound_second))
	{
		return 1;
	}

	if (lower_bound_first->IsLessThan(upper_bound_second))
	{
		return -1;
	}

	// equal
	if (bucket1->IsLowerClosed() && bucket2->IsUpperClosed())
	{
		return 0;
	}

	return 1;  // points not comparable
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::CompareUpperBounds
//
//	@doc:
//		Compare upper bounds of the buckets, return 0 if they match, 1 if
//		ub of bucket1 is greater than that of bucket2 and -1 otherwise.
//
//---------------------------------------------------------------------------
INT
CBucket::CompareUpperBounds(const CBucket *bucket1, const CBucket *bucket2)
{
	GPOS_ASSERT(NULL != bucket1);
	GPOS_ASSERT(NULL != bucket2);

	CPoint *point1 = bucket1->GetUpperBound();
	CPoint *point2 = bucket2->GetUpperBound();

	BOOL is_closed_point1 = bucket1->IsUpperClosed();
	BOOL is_closed_point2 = bucket2->IsUpperClosed();

	if (point1->Equals(point2))
	{
		if (is_closed_point1 == is_closed_point2)
		{
			return 0;
		}

		if (is_closed_point1)
		{
			// bucket2 contains all values less than upper bound not including upper bound point
			// therefore bucket1 upper bound greater than bucket2 upper bound
			return 1;
		}

		return -1;
	}

	if (point1->IsLessThan(point2))
	{
		return -1;
	}

	return 1;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::Intersects
//
//	@doc:
//		Does this bucket intersect with another?
//
//---------------------------------------------------------------------------
BOOL
CBucket::Intersects(const CBucket *bucket) const
{
	if (this->IsSingleton() && bucket->IsSingleton())
	{
		return GetLowerBound()->Equals(bucket->GetLowerBound());
	}

	if (this->IsSingleton())
	{
		return bucket->Contains(GetLowerBound());
	}

	if (bucket->IsSingleton())
	{
		return Contains(bucket->GetLowerBound());
	}

	if (this->Subsumes(bucket) || bucket->Subsumes(this))
	{
		return true;
	}

	if (0 >= CompareLowerBounds(this, bucket))
	{
		// current bucket starts before the other bucket
		if (0 >= CompareLowerBoundToUpperBound(bucket, this))
		{
			// other bucket starts before current bucket ends
			return true;
		}

		return false;
	}

	if (0 >= CompareLowerBoundToUpperBound(this, bucket))
	{
		// current bucket starts before the other bucket ends
		return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::Subsumes
//
//	@doc:
//		Does this bucket subsume another?
//
//---------------------------------------------------------------------------
BOOL
CBucket::Subsumes(const CBucket *bucket) const
{
	// both are singletons
	if (this->IsSingleton() && bucket->IsSingleton())
	{
		return GetLowerBound()->Equals(bucket->GetLowerBound());
	}

	// other one is a singleton
	if (bucket->IsSingleton())
	{
		return this->Contains(bucket->GetLowerBound());
	}

	INT lower_bounds_comparison = CompareLowerBounds(this, bucket);
	INT upper_bounds_comparison = CompareUpperBounds(this, bucket);

	return (0 >= lower_bounds_comparison && 0 <= upper_bounds_comparison);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::MakeBucketIntersect
//
//	@doc:
//		Create a new bucket by intersecting with another
//		and return the percentage of each of the buckets that intersect.
// 		Points will be shared
//
//		We can think of this method as looking at the cartesian product of
//		two histograms, with "this" being a bucket from histogram 1 and
//		and "bucket" being from histogram 2.
//
//		The goal is to build a histogram that reflects the diagonal of
//		the cartesian product, where the two values are equal, which is
//		the result of the equi-join.
//
//		To do this, we take the overlapping rectangles from the original
//		buckets and form new "squares" such that their corners lie on
//		the diagonal. This method will take two overlapping buckets and
//		return one such result bucket.
//
//		Example (shown below): this = [10, 14], bucket = [8,16]
//
//		The result will be [10,14] in this example, since "this"
//		is fully contained in "bucket".
//
//		                                       diagonal
//		                                          V
//		               +----------------------------------+
//		 histogram 1   |       |              |  /        |
//		               |                       /          |
//		               |       |             /|           |
//		      +-->  14 *- - - - - -+-------* - - - - - - -|
//		      |        |       |   |     / |  |           |
//		   "this"      |           |   /   |              |
//		      |        |       |   | /     |  |           |
//		      +-->  10 *- - - -+---*-------+ - - - - - - -|
//		               |       | / |          |           |
//		             8 |       *---+                      |
//		               |     / |              |           |
//		               |   /                               |
//		               | /     |              |           |
//		               +-------+---*-------*--+-----------+
//		                       8  10      14  16
//		                       +-- "bucket" --+
//
//		                                    histogram 2
//
//		The reason why we show this as a two-dimensional picture here instead
//		of just two overlapping intervals is because of how we compute the frequency
//		of this resulting square:
//
//		This is done by applying the general cardinality formula for
//		equi-joins: | R join S on R.a = S.b | = |R| * |S| / max(NDV(R.a), NDV(S.b))
//
//		The join of the two tables is the union of the join of each of the
//		squares we produce, so we apply the formula to each generated square
//		(bucket of the join histogram).
//		Note that there are no equi-join results outside of these squares that
//		overlay the diagonal.
//---------------------------------------------------------------------------
CBucket *
CBucket::MakeBucketIntersect(CMemoryPool *mp, CBucket *bucket,
							 CDouble *result_freq_intersect1,
							 CDouble *result_freq_intersect2) const
{
	// should only be called on intersecting bucket
	GPOS_ASSERT(Intersects(bucket));

	CPoint *lower_new =
		CPoint::MaxPoint(this->GetLowerBound(), bucket->GetLowerBound());
	CPoint *upper_new =
		CPoint::MinPoint(this->GetUpperBound(), bucket->GetUpperBound());

	BOOL lower_new_is_closed = true;
	BOOL upper_new_is_closed = true;

	CDouble distance_new = 1.0;
	if (!lower_new->Equals(upper_new))
	{
		lower_new_is_closed = this->m_is_lower_closed;
		upper_new_is_closed = this->m_is_upper_closed;

		if (lower_new->Equals(bucket->GetLowerBound()))
		{
			lower_new_is_closed = bucket->IsLowerClosed();
			if (lower_new->Equals(this->GetLowerBound()))
			{
				lower_new_is_closed =
					this->IsLowerClosed() && bucket->IsLowerClosed();
			}
		}

		if (upper_new->Equals(bucket->GetUpperBound()))
		{
			upper_new_is_closed = bucket->IsUpperClosed();
			if (upper_new->Equals(this->GetUpperBound()))
			{
				upper_new_is_closed =
					this->IsUpperClosed() && bucket->IsUpperClosed();
			}
		}

		distance_new = upper_new->Distance(lower_new);
	}

	// TODO: , May 1 2013, distance function for data types such as bpchar/varchar
	// that require binary comparison
	GPOS_ASSERT(distance_new <= Width());
	GPOS_ASSERT(distance_new <= bucket->Width());

	// assume the values are equally distributed in the old buckets, so allocate a
	// proportional value of NDVs to the new bucket
	CDouble ratio1 = distance_new / Width();
	CDouble ratio2 = distance_new / bucket->Width();

	// edge case
	if (IsSingleton() && bucket->IsSingleton())
	{
		ratio1 = CDouble(1.0);
		ratio2 = CDouble(1.0);
	}

	// we are assuming an equi-join, so the side with the fewest NDVs determines the
	// NDV of the join, any values on one side that don't match the other side are
	// discarded
	CDouble distinct_new(std::min(ratio1.Get() * m_distinct.Get(),
								  ratio2.Get() * bucket->m_distinct.Get()));

	// Based on Ramakrishnan and Gehrke, "Database Management Systems, Third Ed", page 484
	// the cardinality of an equality join is the product of the base table cardinalities
	// divided by the MAX of the number of distinct values in each of the inputs
	//
	// Note that we use frequencies here instead of cardinalities, and the resulting frequency
	// is a fraction of the cardinality of the cartesian product

	CDouble freq_intersect1 = ratio1 * m_frequency;
	CDouble freq_intersect2 = ratio2 * bucket->m_frequency;

	CDouble frequency_new(
		freq_intersect1 * freq_intersect2 * DOUBLE(1.0) /
		std::max(ratio1.Get() * m_distinct.Get(),
				 ratio2.Get() * bucket->GetNumDistinct().Get()));

	lower_new->AddRef();
	upper_new->AddRef();

	*result_freq_intersect1 = freq_intersect1;
	*result_freq_intersect2 = freq_intersect2;

	return GPOS_NEW(mp)
		CBucket(lower_new, upper_new, lower_new_is_closed, upper_new_is_closed,
				frequency_new, distinct_new);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::Width
//
//	@doc:
//		Width of bucket
//
//---------------------------------------------------------------------------
CDouble
CBucket::Width() const
{
	if (IsSingleton())
	{
		return CDouble(1.0);
	}
	else
	{
		return m_bucket_upper_bound->Distance(m_bucket_lower_bound);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::Difference
//
//	@doc:
//		Remove a bucket range. This produces an upper and lower split either
//		of which may be NULL.
//
//
//---------------------------------------------------------------------------
void
CBucket::Difference(CMemoryPool *mp, CBucket *bucket_other,
					CBucket **result_bucket_lower,
					CBucket **result_bucket_upper)
{
	// we shouldn't be overwriting anything important
	GPOS_ASSERT(NULL == *result_bucket_lower);
	GPOS_ASSERT(NULL == *result_bucket_upper);

	// if other bucket subsumes this bucket, then result is NULL, NULL
	if (bucket_other->Subsumes(this))
	{
		*result_bucket_lower = NULL;
		*result_bucket_upper = NULL;
		return;
	}

	// if this bucket is below the other bucket, then return this, NULL
	if (this->IsBefore(bucket_other))
	{
		*result_bucket_lower = this->MakeBucketCopy(mp);
		*result_bucket_upper = NULL;
		return;
	}

	// if other bucket is "below" this bucket, then return NULL, this
	if (bucket_other->IsBefore(this))
	{
		*result_bucket_lower = NULL;
		*result_bucket_upper = this->MakeBucketCopy(mp);
		return;
	}

	// if other bucket's LB is after this bucket's LB, then we get a valid first split
	if (this->GetLowerBound()->IsLessThan(bucket_other->GetLowerBound()))
	{
		*result_bucket_lower = this->MakeBucketScaleUpper(
			mp, bucket_other->GetLowerBound(), !bucket_other->IsLowerClosed());
	}

	// if other bucket's UB is lesser than this bucket's LB, then we get a valid split
	if (bucket_other->GetUpperBound()->IsLessThan(this->GetUpperBound()))
	{
		*result_bucket_upper = this->MakeBucketScaleLower(
			mp, bucket_other->GetUpperBound(), !bucket_other->IsUpperClosed());
	}

	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::IsBefore
//
//	@doc:
//		Does this bucket occur before other? E.g. [1,2) is before [3,4)
//
//---------------------------------------------------------------------------
BOOL
CBucket::IsBefore(const CBucket *bucket) const
{
	if (this->Intersects(bucket))
	{
		return false;
	}

	return this->GetUpperBound()->IsLessThanOrEqual(bucket->GetLowerBound());
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::IsAfter
//
//	@doc:
//		Does this bucket occur after other? E.g. [2,4) is after [1,2)
//
//---------------------------------------------------------------------------
BOOL
CBucket::IsAfter(const CBucket *bucket) const
{
	if (this->Intersects(bucket))
	{
		return false;
	}

	return this->GetLowerBound()->IsGreaterThanOrEqual(bucket->GetUpperBound());
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::MakeBucketMerged
//
//	@doc:
//		Merges with another bucket. Returns merged bucket that should be part
//		of the output. It also returns what is leftover from the merge.
//		E.g.
//		merge of [1,100) and [50,150) produces [1, 100), NULL, [100, 150)
//		merge of [1,100) and [50,75) produces [1, 75), [75,100), NULL
//		merge of [1,1) and [1,1) produces [1,1), NULL, NULL
//
//---------------------------------------------------------------------------
CBucket *
CBucket::MakeBucketMerged(CMemoryPool *mp, CBucket *bucket_other, CDouble rows,
						  CDouble rows_other, CBucket **result_bucket_new1,
						  CBucket **result_bucket_new2, BOOL is_union_all)
{
	// we shouldn't be overwriting anything important
	GPOS_ASSERT(NULL == *result_bucket_new1);
	GPOS_ASSERT(NULL == *result_bucket_new2);

	CPoint *result_lower_new =
		CPoint::MinPoint(this->GetLowerBound(), bucket_other->GetLowerBound());
	CPoint *result_upper_new =
		CPoint::MinPoint(this->GetUpperBound(), bucket_other->GetUpperBound());

	CDouble overlap = this->GetOverlapPercentage(result_upper_new);
	CDouble distinct = this->GetNumDistinct() * overlap;
	CDouble rows_new = rows * this->GetFrequency() * overlap;

	CDouble frequency =
		this->GetFrequency() * this->GetOverlapPercentage(result_upper_new);
	if (is_union_all)
	{
		CDouble rows_output = (rows_other + rows);
		CDouble overlap_other =
			bucket_other->GetOverlapPercentage(result_upper_new);
		distinct = distinct + (bucket_other->GetNumDistinct() * overlap_other);
		rows_new = rows_other * bucket_other->GetFrequency() * overlap_other;
		frequency = rows_new / rows_output;
	}

	BOOL is_upper_closed = result_lower_new->Equals(result_upper_new);

	if (result_upper_new->IsLessThan(this->GetUpperBound()))
	{
		*result_bucket_new1 =
			this->MakeBucketScaleLower(mp, result_upper_new, !is_upper_closed);
	}

	if (result_upper_new->IsLessThan(bucket_other->GetUpperBound()))
	{
		*result_bucket_new2 = bucket_other->MakeBucketScaleLower(
			mp, result_upper_new, !is_upper_closed);
	}

	result_lower_new->AddRef();
	result_upper_new->AddRef();

	return GPOS_NEW(mp)
		CBucket(result_lower_new, result_upper_new, true /* is_lower_closed */,
				is_upper_closed, frequency, distinct);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::GetSample
//
//	@doc:
//		Generate a random data point within bucket boundaries
//
//---------------------------------------------------------------------------
CDouble
CBucket::GetSample(ULONG *seed) const
{
	GPOS_ASSERT(CanSample());

	DOUBLE lower_val = m_bucket_lower_bound->GetDatum()->GetValAsDouble().Get();
	if (IsSingleton())
	{
		return CDouble(lower_val);
	}

	DOUBLE upper_val = m_bucket_upper_bound->GetDatum()->GetValAsDouble().Get();
	DOUBLE rand_val = ((DOUBLE) clib::Rand(seed)) / RAND_MAX;

	return CDouble(lower_val + rand_val * (upper_val - lower_val));
}


//---------------------------------------------------------------------------
//	@function:
//		CBucket::MakeBucketSingleton
//
//	@doc:
//		Create a new singleton bucket with the given datum as it lower
//		and upper bounds
//
//---------------------------------------------------------------------------
CBucket *
CBucket::MakeBucketSingleton(CMemoryPool *mp, IDatum *datum)
{
	GPOS_ASSERT(NULL != datum);

	datum->AddRef();
	datum->AddRef();

	return GPOS_NEW(mp)
		CBucket(GPOS_NEW(mp) CPoint(datum), GPOS_NEW(mp) CPoint(datum),
				true /* is_lower_closed */, true /* is_upper_closed */,
				CDouble(1.0), CDouble(1.0));
}