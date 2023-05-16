//---------------------------------------------------------------------------
//	@filename:
//		CPoint.cpp
//
//	@doc:
//		Implementation of histogram point
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CPoint.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/mdcache/CMDAccessor.h"

using namespace gpnaucrates;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPoint::CPoint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPoint::CPoint(IDatum *datum) : m_datum(datum)
{
	GPOS_ASSERT(NULL != m_datum);
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::Equals
//
//	@doc:
//		Equality check
//
//---------------------------------------------------------------------------
BOOL
CPoint::Equals(const CPoint *point) const
{
	GPOS_ASSERT(NULL != point);
	return m_datum->StatsAreEqual(point->m_datum);
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::IsNotEqual
//
//	@doc:
//		Inequality check
//
//---------------------------------------------------------------------------
BOOL
CPoint::IsNotEqual(const CPoint *point) const
{
	return !(this->Equals(point));
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::IsLessThan
//
//	@doc:
//		Less than check
//
//---------------------------------------------------------------------------
BOOL
CPoint::IsLessThan(const CPoint *point) const
{
	GPOS_ASSERT(NULL != point);
	return m_datum->StatsAreComparable(point->m_datum) &&
		   m_datum->StatsAreLessThan(point->m_datum);
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::IsLessThanOrEqual
//
//	@doc:
//		Less than or equals check
//
//---------------------------------------------------------------------------
BOOL
CPoint::IsLessThanOrEqual(const CPoint *point) const
{
	return (this->IsLessThan(point) || this->Equals(point));
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::IsGreaterThan
//
//	@doc:
//		Greater than check
//
//---------------------------------------------------------------------------
BOOL
CPoint::IsGreaterThan(const CPoint *point) const
{
	return m_datum->StatsAreComparable(point->m_datum) &&
		   m_datum->StatsAreGreaterThan(point->m_datum);
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::IsGreaterThanOrEqual
//
//	@doc:
//		Greater than or equals check
//
//---------------------------------------------------------------------------
BOOL
CPoint::IsGreaterThanOrEqual(const CPoint *point) const
{
	return (this->IsGreaterThan(point) || this->Equals(point));
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::Distance
//
//	@doc:
//		Distance between two points
//
//---------------------------------------------------------------------------
CDouble
CPoint::Distance(const CPoint *point) const
{
	GPOS_ASSERT(NULL != point);
	if (m_datum->StatsAreComparable(point->m_datum))
	{
		return CDouble(m_datum->GetStatsDistanceFrom(point->m_datum));
	}

	// default to a non zero constant for overlap
	// computation
	return CDouble(1.0);
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CPoint::OsPrint(IOstream &os) const
{
	m_datum->OsPrint(os);
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::MinPoint
//
//	@doc:
//		minimum of two points using <=
//
//---------------------------------------------------------------------------
CPoint *
CPoint::MinPoint(CPoint *point1, CPoint *point2)
{
	if (point1->IsLessThanOrEqual(point2))
	{
		return point1;
	}
	return point2;
}

//---------------------------------------------------------------------------
//	@function:
//		CPoint::MaxPoint
//
//	@doc:
//		maximum of two points using >=
//
//---------------------------------------------------------------------------
CPoint *
CPoint::MaxPoint(CPoint *point1, CPoint *point2)
{
	if (point1->IsGreaterThanOrEqual(point2))
	{
		return point1;
	}
	return point2;
}