//---------------------------------------------------------------------------
//	@filename:
//		IDatum.cpp
//
//	@doc:
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/IDatum.h"

using namespace gpnaucrates;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IDatum::StatsAreEqual
//
//	@doc:
//		Equality based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
BOOL
IDatum::StatsAreEqual(const IDatum *datum) const
{
	GPOS_ASSERT(NULL != datum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL is_double_comparison =
		this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
#endif	// GPOS_DEBUG
	BOOL is_lint_comparison =
		this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

	GPOS_ASSERT(is_double_comparison || is_lint_comparison);

	if (this->IsNull())
	{
		// nulls are equal from stats point of view
		return datum->IsNull();
	}

	if (datum->IsNull())
	{
		return false;
	}

	if (is_lint_comparison)
	{
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum->GetLINTMapping();
		return l1 == l2;
	}

	GPOS_ASSERT(is_double_comparison);

	CDouble d1 = this->GetDoubleMapping();
	CDouble d2 = datum->GetDoubleMapping();
	return d1 == d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::StatsAreLessThan
//
//	@doc:
//		Less-than based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
BOOL
IDatum::StatsAreLessThan(const IDatum *datum) const
{
	GPOS_ASSERT(NULL != datum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL is_double_comparison =
		this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
#endif	// GPOS_DEBUG
	BOOL is_lint_comparison =
		this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

	GPOS_ASSERT(is_double_comparison || is_lint_comparison);

	if (this->IsNull())
	{
		// nulls are less than everything else except nulls
		return !(datum->IsNull());
	}

	if (datum->IsNull())
	{
		return false;
	}

	if (is_lint_comparison)
	{
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum->GetLINTMapping();
		return l1 < l2;
	}

	GPOS_ASSERT(is_double_comparison);

	CDouble d1 = this->GetDoubleMapping();
	CDouble d2 = datum->GetDoubleMapping();
	return d1 < d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::GetStatsDistanceFrom
//
//	@doc:
//		Distance function based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
CDouble
IDatum::GetStatsDistanceFrom(const IDatum *datum) const
{
	GPOS_ASSERT(NULL != datum);

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL is_double_comparison =
		this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
#endif	// GPOS_DEBUG
	BOOL is_lint_comparison =
		this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

	GPOS_ASSERT(is_double_comparison || is_lint_comparison);

	if (this->IsNull())
	{
		// nulls are equal from stats point of view
		return datum->IsNull();
	}

	if (datum->IsNull())
	{
		return false;
	}

	if (is_lint_comparison)
	{
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum->GetLINTMapping();
		return l1 - l2;
	}

	GPOS_ASSERT(is_double_comparison);

	CDouble d1 = this->GetDoubleMapping();
	CDouble d2 = datum->GetDoubleMapping();
	return d1 - d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::GetValAsDouble
//
//	@doc:
//		 Return double representation of mapping value
//
//---------------------------------------------------------------------------
CDouble
IDatum::GetValAsDouble() const
{
	if (IsNull())
	{
		return CDouble(0.0);
	}

	if (IsDatumMappableToLINT())
	{
		return CDouble(GetLINTMapping());
	}

	return CDouble(GetDoubleMapping());
}


//---------------------------------------------------------------------------
//	@function:
//		IDatum::StatsAreComparable
//
//	@doc:
//		Check if the given pair of datums are stats comparable
//
//---------------------------------------------------------------------------
BOOL IDatum::StatsAreComparable(const IDatum *datum) const
{
	GPOS_ASSERT(NULL != datum);
	BOOL is_types_match = this->MDId()->Equals(datum->MDId());
	// datums can be compared based on either LINT or Doubles or BYTEA values
	BOOL is_double_comparison = this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
	BOOL is_lint_comparison = this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();
	return is_double_comparison || is_lint_comparison;
}