//---------------------------------------------------------------------------
//	@filename:
//		IMDType.cpp
//
//	@doc:
//		Implementation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/IMDType.h"
#include "duckdb/optimizer/cascade/string/CWStringConst.h"
#include "duckdb/optimizer/cascade/base/IDatum.h"
#include "duckdb/optimizer/cascade/statistics/CStatistics.h"

using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDType::StatsAreComparable
//
//	@doc:
//		Return true if we can perform statistical comparison between
//		datums of these two types; else return false
//
//---------------------------------------------------------------------------
BOOL IMDType::StatsAreComparable(const IMDType *mdtype_first, const IMDType *mdtype_second)
{
	GPOS_ASSERT(NULL != mdtype_first);
	GPOS_ASSERT(NULL != mdtype_second);

	const IDatum *datum_first = mdtype_first->DatumNull();
	const IDatum *datum_second = mdtype_second->DatumNull();

	return datum_first->StatsAreComparable(datum_second);
}


//---------------------------------------------------------------------------
//	@function:
//		IMDType::StatsAreComparable
//
//	@doc:
//		Return true if we can perform statistical comparison between
//		datum of the given type and a given datum; else return false
//
//---------------------------------------------------------------------------
BOOL IMDType::StatsAreComparable(const IMDType *mdtype_first, const IDatum *datum_second)
{
	GPOS_ASSERT(NULL != mdtype_first);
	GPOS_ASSERT(NULL != datum_second);
	const IDatum *datum_first = mdtype_first->DatumNull();
	return datum_first->StatsAreComparable(datum_second);
}