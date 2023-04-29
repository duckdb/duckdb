#include "function_list.hpp"
#include "scalar/bit_functions.hpp"
#include "scalar/blob_functions.hpp"
#include "scalar/date_functions.hpp"
#include "scalar/enum_functions.hpp"
#include "scalar/generic_functions.hpp"
#include "scalar/list_functions.hpp"
#include "scalar/map_functions.hpp"
#include "scalar/struct_functions.hpp"
#include "scalar/union_functions.hpp"

namespace duckdb {

#define DUCKDB_SCALAR_FUNCTION(_PARAM)                                                                                 \
	{ _PARAM::Name, _PARAM::Parameters, _PARAM::Description, _PARAM::Example, _PARAM::GetFunction, nullptr }
#define DUCKDB_SCALAR_FUNCTION_SET(_PARAM)                                                                             \
	{ _PARAM::Name, _PARAM::Parameters, _PARAM::Description, _PARAM::Example, nullptr, _PARAM::GetFunctions }
#define DUCKDB_SCALAR_FUNCTION_ALIAS(_PARAM)                                                                           \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::ALIAS::Parameters, _PARAM::ALIAS::Description, _PARAM::ALIAS::Example,                   \
		    _PARAM::ALIAS::GetFunction, nullptr                                                                        \
	}
#define DUCKDB_SCALAR_FUNCTION_SET_ALIAS(_PARAM)                                                                       \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::ALIAS::Parameters, _PARAM::ALIAS::Description, _PARAM::ALIAS::Example, nullptr,          \
		    _PARAM::ALIAS::GetFunctions                                                                                \
	}
#define FINAL_FUNCTION                                                                                                 \
	{ nullptr, nullptr, nullptr, nullptr, nullptr, nullptr }

static StaticFunctionDefinition internal_functions[] = {
	DUCKDB_SCALAR_FUNCTION_SET(AgeFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(AggregateFun),
	DUCKDB_SCALAR_FUNCTION(AliasFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ApplyFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayAggrFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayAggregateFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayApplyFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayDistinctFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayFilterFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ArrayReverseSortFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArraySliceFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ArraySortFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayTransformFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayUniqueFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(Base64Fun),
	DUCKDB_SCALAR_FUNCTION(BitPositionFun),
	DUCKDB_SCALAR_FUNCTION(BitStringFun),
	DUCKDB_SCALAR_FUNCTION(CardinalityFun),
	DUCKDB_SCALAR_FUNCTION_SET(CenturyFun),
	DUCKDB_SCALAR_FUNCTION(CurrentDatabaseFun),
	DUCKDB_SCALAR_FUNCTION(CurrentDateFun),
	DUCKDB_SCALAR_FUNCTION(CurrentQueryFun),
	DUCKDB_SCALAR_FUNCTION(CurrentSchemaFun),
	DUCKDB_SCALAR_FUNCTION(CurrentSchemasFun),
	DUCKDB_SCALAR_FUNCTION(CurrentSettingFun),
	DUCKDB_SCALAR_FUNCTION_SET(DateDiffFun),
	DUCKDB_SCALAR_FUNCTION_SET(DatePartFun),
	DUCKDB_SCALAR_FUNCTION_SET(DateSubFun),
	DUCKDB_SCALAR_FUNCTION_SET(DateTruncFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(DatediffFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayNameFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayOfMonthFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayOfWeekFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayOfYearFun),
	DUCKDB_SCALAR_FUNCTION_SET(DecadeFun),
	DUCKDB_SCALAR_FUNCTION(DecodeFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ElementAtFun),
	DUCKDB_SCALAR_FUNCTION(EncodeFun),
	DUCKDB_SCALAR_FUNCTION(EnumCodeFun),
	DUCKDB_SCALAR_FUNCTION(EnumFirstFun),
	DUCKDB_SCALAR_FUNCTION(EnumLastFun),
	DUCKDB_SCALAR_FUNCTION(EnumRangeFun),
	DUCKDB_SCALAR_FUNCTION(EnumRangeBoundaryFun),
	DUCKDB_SCALAR_FUNCTION_SET(EpochFun),
	DUCKDB_SCALAR_FUNCTION(EpochMsFun),
	DUCKDB_SCALAR_FUNCTION_SET(EraFun),
	DUCKDB_SCALAR_FUNCTION(ErrorFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(FilterFun),
	DUCKDB_SCALAR_FUNCTION(ListFlattenFun),
	DUCKDB_SCALAR_FUNCTION(FromBase64Fun),
	DUCKDB_SCALAR_FUNCTION_SET(GenerateSeriesFun),
	DUCKDB_SCALAR_FUNCTION(GetBitFun),
	DUCKDB_SCALAR_FUNCTION(CurrentTimeFun),
	DUCKDB_SCALAR_FUNCTION(GetCurrentTimestampFun),
	DUCKDB_SCALAR_FUNCTION_SET(GreatestFun),
	DUCKDB_SCALAR_FUNCTION(HashFun),
	DUCKDB_SCALAR_FUNCTION_SET(HoursFun),
	DUCKDB_SCALAR_FUNCTION_SET(ISODayOfWeekFun),
	DUCKDB_SCALAR_FUNCTION_SET(ISOYearFun),
	DUCKDB_SCALAR_FUNCTION_SET(LastDayFun),
	DUCKDB_SCALAR_FUNCTION_SET(LeastFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ListAggrFun),
	DUCKDB_SCALAR_FUNCTION(ListAggregateFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ListApplyFun),
	DUCKDB_SCALAR_FUNCTION(ListDistinctFun),
	DUCKDB_SCALAR_FUNCTION(ListFilterFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ListPackFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListReverseSortFun),
	DUCKDB_SCALAR_FUNCTION(ListSliceFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListSortFun),
	DUCKDB_SCALAR_FUNCTION(ListTransformFun),
	DUCKDB_SCALAR_FUNCTION(ListUniqueFun),
	DUCKDB_SCALAR_FUNCTION(ListValueFun),
	DUCKDB_SCALAR_FUNCTION_SET(MakeDateFun),
	DUCKDB_SCALAR_FUNCTION(MakeTimeFun),
	DUCKDB_SCALAR_FUNCTION(MakeTimestampFun),
	DUCKDB_SCALAR_FUNCTION(MapFun),
	DUCKDB_SCALAR_FUNCTION(MapEntriesFun),
	DUCKDB_SCALAR_FUNCTION(MapExtractFun),
	DUCKDB_SCALAR_FUNCTION(MapFromEntriesFun),
	DUCKDB_SCALAR_FUNCTION(MapKeysFun),
	DUCKDB_SCALAR_FUNCTION(MapValuesFun),
	DUCKDB_SCALAR_FUNCTION_SET(MicrosecondsFun),
	DUCKDB_SCALAR_FUNCTION_SET(MillenniumFun),
	DUCKDB_SCALAR_FUNCTION_SET(MillisecondsFun),
	DUCKDB_SCALAR_FUNCTION_SET(MinutesFun),
	DUCKDB_SCALAR_FUNCTION_SET(MonthFun),
	DUCKDB_SCALAR_FUNCTION_SET(MonthNameFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(NowFun),
	DUCKDB_SCALAR_FUNCTION_SET(QuarterFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListRangeFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(RowFun),
	DUCKDB_SCALAR_FUNCTION_SET(SecondsFun),
	DUCKDB_SCALAR_FUNCTION(SetBitFun),
	DUCKDB_SCALAR_FUNCTION(StatsFun),
	DUCKDB_SCALAR_FUNCTION_SET(StrfTimeFun),
	DUCKDB_SCALAR_FUNCTION_SET(StrpTimeFun),
	DUCKDB_SCALAR_FUNCTION(StructInsertFun),
	DUCKDB_SCALAR_FUNCTION(StructPackFun),
	DUCKDB_SCALAR_FUNCTION_SET(TimeBucketFun),
	DUCKDB_SCALAR_FUNCTION_SET(TimezoneFun),
	DUCKDB_SCALAR_FUNCTION_SET(TimezoneHourFun),
	DUCKDB_SCALAR_FUNCTION_SET(TimezoneMinuteFun),
	DUCKDB_SCALAR_FUNCTION(ToBase64Fun),
	DUCKDB_SCALAR_FUNCTION(ToDaysFun),
	DUCKDB_SCALAR_FUNCTION(ToHoursFun),
	DUCKDB_SCALAR_FUNCTION(ToMicrosecondsFun),
	DUCKDB_SCALAR_FUNCTION(ToMillisecondsFun),
	DUCKDB_SCALAR_FUNCTION(ToMinutesFun),
	DUCKDB_SCALAR_FUNCTION(ToMonthsFun),
	DUCKDB_SCALAR_FUNCTION(ToSecondsFun),
	DUCKDB_SCALAR_FUNCTION(ToTimestampFun),
	DUCKDB_SCALAR_FUNCTION(ToYearsFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(TodayFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(TransactionTimestampFun),
	DUCKDB_SCALAR_FUNCTION_SET(TryStrpTimeFun),
	DUCKDB_SCALAR_FUNCTION(CurrentTransactionIdFun),
	DUCKDB_SCALAR_FUNCTION(TypeOfFun),
	DUCKDB_SCALAR_FUNCTION(UnionExtractFun),
	DUCKDB_SCALAR_FUNCTION(UnionTagFun),
	DUCKDB_SCALAR_FUNCTION(UnionValueFun),
	DUCKDB_SCALAR_FUNCTION(VersionFun),
	DUCKDB_SCALAR_FUNCTION_SET(WeekFun),
	DUCKDB_SCALAR_FUNCTION_SET(WeekDayFun),
	DUCKDB_SCALAR_FUNCTION_SET(WeekOfYearFun),
	DUCKDB_SCALAR_FUNCTION_SET(YearFun),
	DUCKDB_SCALAR_FUNCTION_SET(YearWeekFun),
	FINAL_FUNCTION
};

StaticFunctionDefinition *StaticFunctionDefinition::GetFunctionList() {
	return internal_functions;
}

} // namespace duckdb
