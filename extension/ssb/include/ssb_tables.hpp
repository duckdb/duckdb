#pragma once

#include "duckdb.hpp"

using namespace duckdb;

namespace ssb {

struct CustomerInfo {
	static constexpr char *Name = "customer";
	static constexpr uint64_t ColumnCount = 8;
	static const char *Columns[];
	static const LogicalType Types[];
};
const char *CustomerInfo::Columns[] = {"c_custkey", "c_name",   "c_address", "c_city",
                                       "c_nation",  "c_region", "c_phone",   "c_mktsegment"};
const LogicalType CustomerInfo::Types[] = {LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)};

struct SupplierInfo {
	static constexpr char *Name = "supplier";
	static constexpr uint64_t ColumnCount = 7;
	static const char *Columns[];
	static const LogicalType Types[];
};
const char *SupplierInfo::Columns[] = {"s_suppkey", "s_name", "s_address", "s_city", "s_nation", "s_region", "s_phone"};
const LogicalType SupplierInfo::Types[] = {LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                                           LogicalType(LogicalTypeId::VARCHAR)};

struct LineOrderInfo {
	static constexpr char *Name = "lineorder";
	static constexpr uint64_t ColumnCount = 17;
	static const char *Columns[];
	static const LogicalType Types[];
};
const char *LineOrderInfo::Columns[] = {
    "lo_orderkey",      "lo_linenumber",   "lo_custkey",  "lo_partkey",       "lo_suppkey",       "lo_orderdate",
    "lo_orderpriority", "lo_shippriority", "lo_quantity", "lo_extendedprice", "lo_ordtotalprice", "lo_discount",
    "lo_revenue",       "lo_supplycost",   "lo_tax",      "lo_commitdate",    "lo_shipmode"};
const LogicalType LineOrderInfo::Types[] = {
    LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::TINYINT), LogicalType(LogicalTypeId::INTEGER),
    LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::DATE),
    LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::TINYINT), LogicalType(LogicalTypeId::TINYINT),
    LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::TINYINT),
    LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::TINYINT),
    LogicalType(LogicalTypeId::DATE),    LogicalType(LogicalTypeId::VARCHAR)};

struct PartInfo {
	static constexpr char *Name = "part";
	static constexpr uint64_t ColumnCount = 9;
	static const char *Columns[];
	static const LogicalType Types[];
};
const char *PartInfo::Columns[] = {"p_partkey", "p_name", "p_mfgr", "p_category", "p_brand1",
                                   "p_color",   "p_type", "p_size", "p_container"};
const LogicalType PartInfo::Types[] = {
    LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
    LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
    LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::BIGINT),  LogicalType(LogicalTypeId::VARCHAR)};

struct DateInfo {
	static constexpr char *Name = "date";
	static constexpr uint64_t ColumnCount = 17;
	static const char *Columns[];
	static const LogicalType Types[];
};
const char *DateInfo::Columns[] = {"d_datekey",
                                   "d_date",
                                   "d_dayofweek",
                                   "d_month",
                                   "d_year",
                                   "d_yearmonthnum",
                                   "d_yearmonth",
                                   "d_daynuminweek",
                                   "d_daynuminmonth",
                                   "d_monthnuminyear",
                                   "d_weeknuminyear",
                                   "d_sellingseason",
                                   "d_lastdayinweekfl",
                                   "d_lastdayinmonthfl",
                                   "d_holidayfl",
                                   "d_weekdayfl",
                                   "d_daynuminyear"};
const LogicalType DateInfo::Types[] = {
    LogicalType(LogicalTypeId::DATE),     LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::VARCHAR),
    LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::SMALLINT), LogicalType(LogicalTypeId::INTEGER),
    LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::BIGINT),   LogicalType(LogicalTypeId::BIGINT),
    LogicalType(LogicalTypeId::SMALLINT), LogicalType(LogicalTypeId::BIGINT),   LogicalType(LogicalTypeId::BIGINT),
    LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::BIGINT),   LogicalType(LogicalTypeId::BIGINT),
    LogicalType(LogicalTypeId::BIGINT),   LogicalType(LogicalTypeId::BIGINT)};

} // namespace ssb