#include "../ssbgen/include/driver.hpp"
#include "../ssbgen/include/dss.h"
#include "../ssbgen/include/dsstypes.h"
#include "duckdb.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif
#endif

namespace ssb {

struct ssb_append_container {
	duckdb::unique_ptr<duckdb::InternalAppender> appender;
};

void append_int32(ssb_append_container *info, int32_t value) {
	info->appender->Append<int32_t>(value);
}

void append_int64(ssb_append_container *info, int64_t value) {
	info->appender->Append<int64_t>(value);
}

void append_string(ssb_append_container *info, const char *value) {
	info->appender->Append<const char *>(value);
}

void append_decimal(ssb_append_container *info, int64_t value) {
	info->appender->Append<int64_t>(value);
}

void append_date(ssb_append_container *info, std::string value) {
	info->appender->Append<duckdb::date_t>(duckdb::Date::FromString(value));
}

void append_char(ssb_append_container *info, char value) {
	char val[2];
	val[0] = value;
	val[1] = '\0';
	append_string(info, val);
}

static void append_line_order(order_t *line_order_record, ssb_append_container *append_container) {
	for (long i = 0; i < line_order_record->lines; i++) {
		append_container->appender->BeginRow();
		// lo_orderkey
		append_int64(append_container, *line_order_record->lineorders[i].okey);
		// lo_linenumber
		append_int32(append_container, line_order_record->lineorders[i].linenumber);
		// lo_custkey
		append_int64(append_container, line_order_record->lineorders[i].custkey);
		// lo_partkey
		append_int64(append_container, line_order_record->lineorders[i].partkey);
		// lo_suppkey
		append_int64(append_container, line_order_record->lineorders[i].suppkey);
		// lo_orderdate
		append_date(append_container, line_order_record->lineorders[i].orderdate);
		// lo_orderpriority
		append_string(append_container, line_order_record->lineorders[i].opriority);
		// lo_shippriority
		append_int64(append_container, line_order_record->lineorders[i].ship_priority);
		// lo_quantity
		append_int64(append_container, line_order_record->lineorders[i].quantity);
		// lo_extendedprice
		append_int64(append_container, line_order_record->lineorders[i].extended_price);
		// lo_ordertotalprice
		append_int64(append_container, line_order_record->lineorders[i].order_totalprice);
		// lo_discount
		append_int64(append_container, line_order_record->lineorders[i].discount);
		// lo_revenue
		append_int64(append_container, line_order_record->lineorders[i].revenue);
		// lo_supplycost
		append_int64(append_container, line_order_record->lineorders[i].supp_cost);
		// lo_tax
		append_int64(append_container, line_order_record->lineorders[i].tax);
		// lo_commitdate
		append_date(append_container, line_order_record->lineorders[i].commit_date);
		// lo_shipmode
		append_string(append_container, line_order_record->lineorders[i].shipmode);
		append_container->appender->EndRow();
	}
}

static void append_supplier(supplier_t *supplier_record, ssb_append_container *append_container) {

	append_container->appender->BeginRow();
	// s_suppkey
	append_int64(append_container, supplier_record->suppkey);
	// s_name
	append_string(append_container, supplier_record->name);
	// s_address
	append_string(append_container, supplier_record->address);
	// s_city
	append_string(append_container, supplier_record->city);
	// s_nation
	append_string(append_container, supplier_record->nation_name);
	// s_region
	append_string(append_container, supplier_record->region_name);
	// s_phone
	append_string(append_container, supplier_record->phone);
	// s_acctbal
	append_container->appender->EndRow();
}

static void append_customer(customer_t *customer_record, ssb_append_container *append_container) {
	append_container->appender->BeginRow();

	// c_custkey
	append_int64(append_container, customer_record->custkey);
	// c_name
	append_string(append_container, customer_record->name);
	// c_address
	append_string(append_container, customer_record->address);
	// c_city
	append_string(append_container, customer_record->city);
	// c_nation
	append_string(append_container, customer_record->nation_name);
	// c_region
	append_string(append_container, customer_record->region_name);
	// c_phone
	append_string(append_container, customer_record->phone);
	// c_mktsegment
	append_string(append_container, customer_record->mktsegment);

	append_container->appender->EndRow();
}

static void append_part(part_t *part_record, ssb_append_container *append_container) {

	append_container->appender->BeginRow();
	// p_partkey
	append_int64(append_container, part_record->partkey);
	// p_name
	append_string(append_container, part_record->name);
	// p_mfgr
	append_string(append_container, part_record->mfgr);
	// p_category
	append_string(append_container, part_record->category);
	// p_brand
	append_string(append_container, part_record->brand);
	// p_color
	append_string(append_container, part_record->color);
	// p_type
	append_string(append_container, part_record->type);
	// p_size
	append_int64(append_container, part_record->size);
	// p_container
	append_string(append_container, part_record->container);

	append_container->appender->EndRow();
}

static void append_date(ssb_date_t *date_record, ssb_append_container *append_container) {

	append_container->appender->BeginRow();
	// d_datekey
	append_date(append_container, date_record->datekey);
	// d_date
	append_string(append_container, date_record->date);
	// d_dayofweek
	append_string(append_container, date_record->dayofweek);
	// d_month
	append_string(append_container, date_record->month);
	// d_year
	append_int64(append_container, date_record->year);
	// d_yearmonthnum
	append_int64(append_container, date_record->yearmonthnum);
	// d_yearmonth
	append_string(append_container, date_record->yearmonth);
	// d_daynuminweek
	append_int64(append_container, date_record->daynuminweek);
	// d_daynuminmonth
	append_int64(append_container, date_record->daynuminmonth);
	// d_daynuminyear
	append_int64(append_container, date_record->daynuminyear);
	// d_monthnuminyear
	append_int64(append_container, date_record->monthnuminyear);
	// d_weeknuminyear
	append_int64(append_container, date_record->weeknuminyear);
	// d_sellingseason
	append_string(append_container, date_record->sellingseason);
	// d_lastdayinweekfl
	append_int64(append_container, date_record->lastdayinweekfl == "1");
	// d_lastdayinmonthfl
	append_int64(append_container, date_record->lastdayinmonthfl == "1");
	// d_holidayfl
	append_int64(append_container, date_record->holidayfl == "1");
	// d_weekdayfl
	append_int64(append_container, date_record->weekdayfl == "1");

	append_container->appender->EndRow();
}

static std::string get_table_name(int table_index) {
	switch (table_index) {
	case PART:
		return "part";
	case SUPP:
		return "supplier";
	case CUST:
		return "customer";
	case DATE:
		return "date";
	case LINE:
		return "lineorder";
	default:
		return "";
	}
}

static int translate_dbgen_table_index(int dbgen_table_index) {
	switch (dbgen_table_index) {
	case PART:
		return 0;
	case SUPP:
		return 1;
	case CUST:
		return 2;
	case DATE:
		return 3;
	case LINE:
		return 4;
	default:
		throw duckdb::InternalException("Invalid table index");
	}
}
#define NUMBER_OF_TABLES 5

struct SSBGenParameters {
	SSBGenParameters(duckdb::ClientContext &context, duckdb::Catalog &catalog, const std::string &schema,
	                 long scale_factor) {
		this->scale_factor = scale_factor;
		this->schema = schema;

		tables.resize(NUMBER_OF_TABLES);
		for (size_t i = 0; i < NUMBER_OF_TABLES; i++) {
			auto tname = get_table_name(i);
			if (!tname.empty()) {
				std::string full_tname = std::string(tname);
				auto &tbl_catalog = catalog.GetEntry<duckdb::TableCatalogEntry>(context, schema, full_tname);
				tables[i] = &tbl_catalog;
			}
		}
	}

	long scale_factor;
	std::string schema;
	duckdb::vector<duckdb::optional_ptr<duckdb::TableCatalogEntry>> tables;
};

class SSBTableDataGenerator {
public:
	SSBTableDataGenerator(duckdb::ClientContext &context, SSBGenParameters &parameters);

	// Generates and loads data into the tables
	void GenerateData();

private:
	duckdb::ClientContext &context;
	SSBGenParameters &parameters;
	duckdb::unique_ptr<ssb_append_container[]> append_containers;

private:
	ssb_appender *GetAppender();
};

} // namespace ssb
