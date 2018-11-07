
#include "dsdgen.hpp"
#include "common/exception.hpp"
#include "main/client_context.hpp"
#include "common/types/data_chunk.hpp"

#include "storage/data_table.hpp"

#include "tpcds_constants.hpp"

using namespace duckdb;
using namespace std;

namespace tpcds {

struct tpch_append_information {
	TableCatalogEntry *table;
	DataChunk chunk;
	ClientContext *context;
};

static vector<ColumnDefinition> RegionColumns() {
	return vector<ColumnDefinition>{
	    ColumnDefinition("r_regionkey", TypeId::INTEGER, false),
	    ColumnDefinition("r_name", TypeId::VARCHAR, false),
	    ColumnDefinition("r_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnDefinition> NationColumns() {
	return vector<ColumnDefinition>{
	    ColumnDefinition("n_nationkey", TypeId::INTEGER, false),
	    ColumnDefinition("n_name", TypeId::VARCHAR, false),
	    ColumnDefinition("n_regionkey", TypeId::INTEGER, false),
	    ColumnDefinition("n_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnDefinition> SupplierColumns() {
	return vector<ColumnDefinition>{
	    ColumnDefinition("s_suppkey", TypeId::INTEGER, false),
	    ColumnDefinition("s_name", TypeId::VARCHAR, false),
	    ColumnDefinition("s_address", TypeId::VARCHAR, false),
	    ColumnDefinition("s_nationkey", TypeId::INTEGER, false),
	    ColumnDefinition("s_phone", TypeId::VARCHAR, false),
	    ColumnDefinition("s_acctbal", TypeId::DECIMAL, false),
	    ColumnDefinition("s_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnDefinition> CustomerColumns() {
	return vector<ColumnDefinition>{
	    ColumnDefinition("c_custkey", TypeId::INTEGER, false),
	    ColumnDefinition("c_name", TypeId::VARCHAR, false),
	    ColumnDefinition("c_address", TypeId::VARCHAR, false),
	    ColumnDefinition("c_nationkey", TypeId::INTEGER, false),
	    ColumnDefinition("c_phone", TypeId::VARCHAR, false),
	    ColumnDefinition("c_acctbal", TypeId::DECIMAL, false),
	    ColumnDefinition("c_mktsegment", TypeId::VARCHAR, false),
	    ColumnDefinition("c_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnDefinition> PartColumns() {
	return vector<ColumnDefinition>{
	    ColumnDefinition("p_partkey", TypeId::INTEGER, false),
	    ColumnDefinition("p_name", TypeId::VARCHAR, false),
	    ColumnDefinition("p_mfgr", TypeId::VARCHAR, false),
	    ColumnDefinition("p_brand", TypeId::VARCHAR, false),
	    ColumnDefinition("p_type", TypeId::VARCHAR, false),
	    ColumnDefinition("p_size", TypeId::INTEGER, false),
	    ColumnDefinition("p_container", TypeId::VARCHAR, false),
	    ColumnDefinition("p_retailprice", TypeId::DECIMAL, false),
	    ColumnDefinition("p_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnDefinition> PartSuppColumns() {
	return vector<ColumnDefinition>{
	    ColumnDefinition("ps_partkey", TypeId::INTEGER, false),
	    ColumnDefinition("ps_suppkey", TypeId::INTEGER, false),
	    ColumnDefinition("ps_availqty", TypeId::INTEGER, false),
	    ColumnDefinition("ps_supplycost", TypeId::DECIMAL, false),
	    ColumnDefinition("ps_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnDefinition> OrdersColumns() {
	return vector<ColumnDefinition>{
	    ColumnDefinition("o_orderkey", TypeId::INTEGER, false),
	    ColumnDefinition("o_custkey", TypeId::INTEGER, false),
	    ColumnDefinition("o_orderstatus", TypeId::VARCHAR, false),
	    ColumnDefinition("o_totalprice", TypeId::DECIMAL, false),
	    ColumnDefinition("o_orderdate", TypeId::DATE, false),
	    ColumnDefinition("o_orderpriority", TypeId::VARCHAR, false),
	    ColumnDefinition("o_clerk", TypeId::VARCHAR, false),
	    ColumnDefinition("o_shippriority", TypeId::INTEGER, false),
	    ColumnDefinition("o_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnDefinition> LineitemColumns() {
	return vector<ColumnDefinition>{
	    ColumnDefinition("l_orderkey", TypeId::INTEGER, false),
	    ColumnDefinition("l_partkey", TypeId::INTEGER, false),
	    ColumnDefinition("l_suppkey", TypeId::INTEGER, false),
	    ColumnDefinition("l_linenumber", TypeId::INTEGER, false),
	    ColumnDefinition("l_quantity", TypeId::INTEGER, false),
	    ColumnDefinition("l_extendedprice", TypeId::DECIMAL, false),
	    ColumnDefinition("l_discount", TypeId::DECIMAL, false),
	    ColumnDefinition("l_tax", TypeId::DECIMAL, false),
	    ColumnDefinition("l_returnflag", TypeId::VARCHAR, false),
	    ColumnDefinition("l_linestatus", TypeId::VARCHAR, false),
	    ColumnDefinition("l_shipdate", TypeId::DATE, false),
	    ColumnDefinition("l_commitdate", TypeId::DATE, false),
	    ColumnDefinition("l_receiptdate", TypeId::DATE, false),
	    ColumnDefinition("l_shipinstruct", TypeId::VARCHAR, false),
	    ColumnDefinition("l_shipmode", TypeId::VARCHAR, false),
	    ColumnDefinition("l_comment", TypeId::VARCHAR, false)};
}

typedef int64_t ds_key_t;

#define DECLARER
#include "build_support.h"
#include "params.h"

#include "tdefs.h"
#include "scaling.h"
#include "address.h"
#include "dist.h"
#include "genrand.h"


static void gen_tbl(int tabid, ds_key_t kFirstRow, ds_key_t kRowCount) {
	int direct, bIsVerbose, nLifeFreq, nMultiplier, nChild;
	ds_key_t i, kTotalRows;
	tdef *pT = getSimpleTdefsByNumber(tabid);
	tdef *pC;
	table_func_t *pF = getTdefFunctionsByNumber(tabid);

	kTotalRows = kRowCount;

	/**
	set the frequency of progress updates for verbose output
	to greater of 1000 and the scale base
	*/
	nLifeFreq = 1;
	char const *distname = "rowcounts";
	nMultiplier = dist_member(NULL, (char *)distname, tabid + 1, 2);
	for (i = 0; nLifeFreq < nMultiplier; i++)
		nLifeFreq *= 10;
	if (nLifeFreq < 1000)
		nLifeFreq = 1000;

	/*
	 * small tables use a constrained set of geography information
	 */
	if (pT->flags & FL_SMALL)
		resetCountCount();

	for (i = kFirstRow; kRowCount; i++, kRowCount--) {
		/* not all rows that are built should be printed. Use return code to
		 * deterine output */
		if (!pF->builder(NULL, i))
			if (pF->loader[direct](NULL)) {
				throw Exception("Table generation failed");
			}
	}

	return;
}

void dbgen(double flt_scale, DuckDB &db, string schema, string suffix) {
	DuckDBConnection con(db);
	ClientContext &context = con.context;

	// FIXME: use a transaction for the load
	// FIXME: no schema/suffix support yet
	for (int t = 0; t < TPCDS_TABLE_COUNT; t++) {
		con.Query(TPCDS_TABLE_DDL[t]);
	}

	if (flt_scale == 0) {
		// schema only
		context.transaction.Commit();
		return;
	}

	init_rand(); // no random numbers without this
	tdef *pT;
	for (int table_id = CALL_CENTER; (pT = getSimpleTdefsByNumber(table_id)); table_id++) {
		if (!pT->name)
			break;

		// child tables are created in parent loaders
		if (pT->flags & FL_CHILD) {
			continue;
		}

//#define CALL_CENTER 0
//#define CATALOG_PAGE 1
//#define CATALOG_RETURNS 2
//#define CATALOG_SALES 3
//#define CUSTOMER 4
//#define CUSTOMER_ADDRESS 5
//#define CUSTOMER_DEMOGRAPHICS 6
//#define DATET 7
//#define HOUSEHOLD_DEMOGRAPHICS 8
//#define INCOME_BAND 9
//#define INVENTORY 10
//#define ITEM 11
//#define PROMOTION 12
//#define REASON 13
//#define SHIP_MODE 14
//#define STORE 15
//#define STORE_RETURNS 16
//#define STORE_SALES 17
//#define TIME 18
//#define WAREHOUSE 19
//#define WEB_PAGE 20
//#define WEB_RETURNS 21
//#define WEB_SALES 22
//#define WEB_SITE 23
//#define DBGEN_VERSION 24

		table_func_t *pF = getTdefFunctionsByNumber(table_id);

		// TODO: verify this is correct and required here
		/*
		 * small tables use a constrained set of geography information
		 */
		if (pT->flags & FL_SMALL)
			resetCountCount();

		for (ds_key_t i = 1, kRowCount = get_rowcount(table_id); kRowCount; i++, kRowCount--) {
			/* not all rows that are built should be printed. Use return code to
			 * deterine output */
			if (!pF->builder(NULL, i))
				if (pF->loader[1](NULL)) {
					throw Exception("Table generation failed");
				}
		}
		return;
	}

	//context.transaction.Commit();
}

string get_query(int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-DS query number %d", query);
	}
	return TPCDS_QUERIES[query - 1];
}

string get_answer(double sf, int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-DS query number %d", query);
	}
	const char *answer;
	return "";
}

} // namespace tpcds
