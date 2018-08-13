
#include "dbgen.hpp"
#include "common/exception.hpp"
#include "dbgen_gunk.hpp"

#include "storage/data_table.hpp"

#include "tpch_constants.hpp"

#define DECLARER /* EXTERN references get defined here */

#include "dss.h"
#include "dsstypes.h"

using namespace duckdb;
using namespace std;

extern seed_t Seed[];
seed_t seed_backup[MAX_STREAM + 1];
static bool first_invocation = true;

tdef tdefs[] = {
    {"part.tbl", "part table", 200000, NULL, NULL, PSUPP, 0},
    {"partsupp.tbl", "partsupplier table", 200000, NULL, NULL, NONE, 0},
    {"supplier.tbl", "suppliers table", 10000, NULL, NULL, NONE, 0},
    {"customer.tbl", "customers table", 150000, NULL, NULL, NONE, 0},
    {"orders.tbl", "order table", 150000, NULL, NULL, LINE, 0},
    {"lineitem.tbl", "lineitem table", 150000, NULL, NULL, NONE, 0},
    {"orders.tbl", "orders/lineitem tables", 150000, NULL, NULL, LINE, 0},
    {"part.tbl", "part/partsupplier tables", 200000, NULL, NULL, PSUPP, 0},
    {"nation.tbl", "nation table", NATIONS_MAX, NULL, NULL, NONE, 0},
    {"region.tbl", "region table", NATIONS_MAX, NULL, NULL, NONE, 0},
};

namespace tpch {

struct tpch_append_information {
	shared_ptr<TableCatalogEntry> table;
	DataChunk chunk;
};

void append_value(DataChunk &chunk, size_t index, size_t &column,
                  int32_t value) {
	chunk.data[column++]->SetValue(index, Value::INTEGER(value));
}

void append_string(DataChunk &chunk, size_t index, size_t &column,
                   string value) {
	chunk.data[column++]->SetValue(index, Value(value));
}

void append_decimal(DataChunk &chunk, size_t index, size_t &column,
                    int64_t value) {
	chunk.data[column++]->SetValue(index, Value((double)value / 100.0));
}

void append_date(DataChunk &chunk, size_t index, size_t &column, string value) {
	chunk.data[column++]->SetValue(index, Value::DATE(Date::FromString(value)));
}

void append_char(DataChunk &chunk, size_t index, size_t &column, char value) {
	chunk.data[column++]->SetValue(index, Value(string(1, value)));
}

static void append_to_append_info(tpch_append_information &info) {
	auto &chunk = info.chunk;
	auto &table = info.table;
	if (chunk.maximum_size >= chunk.count) {
		// have to make a new chunk, initialize it
		if (chunk.count > 0) {
			// flush the chunk
			table->storage->AddData(chunk);
		}
		auto types = table->GetTypes();
		chunk.Initialize(types);
	}
	chunk.count++;
	for (size_t i = 0; i < chunk.column_count; i++) {
		chunk.data[i]->count = chunk.count;
	}
}

static void append_order(order_t *o, tpch_append_information *info) {
	auto &append_info = info[ORDER];
	auto &chunk = append_info.chunk;
	append_to_append_info(append_info);
	size_t index = chunk.count - 1;
	size_t column = 0;

	// fill the current row with the order information
	// o_orderkey
	append_value(chunk, index, column, o->okey);
	// o_custkey
	append_value(chunk, index, column, o->custkey);
	// o_orderstatus
	append_char(chunk, index, column, o->orderstatus);
	// o_totalprice
	append_decimal(chunk, index, column, o->totalprice);
	// o_orderdate
	append_date(chunk, index, column, o->odate);
	// o_orderpriority
	append_string(chunk, index, column, string(o->opriority));
	// o_clerk
	append_string(chunk, index, column, string(o->clerk));
	// o_shippriority
	append_value(chunk, index, column, o->spriority);
	// o_comment
	append_string(chunk, index, column, string(o->comment));
}

static void append_line(order_t *o, tpch_append_information *info) {
	auto &append_info = info[LINE];
	auto &chunk = append_info.chunk;

	// fill the current row with the order information
	for (DSS_HUGE i = 0; i < o->lines; i++) {
		append_to_append_info(append_info);
		size_t index = chunk.count - 1;
		size_t column = 0;

		// l_orderkey
		append_value(chunk, index, column, o->l[i].okey);
		// l_partkey
		append_value(chunk, index, column, o->l[i].partkey);
		// l_suppkey
		append_value(chunk, index, column, o->l[i].suppkey);
		// l_linenumber
		append_value(chunk, index, column, o->l[i].lcnt);
		// l_quantity
		append_value(chunk, index, column, o->l[i].quantity);
		// l_extendedprice
		append_decimal(chunk, index, column, o->l[i].eprice);
		// l_discount
		append_decimal(chunk, index, column, o->l[i].discount);
		// l_tax
		append_decimal(chunk, index, column, o->l[i].tax);
		// l_returnflag
		append_char(chunk, index, column, o->l[i].rflag[0]);
		// l_linestatus
		append_char(chunk, index, column, o->l[i].lstatus[0]);
		// l_shipdate
		append_date(chunk, index, column, o->l[i].sdate);
		// l_commitdate
		append_date(chunk, index, column, o->l[i].cdate);
		// l_receiptdate
		append_date(chunk, index, column, o->l[i].rdate);
		// l_shipinstruct
		append_string(chunk, index, column, string(o->l[i].shipinstruct));
		// l_shipmode
		append_string(chunk, index, column, string(o->l[i].shipmode));
		// l_comment
		append_string(chunk, index, column, string(o->l[i].comment));
	}
}

static void append_order_line(order_t *o, tpch_append_information *info) {
	append_order(o, info);
	append_line(o, info);
}

static void append_supp(supplier_t *supp, tpch_append_information *info) {
	auto &append_info = info[SUPP];
	auto &chunk = append_info.chunk;
	append_to_append_info(append_info);
	size_t index = chunk.count - 1;
	size_t column = 0;

	// s_suppkey
	append_value(chunk, index, column, supp->suppkey);
	// s_name
	append_string(chunk, index, column, supp->name);
	// s_address
	append_string(chunk, index, column, supp->address);
	// s_nationkey
	append_value(chunk, index, column, supp->nation_code);
	// s_phone
	append_string(chunk, index, column, supp->phone);
	// s_acctbal
	append_decimal(chunk, index, column, supp->acctbal);
	// s_comment
	append_string(chunk, index, column, supp->comment);
}

static void append_cust(customer_t *c, tpch_append_information *info) {
	auto &append_info = info[CUST];
	auto &chunk = append_info.chunk;
	append_to_append_info(append_info);
	size_t index = chunk.count - 1;
	size_t column = 0;

	// c_custkey
	append_value(chunk, index, column, c->custkey);
	// c_name
	append_string(chunk, index, column, c->name);
	// c_address
	append_string(chunk, index, column, c->address);
	// c_nationkey
	append_value(chunk, index, column, c->nation_code);
	// c_phone
	append_string(chunk, index, column, c->phone);
	// c_acctbal
	append_decimal(chunk, index, column, c->acctbal);
	// c_mktsegment
	append_string(chunk, index, column, c->mktsegment);
	// c_comment
	append_string(chunk, index, column, c->comment);
}

static void append_part(part_t *part, tpch_append_information *info) {
	auto &append_info = info[PART];
	auto &chunk = append_info.chunk;
	append_to_append_info(append_info);
	size_t index = chunk.count - 1;
	size_t column = 0;

	// p_partkey
	append_value(chunk, index, column, part->partkey);
	// p_name
	append_string(chunk, index, column, part->name);
	// p_mfgr
	append_string(chunk, index, column, part->mfgr);
	// p_brand
	append_string(chunk, index, column, part->brand);
	// p_type
	append_string(chunk, index, column, part->type);
	// p_size
	append_value(chunk, index, column, part->size);
	// p_container
	append_string(chunk, index, column, part->container);
	// p_retailprice
	append_decimal(chunk, index, column, part->retailprice);
	// p_comment
	append_string(chunk, index, column, part->comment);
}

static void append_psupp(part_t *part, tpch_append_information *info) {
	auto &append_info = info[PSUPP];
	auto &chunk = append_info.chunk;
	for (size_t i = 0; i < SUPP_PER_PART; i++) {
		append_to_append_info(append_info);
		size_t index = chunk.count - 1;
		size_t column = 0;

		// ps_partkey
		append_value(chunk, index, column, part->s[i].partkey);
		// ps_suppkey
		append_value(chunk, index, column, part->s[i].suppkey);
		// ps_availqty
		append_value(chunk, index, column, part->s[i].qty);
		// ps_supplycost
		append_decimal(chunk, index, column, part->s[i].scost);
		// ps_comment
		append_string(chunk, index, column, part->s[i].comment);
	}
}

static void append_part_psupp(part_t *part, tpch_append_information *info) {
	append_part(part, info);
	append_psupp(part, info);
}

static void append_nation(code_t *c, tpch_append_information *info) {
	auto &append_info = info[NATION];
	auto &chunk = append_info.chunk;
	append_to_append_info(append_info);
	size_t index = chunk.count - 1;
	size_t column = 0;

	// n_nationkey
	append_value(chunk, index, column, c->code);
	// n_name
	append_string(chunk, index, column, c->text);
	// n_regionkey
	append_value(chunk, index, column, c->join);
	// n_comment
	append_string(chunk, index, column, c->comment);
}

static void append_region(code_t *c, tpch_append_information *info) {
	auto &append_info = info[REGION];
	auto &chunk = append_info.chunk;
	append_to_append_info(append_info);
	size_t index = chunk.count - 1;
	size_t column = 0;

	// r_regionkey
	append_value(chunk, index, column, c->code);
	// r_name
	append_string(chunk, index, column, c->text);
	// r_comment
	append_string(chunk, index, column, c->comment);
}

static void gen_tbl(int tnum, DSS_HUGE count, tpch_append_information *info) {
	order_t o;
	supplier_t supp;
	customer_t cust;
	part_t part;
	code_t code;

	for (DSS_HUGE i = 1; count; count--, i++) {
		row_start(tnum);
		switch (tnum) {
		case LINE:
		case ORDER:
		case ORDER_LINE:
			mk_order(i, &o, 0);
			append_order_line(&o, info);
			break;
		case SUPP:
			mk_supp(i, &supp);
			append_supp(&supp, info);
			break;
		case CUST:
			mk_cust(i, &cust);
			append_cust(&cust, info);
			break;
		case PSUPP:
		case PART:
		case PART_PSUPP:
			mk_part(i, &part);
			append_part_psupp(&part, info);
			break;
		case NATION:
			mk_nation(i, &code);
			append_nation(&code, info);
			break;
		case REGION:
			mk_region(i, &code);
			append_region(&code, info);
			break;
		}
		row_stop(tnum);
	}
}

string get_table_name(int num) {
	switch (num) {
	case PART:
		return "part";
	case PSUPP:
		return "partsupp";
	case SUPP:
		return "supplier";
	case CUST:
		return "customer";
	case ORDER:
		return "orders";
	case LINE:
		return "lineitem";
	case NATION:
		return "nation";
	case REGION:
		return "region";
	default:
		return "";
	}
}

static vector<ColumnCatalogEntry> RegionColumns() {
	return vector<ColumnCatalogEntry>{
	    ColumnCatalogEntry("r_regionkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("r_name", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("r_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnCatalogEntry> NationColumns() {
	return vector<ColumnCatalogEntry>{
	    ColumnCatalogEntry("n_nationkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("n_name", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("n_regionkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("n_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnCatalogEntry> SupplierColumns() {
	return vector<ColumnCatalogEntry>{
	    ColumnCatalogEntry("s_suppkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("s_name", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("s_address", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("s_nationkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("s_phone", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("s_acctbal", TypeId::DECIMAL, false),
	    ColumnCatalogEntry("s_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnCatalogEntry> CustomerColumns() {
	return vector<ColumnCatalogEntry>{
	    ColumnCatalogEntry("c_custkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("c_name", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("c_address", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("c_nationkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("c_phone", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("c_acctbal", TypeId::DECIMAL, false),
	    ColumnCatalogEntry("c_mktsegment", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("c_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnCatalogEntry> PartColumns() {
	return vector<ColumnCatalogEntry>{
	    ColumnCatalogEntry("p_partkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("p_name", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("p_mfgr", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("p_brand", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("p_type", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("p_size", TypeId::INTEGER, false),
	    ColumnCatalogEntry("p_container", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("p_retailprice", TypeId::DECIMAL, false),
	    ColumnCatalogEntry("p_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnCatalogEntry> PartSuppColumns() {
	return vector<ColumnCatalogEntry>{
	    ColumnCatalogEntry("ps_partkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("ps_suppkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("ps_availqty", TypeId::INTEGER, false),
	    ColumnCatalogEntry("ps_supplycost", TypeId::DECIMAL, false),
	    ColumnCatalogEntry("ps_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnCatalogEntry> OrdersColumns() {
	return vector<ColumnCatalogEntry>{
	    ColumnCatalogEntry("o_orderkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("o_custkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("o_orderstatus", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("o_totalprice", TypeId::DECIMAL, false),
	    ColumnCatalogEntry("o_orderdate", TypeId::DATE, false),
	    ColumnCatalogEntry("o_orderpriority", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("o_clerk", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("o_shippriority", TypeId::INTEGER, false),
	    ColumnCatalogEntry("o_comment", TypeId::VARCHAR, false)};
}

static vector<ColumnCatalogEntry> LineitemColumns() {
	return vector<ColumnCatalogEntry>{
	    ColumnCatalogEntry("l_orderkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("l_partkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("l_suppkey", TypeId::INTEGER, false),
	    ColumnCatalogEntry("l_linenumber", TypeId::INTEGER, false),
	    ColumnCatalogEntry("l_quantity", TypeId::INTEGER, false),
	    ColumnCatalogEntry("l_extendedprice", TypeId::DECIMAL, false),
	    ColumnCatalogEntry("l_discount", TypeId::DECIMAL, false),
	    ColumnCatalogEntry("l_tax", TypeId::DECIMAL, false),
	    ColumnCatalogEntry("l_returnflag", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("l_linestatus", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("l_shipdate", TypeId::DATE, false),
	    ColumnCatalogEntry("l_commitdate", TypeId::DATE, false),
	    ColumnCatalogEntry("l_receiptdate", TypeId::DATE, false),
	    ColumnCatalogEntry("l_shipinstruct", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("l_shipmode", TypeId::VARCHAR, false),
	    ColumnCatalogEntry("l_comment", TypeId::VARCHAR, false)};
}

void dbgen(double flt_scale, Catalog &catalog, string schema, string suffix) {
	catalog.CreateTable(schema, "region" + suffix, RegionColumns());
	catalog.CreateTable(schema, "nation" + suffix, NationColumns());
	catalog.CreateTable(schema, "supplier" + suffix, SupplierColumns());
	catalog.CreateTable(schema, "customer" + suffix, CustomerColumns());
	catalog.CreateTable(schema, "part" + suffix, PartColumns());
	catalog.CreateTable(schema, "partsupp" + suffix, PartSuppColumns());
	catalog.CreateTable(schema, "orders" + suffix, OrdersColumns());
	catalog.CreateTable(schema, "lineitem" + suffix, LineitemColumns());

	if (flt_scale == 0) {
		// schema only
		return;
	}

	// generate the actual data
	DSS_HUGE rowcnt = 0;
	DSS_HUGE i;
	// all tables
	table = (1 << CUST) | (1 << SUPP) | (1 << NATION) | (1 << REGION) |
	        (1 << PART_PSUPP) | (1 << ORDER_LINE);
	force = 0;
	insert_segments = 0;
	delete_segments = 0;
	insert_orders_segment = 0;
	insert_lineitem_segment = 0;
	delete_segment = 0;
	verbose = 0;
	set_seeds = 0;
	scale = 1;
	updates = 0;

	// check if it is the first invocation
	if (first_invocation) {
		// store the initial random seed
		memcpy(seed_backup, Seed, sizeof(seed_t) * MAX_STREAM + 1);
		first_invocation = false;
	} else {
		// restore random seeds from backup
		memcpy(Seed, seed_backup, sizeof(seed_t) * MAX_STREAM + 1);
	}
	tdefs[PART].base = 200000;
	tdefs[PSUPP].base = 200000;
	tdefs[SUPP].base = 10000;
	tdefs[CUST].base = 150000;
	tdefs[ORDER].base = 150000 * ORDERS_PER_CUST;
	tdefs[LINE].base = 150000 * ORDERS_PER_CUST;
	tdefs[ORDER_LINE].base = 150000 * ORDERS_PER_CUST;
	tdefs[PART_PSUPP].base = 200000;
	tdefs[NATION].base = NATIONS_MAX;
	tdefs[REGION].base = NATIONS_MAX;

	children = 1;
	d_path = NULL;

	if (flt_scale < MIN_SCALE) {
		int i;
		int int_scale;

		scale = 1;
		int_scale = (int)(1000 * flt_scale);
		for (i = PART; i < REGION; i++) {
			tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base) / 1000;
			if (tdefs[i].base < 1)
				tdefs[i].base = 1;
		}
	} else {
		scale = (long)flt_scale;
	}

	load_dists();

	/* have to do this after init */
	tdefs[NATION].base = nations.count;
	tdefs[REGION].base = regions.count;

	auto append_info = unique_ptr<tpch_append_information[]>(
	    new tpch_append_information[REGION + 1]);
	for (size_t i = PART; i <= REGION; i++) {
		auto tname = get_table_name(i);
		if (!tname.empty()) {
			append_info[i].table = catalog.GetTable(schema, tname + suffix);
		}
	}

	for (i = PART; i <= REGION; i++) {
		if (table & (1 << i)) {
			if (i < NATION) {
				rowcnt = tdefs[i].base * scale;
			} else {
				rowcnt = tdefs[i].base;
			}
			// actually doing something
			gen_tbl((int)i, rowcnt, append_info.get());
		}
	}
	// flush any incomplete chunks
	for (size_t i = PART; i <= REGION; i++) {
		if (append_info[i].table) {
			if (append_info[i].chunk.count > 0) {
				append_info[i].table->storage->AddData(append_info[i].chunk);
			}
		}
	}

	cleanup_dists();
}

string get_query(int query) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
		throw Exception("Out of range TPC-H query number %d", query);
	}
	return TPCH_QUERIES[query - 1];
}

static bool compare_result(const char *csv, DataChunk &result,
                           std::string &error_message) {
	auto types = result.GetTypes();

	std::istringstream f(csv);
	std::string line;
	size_t row = 0;
	/// read and parse the header line
	std::getline(f, line);
	// check if the column length matches
	auto split = StringUtil::Split(line, '|');
	if (split.size() != types.size()) {
		// column length is different
		goto incorrect;
	}
	// now compare the actual data
	while (std::getline(f, line)) {
		if (result.count <= row) {
			goto incorrect;
		}
		split = StringUtil::Split(line, '|');
		if (split.size() != types.size()) {
			// column length is different
			goto incorrect;
		}
		// now compare the values
		for (size_t i = 0; i < split.size(); i++) {
			// first create a string value
			Value value(split[i]);
			// cast to the type of the column
			try {
				value = value.CastAs(types[i]);
			} catch (...) {
				goto incorrect;
			}
			// now perform a comparison
			if (!Value::Equals(value, result.data[i]->GetValue(row))) {
				goto incorrect;
			}
		}
		row++;
	}
	if (result.count != row) {
		goto incorrect;
	}
	return true;
incorrect:
	error_message = "Incorrect answer for query!\nProvided answer:\n" +
	                result.ToString() + "\nExpected answer:\n" + string(csv) +
	                "\n";
	return false;
}

bool check_result(double sf, int query, DuckDBResult &result,
                  std::string &error_message) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
		throw Exception("Out of range TPC-H query number %d", query);
	}
	if (!result.GetSuccess()) {
		error_message = result.GetErrorMessage();
		return false;
	}
	const char *answer;
	if (sf == 0.1) {
		answer = TPCH_ANSWERS_SF0_1[query - 1];
	} else if (sf == 1) {
		answer = TPCH_ANSWERS_SF1[query - 1];
	} else {
		throw Exception("Don't have TPC-H answers for SF %llf!", sf);
	}
	DataChunk big_chunk;
	result.GatherResult(big_chunk);
	return compare_result(answer, big_chunk, error_message);
}

} // namespace tpch
