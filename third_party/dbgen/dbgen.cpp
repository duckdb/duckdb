#include "dbgen.hpp"

#include "common/exception.hpp"
#include "dbgen_gunk.hpp"
#include "main/client_context.hpp"
#include "storage/data_table.hpp"
#include "tpch_constants.hpp"
#include "common/types/date.hpp"

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
	TableCatalogEntry *table;
	DataChunk chunk;
	ClientContext *context;
};

void append_value(DataChunk &chunk, size_t index, size_t &column, int32_t value) {
	((int32_t *)chunk.data[column++].data)[index] = value;
}

void append_string(DataChunk &chunk, size_t index, size_t &column, const char *value) {
	chunk.data[column++].SetStringValue(index, value);
}

void append_decimal(DataChunk &chunk, size_t index, size_t &column, int64_t value) {
	((double *)chunk.data[column++].data)[index] = (double)value / 100.0;
}

void append_date(DataChunk &chunk, size_t index, size_t &column, string value) {
	((date_t *)chunk.data[column++].data)[index] = Date::FromString(value);
}

void append_char(DataChunk &chunk, size_t index, size_t &column, char value) {
	char val[2];
	val[0] = value;
	val[1] = '\0';
	append_string(chunk, index, column, val);
}

static void append_to_append_info(tpch_append_information &info) {
	auto &chunk = info.chunk;
	auto &table = info.table;
	if (chunk.column_count == 0) {
		// initalize the chunk
		auto types = table->GetTypes();
		chunk.Initialize(types);
	} else if (chunk.size() >= STANDARD_VECTOR_SIZE) {
		// flush the chunk
		table->storage->Append(*table, *info.context, chunk);
		// have to reset the chunk
		chunk.Reset();
	}
	for (size_t i = 0; i < chunk.column_count; i++) {
		chunk.data[i].count++;
	}
}

static void append_order(order_t *o, tpch_append_information *info) {
	auto &append_info = info[ORDER];
	auto &chunk = append_info.chunk;
	append_to_append_info(append_info);
	size_t index = chunk.size() - 1;
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
	append_string(chunk, index, column, o->opriority);
	// o_clerk
	append_string(chunk, index, column, o->clerk);
	// o_shippriority
	append_value(chunk, index, column, o->spriority);
	// o_comment
	append_string(chunk, index, column, o->comment);
}

static void append_line(order_t *o, tpch_append_information *info) {
	auto &append_info = info[LINE];
	auto &chunk = append_info.chunk;

	// fill the current row with the order information
	for (DSS_HUGE i = 0; i < o->lines; i++) {
		append_to_append_info(append_info);
		size_t index = chunk.size() - 1;
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
		append_string(chunk, index, column, o->l[i].shipinstruct);
		// l_shipmode
		append_string(chunk, index, column, o->l[i].shipmode);
		// l_comment
		append_string(chunk, index, column, o->l[i].comment);
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
	size_t index = chunk.size() - 1;
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
	size_t index = chunk.size() - 1;
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
	size_t index = chunk.size() - 1;
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
		size_t index = chunk.size() - 1;
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
	size_t index = chunk.size() - 1;
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
	size_t index = chunk.size() - 1;
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
		row_stop_h(tnum);
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

static vector<ColumnDefinition> RegionColumns() {
	return vector<ColumnDefinition>{ColumnDefinition("r_regionkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("r_name", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("r_comment", SQLType(SQLTypeId::VARCHAR), false)};
}

static vector<ColumnDefinition> NationColumns() {
	return vector<ColumnDefinition>{ColumnDefinition("n_nationkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("n_name", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("n_regionkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("n_comment", SQLType(SQLTypeId::VARCHAR), false)};
}

static vector<ColumnDefinition> SupplierColumns() {
	return vector<ColumnDefinition>{ColumnDefinition("s_suppkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("s_name", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("s_address", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("s_nationkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("s_phone", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("s_acctbal", SQLType(SQLTypeId::DECIMAL), false),
	                                ColumnDefinition("s_comment", SQLType(SQLTypeId::VARCHAR), false)};
}

static vector<ColumnDefinition> CustomerColumns() {
	return vector<ColumnDefinition>{ColumnDefinition("c_custkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("c_name", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("c_address", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("c_nationkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("c_phone", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("c_acctbal", SQLType(SQLTypeId::DECIMAL), false),
	                                ColumnDefinition("c_mktsegment", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("c_comment", SQLType(SQLTypeId::VARCHAR), false)};
}

static vector<ColumnDefinition> PartColumns() {
	return vector<ColumnDefinition>{ColumnDefinition("p_partkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("p_name", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("p_mfgr", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("p_brand", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("p_type", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("p_size", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("p_container", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("p_retailprice", SQLType(SQLTypeId::DECIMAL), false),
	                                ColumnDefinition("p_comment", SQLType(SQLTypeId::VARCHAR), false)};
}

static vector<ColumnDefinition> PartSuppColumns() {
	return vector<ColumnDefinition>{ColumnDefinition("ps_partkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("ps_suppkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("ps_availqty", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("ps_supplycost", SQLType(SQLTypeId::DECIMAL), false),
	                                ColumnDefinition("ps_comment", SQLType(SQLTypeId::VARCHAR), false)};
}

static vector<ColumnDefinition> OrdersColumns() {
	return vector<ColumnDefinition>{ColumnDefinition("o_orderkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("o_custkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("o_orderstatus", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("o_totalprice", SQLType(SQLTypeId::DECIMAL), false),
	                                ColumnDefinition("o_orderdate", SQLType(SQLTypeId::DATE), false),
	                                ColumnDefinition("o_orderpriority", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("o_clerk", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("o_shippriority", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("o_comment", SQLType(SQLTypeId::VARCHAR), false)};
}

static vector<ColumnDefinition> LineitemColumns() {
	return vector<ColumnDefinition>{ColumnDefinition("l_orderkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("l_partkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("l_suppkey", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("l_linenumber", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("l_quantity", SQLType(SQLTypeId::INTEGER), false),
	                                ColumnDefinition("l_extendedprice", SQLType(SQLTypeId::DECIMAL), false),
	                                ColumnDefinition("l_discount", SQLType(SQLTypeId::DECIMAL), false),
	                                ColumnDefinition("l_tax", SQLType(SQLTypeId::DECIMAL), false),
	                                ColumnDefinition("l_returnflag", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("l_linestatus", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("l_shipdate", SQLType(SQLTypeId::DATE), false),
	                                ColumnDefinition("l_commitdate", SQLType(SQLTypeId::DATE), false),
	                                ColumnDefinition("l_receiptdate", SQLType(SQLTypeId::DATE), false),
	                                ColumnDefinition("l_shipinstruct", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("l_shipmode", SQLType(SQLTypeId::VARCHAR), false),
	                                ColumnDefinition("l_comment", SQLType(SQLTypeId::VARCHAR), false)};
}

void dbgen(double flt_scale, DuckDB &db, string schema, string suffix) {
	ClientContext context(db);
	context.transaction.BeginTransaction();

	auto &transaction = context.ActiveTransaction();

	CreateTableInformation region(schema, "region" + suffix, RegionColumns());
	CreateTableInformation supplier(schema, "supplier" + suffix, SupplierColumns());
	CreateTableInformation customer(schema, "customer" + suffix, CustomerColumns());
	CreateTableInformation nation(schema, "nation" + suffix, NationColumns());
	CreateTableInformation part(schema, "part" + suffix, PartColumns());
	CreateTableInformation partsupp(schema, "partsupp" + suffix, PartSuppColumns());
	CreateTableInformation orders(schema, "orders" + suffix, OrdersColumns());
	CreateTableInformation lineitem(schema, "lineitem" + suffix, LineitemColumns());

	db.catalog.CreateTable(transaction, &region);
	db.catalog.CreateTable(transaction, &supplier);
	db.catalog.CreateTable(transaction, &customer);
	db.catalog.CreateTable(transaction, &nation);
	db.catalog.CreateTable(transaction, &part);
	db.catalog.CreateTable(transaction, &partsupp);
	db.catalog.CreateTable(transaction, &orders);
	db.catalog.CreateTable(transaction, &lineitem);

	if (flt_scale == 0) {
		// schema only
		context.transaction.Commit();
		return;
	}

	// generate the actual data
	DSS_HUGE rowcnt = 0;
	DSS_HUGE i;
	// all tables
	table = (1 << CUST) | (1 << SUPP) | (1 << NATION) | (1 << REGION) | (1 << PART_PSUPP) | (1 << ORDER_LINE);
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

	auto append_info = unique_ptr<tpch_append_information[]>(new tpch_append_information[REGION + 1]);
	for (size_t i = PART; i <= REGION; i++) {
		auto tname = get_table_name(i);
		if (!tname.empty()) {
			append_info[i].table = db.catalog.GetTable(transaction, schema, tname + suffix);
		}
		append_info[i].context = &context;
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
			if (append_info[i].chunk.size() > 0) {
				append_info[i].table->storage->Append(*append_info[i].table, *append_info[i].context,
				                                      append_info[i].chunk);
			}
		}
	}

	cleanup_dists();
	context.transaction.Commit();
}

string get_query(int query) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-H query number %d", query);
	}
	return TPCH_QUERIES[query - 1];
}

string get_answer(double sf, int query) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-H query number %d", query);
	}
	const char *answer;
	if (sf == 0.1) {
		answer = TPCH_ANSWERS_SF0_1[query - 1];
	} else if (sf == 1) {
		answer = TPCH_ANSWERS_SF1[query - 1];
	} else {
		throw NotImplementedException("Don't have TPC-H answers for SF %llf!", sf);
	}
	return answer;
}

} // namespace tpch
