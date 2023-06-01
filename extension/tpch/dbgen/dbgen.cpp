#include "dbgen/dbgen.hpp"
#include "dbgen/dbgen_gunk.hpp"
#include "tpch_constants.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#endif

#define DECLARER /* EXTERN references get defined here */

#include "dbgen/dss.h"
#include "dbgen/dsstypes.h"

#include <cassert>
#include <cmath>
#include <mutex>

using namespace duckdb;

namespace tpch {

struct tpch_append_information {
	duckdb::unique_ptr<InternalAppender> appender;
};

void append_value(tpch_append_information &info, int32_t value) {
	info.appender->Append<int32_t>(value);
}

void append_string(tpch_append_information &info, const char *value) {
	info.appender->Append<const char *>(value);
}

void append_decimal(tpch_append_information &info, int64_t value) {
	info.appender->Append<int64_t>(value);
}

void append_date(tpch_append_information &info, string value) {
	info.appender->Append<date_t>(Date::FromString(value));
}

void append_char(tpch_append_information &info, char value) {
	char val[2];
	val[0] = value;
	val[1] = '\0';
	append_string(info, val);
}

static void append_order(order_t *o, tpch_append_information *info) {
	auto &append_info = info[ORDER];

	// fill the current row with the order information
	append_info.appender->BeginRow();
	// o_orderkey
	append_value(append_info, o->okey);
	// o_custkey
	append_value(append_info, o->custkey);
	// o_orderstatus
	append_char(append_info, o->orderstatus);
	// o_totalprice
	append_decimal(append_info, o->totalprice);
	// o_orderdate
	append_date(append_info, o->odate);
	// o_orderpriority
	append_string(append_info, o->opriority);
	// o_clerk
	append_string(append_info, o->clerk);
	// o_shippriority
	append_value(append_info, o->spriority);
	// o_comment
	append_string(append_info, o->comment);
	append_info.appender->EndRow();
}

static void append_line(order_t *o, tpch_append_information *info) {
	auto &append_info = info[LINE];

	// fill the current row with the order information
	for (DSS_HUGE i = 0; i < o->lines; i++) {
		append_info.appender->BeginRow();
		// l_orderkey
		append_value(append_info, o->l[i].okey);
		// l_partkey
		append_value(append_info, o->l[i].partkey);
		// l_suppkey
		append_value(append_info, o->l[i].suppkey);
		// l_linenumber
		append_value(append_info, o->l[i].lcnt);
		// l_quantity
		append_decimal(append_info, o->l[i].quantity);
		// l_extendedprice
		append_decimal(append_info, o->l[i].eprice);
		// l_discount
		append_decimal(append_info, o->l[i].discount);
		// l_tax
		append_decimal(append_info, o->l[i].tax);
		// l_returnflag
		append_char(append_info, o->l[i].rflag[0]);
		// l_linestatus
		append_char(append_info, o->l[i].lstatus[0]);
		// l_shipdate
		append_date(append_info, o->l[i].sdate);
		// l_commitdate
		append_date(append_info, o->l[i].cdate);
		// l_receiptdate
		append_date(append_info, o->l[i].rdate);
		// l_shipinstruct
		append_string(append_info, o->l[i].shipinstruct);
		// l_shipmode
		append_string(append_info, o->l[i].shipmode);
		// l_comment
		append_string(append_info, o->l[i].comment);
		append_info.appender->EndRow();
	}
}

static void append_order_line(order_t *o, tpch_append_information *info) {
	append_order(o, info);
	append_line(o, info);
}

static void append_supp(supplier_t *supp, tpch_append_information *info) {
	auto &append_info = info[SUPP];

	append_info.appender->BeginRow();
	// s_suppkey
	append_value(append_info, supp->suppkey);
	// s_name
	append_string(append_info, supp->name);
	// s_address
	append_string(append_info, supp->address);
	// s_nationkey
	append_value(append_info, supp->nation_code);
	// s_phone
	append_string(append_info, supp->phone);
	// s_acctbal
	append_decimal(append_info, supp->acctbal);
	// s_comment
	append_string(append_info, supp->comment);
	append_info.appender->EndRow();
}

static void append_cust(customer_t *c, tpch_append_information *info) {
	auto &append_info = info[CUST];

	append_info.appender->BeginRow();
	// c_custkey
	append_value(append_info, c->custkey);
	// c_name
	append_string(append_info, c->name);
	// c_address
	append_string(append_info, c->address);
	// c_nationkey
	append_value(append_info, c->nation_code);
	// c_phone
	append_string(append_info, c->phone);
	// c_acctbal
	append_decimal(append_info, c->acctbal);
	// c_mktsegment
	append_string(append_info, c->mktsegment);
	// c_comment
	append_string(append_info, c->comment);
	append_info.appender->EndRow();
}

static void append_part(part_t *part, tpch_append_information *info) {
	auto &append_info = info[PART];

	append_info.appender->BeginRow();
	// p_partkey
	append_value(append_info, part->partkey);
	// p_name
	append_string(append_info, part->name);
	// p_mfgr
	append_string(append_info, part->mfgr);
	// p_brand
	append_string(append_info, part->brand);
	// p_type
	append_string(append_info, part->type);
	// p_size
	append_value(append_info, part->size);
	// p_container
	append_string(append_info, part->container);
	// p_retailprice
	append_decimal(append_info, part->retailprice);
	// p_comment
	append_string(append_info, part->comment);
	append_info.appender->EndRow();
}

static void append_psupp(part_t *part, tpch_append_information *info) {
	auto &append_info = info[PSUPP];
	for (size_t i = 0; i < SUPP_PER_PART; i++) {
		append_info.appender->BeginRow();
		// ps_partkey
		append_value(append_info, part->s[i].partkey);
		// ps_suppkey
		append_value(append_info, part->s[i].suppkey);
		// ps_availqty
		append_value(append_info, part->s[i].qty);
		// ps_supplycost
		append_decimal(append_info, part->s[i].scost);
		// ps_comment
		append_string(append_info, part->s[i].comment);
		append_info.appender->EndRow();
	}
}

static void append_part_psupp(part_t *part, tpch_append_information *info) {
	append_part(part, info);
	append_psupp(part, info);
}

static void append_nation(code_t *c, tpch_append_information *info) {
	auto &append_info = info[NATION];

	append_info.appender->BeginRow();
	// n_nationkey
	append_value(append_info, c->code);
	// n_name
	append_string(append_info, c->text);
	// n_regionkey
	append_value(append_info, c->join);
	// n_comment
	append_string(append_info, c->comment);
	append_info.appender->EndRow();
}

static void append_region(code_t *c, tpch_append_information *info) {
	auto &append_info = info[REGION];

	append_info.appender->BeginRow();
	// r_regionkey
	append_value(append_info, c->code);
	// r_name
	append_string(append_info, c->text);
	// r_comment
	append_string(append_info, c->comment);
	append_info.appender->EndRow();
}

static void gen_tbl(int tnum, DSS_HUGE count, tpch_append_information *info, DBGenContext *dbgen_ctx,
                    idx_t offset = 0) {
	order_t o;
	supplier_t supp;
	customer_t cust;
	part_t part;
	code_t code;

	for (DSS_HUGE i = offset + 1; count; count--, i++) {
		row_start(tnum, dbgen_ctx);
		switch (tnum) {
		case LINE:
		case ORDER:
		case ORDER_LINE:
			mk_order(i, &o, dbgen_ctx, 0);
			append_order_line(&o, info);
			break;
		case SUPP:
			mk_supp(i, &supp, dbgen_ctx);
			append_supp(&supp, info);
			break;
		case CUST:
			mk_cust(i, &cust, dbgen_ctx);
			append_cust(&cust, info);
			break;
		case PSUPP:
		case PART:
		case PART_PSUPP:
			mk_part(i, &part, dbgen_ctx);
			append_part_psupp(&part, info);
			break;
		case NATION:
			mk_nation(i, &code, dbgen_ctx);
			append_nation(&code, info);
			break;
		case REGION:
			mk_region(i, &code, dbgen_ctx);
			append_region(&code, info);
			break;
		}
		row_stop_h(tnum, dbgen_ctx);
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

struct RegionInfo {
	static constexpr char *Name = "region";
	static constexpr idx_t ColumnCount = 3;
	static const char *Columns[];
	static const LogicalType Types[];
};
const char *RegionInfo::Columns[] = {"r_regionkey", "r_name", "r_comment"};
const LogicalType RegionInfo::Types[] = {LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR),
                                         LogicalType(LogicalTypeId::VARCHAR)};

struct NationInfo {
	static constexpr char *Name = "nation";
	static const char *Columns[];
	static constexpr idx_t ColumnCount = 4;
	static const LogicalType Types[];
};
const char *NationInfo::Columns[] = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
const LogicalType NationInfo::Types[] = {LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR),
                                         LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR)};

struct SupplierInfo {
	static constexpr char *Name = "supplier";
	static const char *Columns[];
	static constexpr idx_t ColumnCount = 7;
	static const LogicalType Types[];
};
const char *SupplierInfo::Columns[] = {"s_suppkey", "s_name",    "s_address", "s_nationkey",
                                       "s_phone",   "s_acctbal", "s_comment"};
const LogicalType SupplierInfo::Types[] = {LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::INTEGER),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType::DECIMAL(15, 2),
                                           LogicalType(LogicalTypeId::VARCHAR)};

struct CustomerInfo {
	static constexpr char *Name = "customer";
	static const char *Columns[];
	static constexpr idx_t ColumnCount = 8;
	static const LogicalType Types[];
};
const char *CustomerInfo::Columns[] = {"c_custkey", "c_name",    "c_address",    "c_nationkey",
                                       "c_phone",   "c_acctbal", "c_mktsegment", "c_comment"};
const LogicalType CustomerInfo::Types[] = {LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::INTEGER),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType::DECIMAL(15, 2),
                                           LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)};

struct PartInfo {
	static constexpr char *Name = "part";
	static const char *Columns[];
	static constexpr idx_t ColumnCount = 9;
	static const LogicalType Types[];
};
const char *PartInfo::Columns[] = {"p_partkey", "p_name",      "p_mfgr",        "p_brand",  "p_type",
                                   "p_size",    "p_container", "p_retailprice", "p_comment"};
const LogicalType PartInfo::Types[] = {
    LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
    LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::INTEGER),
    LogicalType(LogicalTypeId::VARCHAR), LogicalType::DECIMAL(15, 2),         LogicalType(LogicalTypeId::VARCHAR)};

struct PartsuppInfo {
	static constexpr char *Name = "partsupp";
	static const char *Columns[];
	static constexpr idx_t ColumnCount = 5;
	static const LogicalType Types[];
};
const char *PartsuppInfo::Columns[] = {"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"};
const LogicalType PartsuppInfo::Types[] = {LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER),
                                           LogicalType(LogicalTypeId::INTEGER), LogicalType::DECIMAL(15, 2),
                                           LogicalType(LogicalTypeId::VARCHAR)};

struct OrdersInfo {
	static constexpr char *Name = "orders";
	static const char *Columns[];
	static constexpr idx_t ColumnCount = 9;
	static const LogicalType Types[];
};
const char *OrdersInfo::Columns[] = {"o_orderkey",      "o_custkey", "o_orderstatus",  "o_totalprice", "o_orderdate",
                                     "o_orderpriority", "o_clerk",   "o_shippriority", "o_comment"};
const LogicalType OrdersInfo::Types[] = {
    LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR),
    LogicalType::DECIMAL(15, 2),         LogicalType(LogicalTypeId::DATE),    LogicalType(LogicalTypeId::VARCHAR),
    LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR)};

struct LineitemInfo {
	static constexpr char *Name = "lineitem";
	static const char *Columns[];
	static constexpr idx_t ColumnCount = 16;
	static const LogicalType Types[];
};
const char *LineitemInfo::Columns[] = {"l_orderkey",    "l_partkey",       "l_suppkey",  "l_linenumber",
                                       "l_quantity",    "l_extendedprice", "l_discount", "l_tax",
                                       "l_returnflag",  "l_linestatus",    "l_shipdate", "l_commitdate",
                                       "l_receiptdate", "l_shipinstruct",  "l_shipmode", "l_comment"};
const LogicalType LineitemInfo::Types[] = {
    LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER),
    LogicalType(LogicalTypeId::INTEGER), LogicalType::DECIMAL(15, 2),         LogicalType::DECIMAL(15, 2),
    LogicalType::DECIMAL(15, 2),         LogicalType::DECIMAL(15, 2),         LogicalType(LogicalTypeId::VARCHAR),
    LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::DATE),    LogicalType(LogicalTypeId::DATE),
    LogicalType(LogicalTypeId::DATE),    LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
    LogicalType(LogicalTypeId::VARCHAR)};

template <class T>
static void CreateTPCHTable(ClientContext &context, string catalog_name, string schema, string suffix) {
	auto info = make_uniq<CreateTableInfo>();
	info->schema = schema;
	info->table = T::Name + suffix;
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	info->temporary = false;
	for (idx_t i = 0; i < T::ColumnCount; i++) {
		info->columns.AddColumn(ColumnDefinition(T::Columns[i], T::Types[i]));
		info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
	}
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	catalog.CreateTable(context, std::move(info));
}

void DBGenWrapper::CreateTPCHSchema(ClientContext &context, string catalog, string schema, string suffix) {
	CreateTPCHTable<RegionInfo>(context, catalog, schema, suffix);
	CreateTPCHTable<NationInfo>(context, catalog, schema, suffix);
	CreateTPCHTable<SupplierInfo>(context, catalog, schema, suffix);
	CreateTPCHTable<CustomerInfo>(context, catalog, schema, suffix);
	CreateTPCHTable<PartInfo>(context, catalog, schema, suffix);
	CreateTPCHTable<PartsuppInfo>(context, catalog, schema, suffix);
	CreateTPCHTable<OrdersInfo>(context, catalog, schema, suffix);
	CreateTPCHTable<LineitemInfo>(context, catalog, schema, suffix);
}

void skip(int table, int children, DSS_HUGE step, DBGenContext &dbgen_ctx) {
	switch (table) {
	case CUST:
		sd_cust(children, step, &dbgen_ctx);
		break;
	case SUPP:
		sd_supp(children, step, &dbgen_ctx);
		break;
	case NATION:
		sd_nation(children, step, &dbgen_ctx);
		break;
	case REGION:
		sd_region(children, step, &dbgen_ctx);
		break;
	case ORDER_LINE:
		sd_line(children, step, &dbgen_ctx);
		sd_order(children, step, &dbgen_ctx);
		break;
	case PART_PSUPP:
		sd_part(children, step, &dbgen_ctx);
		sd_psupp(children, step, &dbgen_ctx);
		break;
	}
}

void DBGenWrapper::LoadTPCHData(ClientContext &context, double flt_scale, string catalog_name, string schema,
                                string suffix, int children_p, int current_step) {
	if (flt_scale == 0) {
		return;
	}

	// generate the actual data
	DSS_HUGE rowcnt = 0;
	DSS_HUGE extra;
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
	updates = 0;

	DBGenContext dbgen_ctx;

	tdef *tdefs = dbgen_ctx.tdefs;
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

	children = children_p;
	d_path = NULL;

	if (current_step >= children) {
		return;
	}

	if (flt_scale < MIN_SCALE) {
		int i;
		int int_scale;

		dbgen_ctx.scale_factor = 1;
		int_scale = (int)(1000 * flt_scale);
		for (i = PART; i < REGION; i++) {
			tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base) / 1000;
			if (tdefs[i].base < 1) {
				tdefs[i].base = 1;
			}
		}
	} else {
		dbgen_ctx.scale_factor = (long)flt_scale;
	}
	load_dists(10 * 1024 * 1024, &dbgen_ctx); // 10MiB

	/* have to do this after init */
	tdefs[NATION].base = nations.count;
	tdefs[REGION].base = regions.count;

	auto &catalog = Catalog::GetCatalog(context, catalog_name);

	auto append_info = duckdb::unique_ptr<tpch_append_information[]>(new tpch_append_information[REGION + 1]);
	memset(append_info.get(), 0, sizeof(tpch_append_information) * REGION + 1);
	for (size_t i = PART; i <= REGION; i++) {
		auto tname = get_table_name(i);
		if (!tname.empty()) {
			string full_tname = string(tname) + string(suffix);
			auto &tbl_catalog = catalog.GetEntry<TableCatalogEntry>(context, schema, full_tname);
			append_info[i].appender = make_uniq<InternalAppender>(context, tbl_catalog);
		}
	}

	for (i = PART; i <= REGION; i++) {
		if (table & (1 << i)) {
			if (i < NATION) {
				rowcnt = tdefs[i].base * dbgen_ctx.scale_factor;
			} else {
				rowcnt = tdefs[i].base;
			}
			if (children > 1 && current_step != -1) {
				size_t part_size = std::ceil((double)rowcnt / (double)children);
				auto part_offset = part_size * current_step;
				auto part_end = part_offset + part_size;
				rowcnt = part_end > rowcnt ? rowcnt - part_offset : part_size;
				skip(i, children, part_offset, dbgen_ctx);
				if (rowcnt > 0) {
					// generate part of the table
					gen_tbl((int)i, rowcnt, append_info.get(), &dbgen_ctx, part_offset);
				}
			} else {
				// generate full table
				gen_tbl((int)i, rowcnt, append_info.get(), &dbgen_ctx);
			}
		}
	}
	// flush any incomplete chunks
	for (size_t i = PART; i <= REGION; i++) {
		if (append_info[i].appender) {
			append_info[i].appender->Flush();
			append_info[i].appender.reset();
		}
	}

	cleanup_dists();
}

string DBGenWrapper::GetQuery(int query) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-H query number %d", query);
	}
	return TPCH_QUERIES[query - 1];
}

string DBGenWrapper::GetAnswer(double sf, int query) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-H query number %d", query);
	}
	const char *answer;
	if (sf == 0.01) {
		answer = TPCH_ANSWERS_SF0_01[query - 1];
	} else if (sf == 0.1) {
		answer = TPCH_ANSWERS_SF0_1[query - 1];
	} else if (sf == 1) {
		answer = TPCH_ANSWERS_SF1[query - 1];
	} else {
		throw NotImplementedException("Don't have TPC-H answers for SF %llf!", sf);
	}
	return answer;
}

} // namespace tpch
