#include "dbgen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "dbgen_gunk.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "tpch_constants.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/binder.hpp"

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

struct UnsafeAppender {
	UnsafeAppender(ClientContext &context, TableCatalogEntry *tbl) :
		context(context), tbl(tbl), col(0) {
		vector<LogicalType> types;
		for(idx_t i = 0; i < tbl->columns.size(); i++) {
			types.push_back(tbl->columns[i].type);
		}
		chunk.Initialize(types);
	}

	void BeginRow() {
		col = 0;
	}

	void EndRow() {
		assert(col == chunk.column_count());
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			Flush();
		}
	}

	void Flush() {
		if (chunk.size() == 0) {
			return;
		}
		tbl->storage->Append(*tbl, context, chunk);
		chunk.Reset();
	}

	template<class T>
	void AppendValue(T value) {
		assert(col < chunk.column_count());
		FlatVector::GetData<T>(chunk.data[col])[chunk.size()] = value;
		col++;
	}

	void AppendString(const char *value) {
		AppendValue<string_t>(StringVector::AddString(chunk.data[col], value));
	}
private:
	ClientContext &context;
	TableCatalogEntry *tbl;
	DataChunk chunk;
	idx_t col;
};

struct tpch_append_information {
	unique_ptr<UnsafeAppender> appender;
};

void append_value(tpch_append_information &info, int32_t value) {
	info.appender->AppendValue<int32_t>(value);
}

void append_string(tpch_append_information &info, const char *value) {
	info.appender->AppendString(value);
}

void append_decimal(tpch_append_information &info, int64_t value) {
	info.appender->Append<int64_t>(value);
}

void append_date(tpch_append_information &info, string value) {
	info.appender->AppendValue<int32_t>(Date::FromString(value));
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
		append_value(append_info, o->l[i].quantity);
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

// FIXME; find replace: LogicalType(LogicalTypeId::DOUBLE) -> LogicalType(LogicalTypeId::DECIMAL, 15, 2)
struct RegionInfo {
	static constexpr char *Name = "region";
	static constexpr char *Columns[] = { "r_regionkey", "r_name", "r_comment" };
	static constexpr idx_t ColumnCount = 3;
	static const LogicalType Types[];
};
const LogicalType RegionInfo::Types[] = { LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR) };

struct NationInfo {
	static constexpr char *Name = "nation";
	static constexpr char *Columns[] = { "n_nationkey", "n_name", "n_regionkey", "n_comment" };
	static constexpr idx_t ColumnCount = 4;
	static const LogicalType Types[];
};
const LogicalType NationInfo::Types[] = { LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR) };

struct SupplierInfo {
	static constexpr char *Name = "supplier";
	static constexpr char *Columns[] = { "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment" };
	static constexpr idx_t ColumnCount = 7;
	static const LogicalType Types[];
};
const LogicalType SupplierInfo::Types[] = { LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::VARCHAR) };

struct CustomerInfo {
	static constexpr char *Name = "customer";
	static constexpr char *Columns[] = { "c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment" };
	static constexpr idx_t ColumnCount = 8;
	static const LogicalType Types[];
};
const LogicalType CustomerInfo::Types[] = { LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR) };

struct PartInfo {
	static constexpr char *Name = "part";
	static constexpr char *Columns[] = { "p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment" };
	static constexpr idx_t ColumnCount = 9;
	static const LogicalType Types[];
};
const LogicalType PartInfo::Types[] = { LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::VARCHAR) };

struct PartsuppInfo {
	static constexpr char *Name = "partsupp";
	static constexpr char *Columns[] = { "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment" };
	static constexpr idx_t ColumnCount = 5;
	static const LogicalType Types[];
};
const LogicalType PartsuppInfo::Types[] = { LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::VARCHAR) };

struct OrdersInfo {
	static constexpr char *Name = "orders";
	static constexpr char *Columns[] = { "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment" };
	static constexpr idx_t ColumnCount = 9;
	static const LogicalType Types[];
};
const LogicalType OrdersInfo::Types[] = { LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::DATE), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::VARCHAR) };

struct LineitemInfo {
	static constexpr char *Name = "lineitem";
	static constexpr char *Columns[] = { "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment" };
	static constexpr idx_t ColumnCount = 16;
	static const LogicalType Types[];
};
const LogicalType LineitemInfo::Types[] = { LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::DOUBLE), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::DATE), LogicalType(LogicalTypeId::DATE), LogicalType(LogicalTypeId::DATE), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR) };

template<class T>
static void CreateTPCHTable(ClientContext &context, string schema, string suffix) {
	auto info = make_unique<CreateTableInfo>();
	info->schema = schema;
	info->table = T::Name + suffix;
	info->on_conflict = OnCreateConflict::ERROR;
	info->temporary = false;
	for(idx_t i = 0; i < T::ColumnCount; i++) {
		info->columns.push_back(ColumnDefinition(T::Columns[i], T::Types[i]));
		info->constraints.push_back(make_unique<NotNullConstraint>(i));
	}
	Binder binder(context);
	auto bound_info = binder.BindCreateTableInfo(move(info));
	auto &catalog = Catalog::GetCatalog(context);

	catalog.CreateTable(context, bound_info.get());
}

void DBGenWrapper::CreateTPCHSchema(ClientContext &context, string schema, string suffix) {
	CreateTPCHTable<RegionInfo>(context, schema, suffix);
	CreateTPCHTable<NationInfo>(context, schema, suffix);
	CreateTPCHTable<SupplierInfo>(context, schema, suffix);
	CreateTPCHTable<CustomerInfo>(context, schema, suffix);
	CreateTPCHTable<PartInfo>(context, schema, suffix);
	CreateTPCHTable<PartsuppInfo>(context, schema, suffix);
	CreateTPCHTable<OrdersInfo>(context, schema, suffix);
	CreateTPCHTable<LineitemInfo>(context, schema, suffix);
}

void DBGenWrapper::LoadTPCHData(ClientContext &context, double flt_scale, string schema, string suffix) {
	if (flt_scale == 0) {
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
			if (tdefs[i].base < 1) {
				tdefs[i].base = 1;
			}
		}
	} else {
		scale = (long)flt_scale;
	}

	load_dists();

	/* have to do this after init */
	tdefs[NATION].base = nations.count;
	tdefs[REGION].base = regions.count;

	auto &catalog = Catalog::GetCatalog(context);

	auto append_info = unique_ptr<tpch_append_information[]>(new tpch_append_information[REGION + 1]);
	memset(append_info.get(), 0, sizeof(tpch_append_information) * REGION + 1);
	for (size_t i = PART; i <= REGION; i++) {
		auto tname = get_table_name(i);
		if (!tname.empty()) {
			string full_tname = string(tname) + string(suffix);
			auto tbl_catalog = catalog.GetEntry<TableCatalogEntry>(context, schema, full_tname);
			append_info[i].appender = make_unique<UnsafeAppender>(context, tbl_catalog);
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
