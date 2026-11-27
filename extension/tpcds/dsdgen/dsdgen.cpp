#include "dsdgen.hpp"

#include "append_info-c.hpp"
#include "dsdgen_helpers.hpp"
#include "dsdgen_schema.hpp"
#include "tpcds_constants.hpp"
#include "tpcds_errors.hpp"
#include "dsdgen-c/constants.h"
#include "dsdgen-c/porting.h"
#include "dsdgen-c/parallel.h"
#include "dsdgen-c/tables.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <map>
#include <mutex>
#include <unordered_map>

using namespace duckdb::cxx;

namespace tpcds {

static constexpr idx_t DEFAULT_ROW_GROUP_SIZE = 122880;
static constexpr idx_t DSDGEN_MIN_ROW_BATCH_SIZE = 10000;
static constexpr idx_t DSDGEN_TARGET_CHUNK_ROWS = 2 * DEFAULT_ROW_GROUP_SIZE;
static constexpr idx_t DSDGEN_PROGRESS_UNIT_SCALE = 100;
static constexpr idx_t DSDGEN_MAX_SCALE_FACTOR = 100000;
//! Tables with more output rows are split into work items: each is a step after which the thread is handed back
static constexpr idx_t DSDGEN_PARALLEL_SPLIT_MIN_ROWS = DSDGEN_TARGET_CHUNK_ROWS;
static constexpr idx_t DSDGEN_MIN_PARALLEL_TASK_ROWS = DSDGEN_MIN_ROW_BATCH_SIZE;
// TPC-DS worker initialization creates large per-thread caches, so fanout is bounded.
static constexpr idx_t DSDGEN_WORK_ITEMS_PER_THREAD = 128;
static constexpr idx_t DSDGEN_MAX_WORK_ITEMS_PER_TABLE = 4096;
static constexpr idx_t DSDGEN_PROGRESS_REPORT_ROW_INTERVAL = 1000;
//! A work item whose rows can be inserted right away inserts them in batches of this many rows
static constexpr idx_t DSDGEN_INSERT_BATCH_ROWS = DSDGEN_TARGET_CHUNK_ROWS;
static constexpr const char *DSDGEN_BUFFER_NAME = "__tpcds_dsdgen_buffer";

static constexpr int DSDGEN_TABLES[] = {CALL_CENTER,
                                        CATALOG_PAGE,
                                        CATALOG_SALES,
                                        CUSTOMER,
                                        CUSTOMER_ADDRESS,
                                        CUSTOMER_DEMOGRAPHICS,
                                        DATET,
                                        HOUSEHOLD_DEMOGRAPHICS,
                                        INCOME_BAND,
                                        INVENTORY,
                                        ITEM,
                                        PROMOTION,
                                        REASON,
                                        SHIP_MODE,
                                        STORE,
                                        STORE_SALES,
                                        TIME,
                                        WAREHOUSE,
                                        WEB_PAGE,
                                        WEB_SALES,
                                        WEB_SITE};

//===--------------------------------------------------------------------===//
// Table schemas
//===--------------------------------------------------------------------===//
struct DSDGenTableSchema {
	const char *name;
	const char *const *columns;
	const char *const *types;
	idx_t column_count;
	const char *const *primary_key_columns;
	idx_t primary_key_count;
};

template <class T>
static DSDGenTableSchema TableSchema() {
	return DSDGenTableSchema {T::Name, T::Columns, T::Types, T::ColumnCount, T::PrimaryKeyColumns, T::PrimaryKeyCount};
}

static DSDGenTableSchema GetDSDGenTableSchema(int table_id) {
	switch (table_id) {
	case CALL_CENTER:
		return TableSchema<CallCenterInfo>();
	case CATALOG_PAGE:
		return TableSchema<CatalogPageInfo>();
	case CATALOG_RETURNS:
		return TableSchema<CatalogReturnsInfo>();
	case CATALOG_SALES:
		return TableSchema<CatalogSalesInfo>();
	case CUSTOMER:
		return TableSchema<CustomerInfo>();
	case CUSTOMER_ADDRESS:
		return TableSchema<CustomerAddressInfo>();
	case CUSTOMER_DEMOGRAPHICS:
		return TableSchema<CustomerDemographicsInfo>();
	case DATET:
		return TableSchema<DateDimInfo>();
	case HOUSEHOLD_DEMOGRAPHICS:
		return TableSchema<HouseholdDemographicsInfo>();
	case INCOME_BAND:
		return TableSchema<IncomeBandInfo>();
	case INVENTORY:
		return TableSchema<InventoryInfo>();
	case ITEM:
		return TableSchema<ItemInfo>();
	case PROMOTION:
		return TableSchema<PromotionInfo>();
	case REASON:
		return TableSchema<ReasonInfo>();
	case SHIP_MODE:
		return TableSchema<ShipModeInfo>();
	case STORE:
		return TableSchema<StoreInfo>();
	case STORE_RETURNS:
		return TableSchema<StoreReturnsInfo>();
	case STORE_SALES:
		return TableSchema<StoreSalesInfo>();
	case TIME:
		return TableSchema<TimeDimInfo>();
	case WAREHOUSE:
		return TableSchema<WarehouseInfo>();
	case WEB_PAGE:
		return TableSchema<WebPageInfo>();
	case WEB_RETURNS:
		return TableSchema<WebReturnsInfo>();
	case WEB_SALES:
		return TableSchema<WebSalesInfo>();
	case WEB_SITE:
		return TableSchema<WebSiteInfo>();
	default:
		ThrowInternalError("Unexpected TPC-DS table id");
	}
}

//===--------------------------------------------------------------------===//
// Work planning
//===--------------------------------------------------------------------===//
struct DSDGenTableRange {
	idx_t offset;
	idx_t count;
};

struct DSDGenWorkItem {
	int table_id;
	int children;
	int step;
	idx_t offset;
	idx_t count;
	idx_t progress_units_per_row;
	idx_t progress_units;
};

static int GetDSDGenChildTable(int table_id) {
	switch (table_id) {
	case CATALOG_SALES:
		return CATALOG_RETURNS;
	case STORE_SALES:
		return STORE_RETURNS;
	case WEB_SALES:
		return WEB_RETURNS;
	default:
		return -1;
	}
}

static DSDGenTableRange GetDSDGenTableRange(idx_t row_count, int children, int current_step) {
	if (children <= 1) {
		return DSDGenTableRange {0, row_count};
	}
	auto child_count = static_cast<idx_t>(children);
	auto step = static_cast<idx_t>(current_step);
	auto base_count = row_count / child_count;
	auto extra_rows = row_count % child_count;
	auto offset = (base_count * step) + std::min<idx_t>(step, extra_rows);
	auto count = base_count + (step < extra_rows ? 1 : 0);
	return DSDGenTableRange {offset, count};
}

static idx_t GetDSDGenTableOutputMultiplier(int table_id) {
	switch (table_id) {
	case CATALOG_SALES:
		return 10;
	case STORE_SALES:
	case WEB_SALES:
		return 12;
	default:
		return 1;
	}
}

static idx_t GetDSDGenTableOrder(int table_id) {
	for (idx_t i = 0; i < sizeof(DSDGEN_TABLES) / sizeof(DSDGEN_TABLES[0]); i++) {
		if (DSDGEN_TABLES[i] == table_id) {
			return i;
		}
	}
	ThrowInternalError("Unexpected TPC-DS table id");
}

static idx_t GetDSDGenReturnPercent(int table_id) {
	switch (table_id) {
	case CATALOG_SALES:
		return CR_RETURN_PCT;
	case STORE_SALES:
		return SR_RETURN_PCT;
	case WEB_SALES:
		return WR_RETURN_PCT;
	default:
		return 0;
	}
}

static idx_t GetDSDGenTableProgressUnitsPerRow(int table_id) {
	auto output_rows = GetDSDGenTableOutputMultiplier(table_id);
	auto progress_units = output_rows * GetDSDGenTableSchema(table_id).column_count * DSDGEN_PROGRESS_UNIT_SCALE;
	auto child_table = GetDSDGenChildTable(table_id);
	if (child_table != -1) {
		progress_units += output_rows * GetDSDGenReturnPercent(table_id) *
		                  GetDSDGenTableSchema(child_table).column_count * DSDGEN_PROGRESS_UNIT_SCALE / 100;
	}
	return progress_units;
}

//! Large tables are split into work items also for a single thread: a call generates one work item, after which the
//! thread is handed back
static idx_t GetDSDGenTableChildCount(int table_id, idx_t row_count, idx_t thread_count) {
	if (row_count == 0) {
		return 1;
	}
	auto output_row_count = row_count * GetDSDGenTableOutputMultiplier(table_id);
	auto row_count_child_count = output_row_count < DSDGEN_PARALLEL_SPLIT_MIN_ROWS
	                                 ? 1
	                                 : (output_row_count + DSDGEN_TARGET_CHUNK_ROWS - 1) / DSDGEN_TARGET_CHUNK_ROWS;
	auto max_child_count =
	    std::min<idx_t>(thread_count * DSDGEN_WORK_ITEMS_PER_THREAD, DSDGEN_MAX_WORK_ITEMS_PER_TABLE);
	max_child_count = std::min<idx_t>(max_child_count, std::max<idx_t>(1, row_count / DSDGEN_MIN_PARALLEL_TASK_ROWS));
	return std::min<idx_t>(row_count_child_count, max_child_count);
}

static bool DSDGenPhysicalTableMatches(int table_id, int physical_table_id) {
	return physical_table_id == table_id || physical_table_id == GetDSDGenChildTable(table_id);
}

//===--------------------------------------------------------------------===//
// SQL helpers
//===--------------------------------------------------------------------===//
static std::string QuoteLiteral(const std::string &value) {
	std::string result = "'";
	for (auto c : value) {
		if (c == '\'') {
			result += '\'';
		}
		result += c;
	}
	return result + "'";
}

static SqlStatement ParseStatement(Connection &con, const std::string &sql) {
	auto statements = con.ParseSQL(sql);
	auto statement = statements.Next();
	if (!statement) {
		ThrowInternalError("dsdgen: no statement in \"" + sql + "\"");
	}
	return statement;
}

//! Runs a query returning a single VARCHAR value
static std::string QueryString(Connection &con, const std::string &sql) {
	auto result = con.Execute(sql);
	auto chunk = result.FetchChunk();
	if (!chunk || chunk.GetRowCount() != 1) {
		ThrowInternalError("dsdgen: expected a single row from \"" + sql + "\"");
	}
	return chunk.GetVector(0).GetValue(0).ToText();
}

//! Splits a schema argument into its dotted components, as DuckDB parses a qualified name
static bool TryParseSchemaPath(const std::string &input, std::vector<std::string> &result) {
	result.clear();
	std::string entry;
	for (idx_t i = 0; i < input.size(); i++) {
		auto c = input[i];
		if (c == '"') {
			bool terminated = false;
			for (i++; i < input.size(); i++) {
				if (input[i] == '"') {
					if (i + 1 < input.size() && input[i + 1] == '"') {
						entry += '"';
						i++;
						continue;
					}
					terminated = true;
					break;
				}
				entry += input[i];
			}
			if (!terminated) {
				return false;
			}
		} else if (c == '.') {
			result.push_back(std::move(entry));
			entry.clear();
		} else {
			entry += c;
		}
	}
	result.push_back(std::move(entry));
	return true;
}

//! The schemas of a catalog by their path
static std::vector<std::vector<std::string>> GetSchemaPaths(Connection &con, const std::string &catalog) {
	auto result = con.Execute("SELECT oid::BIGINT, schema_name, parent_schema_oid::BIGINT FROM duckdb_schemas() "
	                          "WHERE database_name = " +
	                          QuoteLiteral(catalog));
	struct SchemaEntry {
		std::string name;
		int64_t parent;
		bool has_parent;
	};
	std::unordered_map<int64_t, SchemaEntry> schemas;
	while (auto chunk = result.FetchChunk()) {
		if (chunk.GetRowCount() == 0) {
			break;
		}
		auto oids = chunk.GetVector(0);
		auto names = chunk.GetVector(1);
		auto parents = chunk.GetVector(2);
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			auto parent = parents.GetValue(i);
			SchemaEntry entry {names.GetValue(i).ToText(), 0, !parent.IsNull()};
			if (entry.has_parent) {
				entry.parent = parent.Get<int64_t>();
			}
			schemas.emplace(oids.GetValue(i).Get<int64_t>(), std::move(entry));
		}
	}
	std::vector<std::vector<std::string>> paths;
	for (auto &schema : schemas) {
		std::vector<std::string> path;
		auto current = &schema.second;
		while (current) {
			path.insert(path.begin(), current->name);
			auto parent = current->has_parent ? schemas.find(current->parent) : schemas.end();
			current = parent == schemas.end() ? nullptr : &parent->second;
		}
		paths.push_back(std::move(path));
	}
	return paths;
}

//! Resolves a schema argument as Catalog::ResolveSchema does: an existing nested schema path over a dotted name
static std::vector<std::string> ResolveSchemaPath(Connection &con, const std::string &catalog,
                                                  const std::string &schema_arg) {
	std::vector<std::string> literal {schema_arg};
	std::vector<std::string> nested;
	if (!TryParseSchemaPath(schema_arg, nested)) {
		return literal;
	}
	bool has_empty_component =
	    std::any_of(nested.begin(), nested.end(), [](const std::string &part) { return part.empty(); });
	if (!has_empty_component && nested == literal) {
		return literal;
	}
	auto paths = GetSchemaPaths(con, catalog);
	auto exists = [&](const std::vector<std::string> &path) {
		for (auto &entry : paths) {
			if (entry.size() != path.size()) {
				continue;
			}
			bool equal = true;
			for (idx_t i = 0; i < path.size() && equal; i++) {
				equal = entry[i].size() == path[i].size() &&
				        std::equal(entry[i].begin(), entry[i].end(), path[i].begin(), [](char l, char r) {
					        return std::tolower(static_cast<unsigned char>(l)) ==
					               std::tolower(static_cast<unsigned char>(r));
				        });
			}
			if (equal) {
				return true;
			}
		}
		return false;
	};
	if (has_empty_component) {
		if (exists(literal)) {
			return literal;
		}
		ThrowInvalidInputError("Failed to parse schema path \"" + schema_arg +
		                       "\" - schema paths cannot contain empty components. Schema names that contain a '.' "
		                       "or a '\"' must be double-quoted (e.g. '\"my.schema\"')");
	}
	if (!exists(nested) && exists(literal)) {
		return literal;
	}
	return nested;
}

//===--------------------------------------------------------------------===//
// Generator
//===--------------------------------------------------------------------===//
DSDGenGenerator::~DSDGenGenerator() {
}

//! The rows a work item generated, for its table and the child table
struct DSDGenItemBuffers {
	DSDGenWorkItem work_item;
	std::vector<std::unique_ptr<tpcds_append_information>> append_info;
};

//! Inserts the work items of one table, and the rows they generate for its child table, in the order of their steps.
//! Each table inserts through its own connection, so the tables are filled in parallel.
struct DSDGenTableInserter {
	//! Closed once every work item of the table is inserted
	std::unique_ptr<Connection> connection;
	//! Held while inserting: the connection runs one statement at a time, and the rows go in step order
	std::mutex insert_lock;
	//! Protects the finished work items and the next step to insert
	std::mutex queue_lock;
	std::vector<std::unique_ptr<DSDGenItemBuffers>> finished_items;
	int next_step = 0;
	//! The work items still to insert, written while holding the insert lock
	idx_t remaining_items = 0;
};

class TPCDSDSDGenGenerator : public DSDGenGenerator {
public:
	TPCDSDSDGenGenerator(Context &context, const DSDGenOptions &options)
	    : scale(options.sf), connection(std::make_unique<Connection>(context.ConnectInTransaction())) {
		ResolveTables(options);
		CreateTables(options);
		auto &factory = connection->GetFactory();
		for (int table_id = CALL_CENTER; table_id < DBGEN_VERSION; table_id++) {
			auto schema = GetDSDGenTableSchema(table_id);
			layouts.push_back(std::make_unique<TPCDSTableLayout>(factory, schema.types, schema.column_count));
		}

		thread_count = 1;
#ifndef DUCKDB_NO_THREADS
		auto threads = context.GetConfig().GetOption("threads").value;
		thread_count =
		    static_cast<idx_t>(std::max<int64_t>(1, threads.Cast(factory, factory.ParseType("BIGINT")).Get<int64_t>()));
#endif
		if (scale > 0) {
			InitializeDSDgen(scale);
			PlanWork();
		}
		connection.reset();
		inserters.resize(DBGEN_VERSION);
		for (auto &work_item : work_items) {
			auto &inserter = inserters[work_item.table_id];
			if (!inserter) {
				inserter = std::make_unique<DSDGenTableInserter>();
				inserter->connection = std::make_unique<Connection>(context.ConnectInTransaction());
			}
			inserter->remaining_items++;
		}
		remaining_items = work_items.size();
		if (work_items.empty()) {
			finished.store(true);
		}
	}

	idx_t MaxThreads() const override {
		return std::max<idx_t>(1, std::min<idx_t>(thread_count, work_items.size()));
	}

	bool Generate(Context &context) override {
		try {
			return GenerateStep(context);
		} catch (...) {
			failed.store(true);
			throw;
		}
	}

	double Progress() const override {
		// the query completes after the scan, with the commit; DuckDB reports 100% then
		static constexpr double MAX_PROGRESS = 0.999;
		if (finished.load()) {
			return MAX_PROGRESS;
		}
		if (total_work == 0) {
			return 0.0;
		}
		auto current_generated_work = std::min<idx_t>(generated_work.load(), total_work);
		auto current_inserted_work = std::min<idx_t>(inserted_work.load(), total_work);
		auto generated_progress = static_cast<double>(current_generated_work) / static_cast<double>(total_work);
		auto inserted_progress = static_cast<double>(current_inserted_work) / static_cast<double>(total_work);
		static constexpr double INSERT_WEIGHT = 0.05;
		auto progress = generated_progress * (1.0 - INSERT_WEIGHT) + inserted_progress * INSERT_WEIGHT;
		return std::min<double>(progress, MAX_PROGRESS);
	}

private:
	void ResolveTables(const DSDGenOptions &options) {
		auto catalog = options.catalog ? *options.catalog : QueryString(*connection, "SELECT current_database()");
		auto schema = options.schema ? *options.schema : QueryString(*connection, "SELECT current_schema()");
		auto schema_path = ResolveSchemaPath(*connection, catalog, schema);
		std::string prefix = RenderQuotedIdentifier(catalog) + ".";
		for (auto &part : schema_path) {
			prefix += RenderQuotedIdentifier(part) + ".";
		}
		for (int table_id = CALL_CENTER; table_id < DBGEN_VERSION; table_id++) {
			auto table_schema = GetDSDGenTableSchema(table_id);
			table_names.push_back(prefix + RenderQuotedIdentifier(std::string(table_schema.name) + options.suffix));
		}
	}

	void CreateTables(const DSDGenOptions &options) {
		for (int table_id = CALL_CENTER; table_id < DBGEN_VERSION; table_id++) {
			auto schema = GetDSDGenTableSchema(table_id);
			std::string sql = options.overwrite ? "CREATE OR REPLACE TABLE " : "CREATE TABLE ";
			sql += table_names[table_id] + " (";
			for (idx_t col = 0; col < schema.column_count; col++) {
				sql += (col > 0 ? ", " : "") + RenderQuotedIdentifier(schema.columns[col]) + " " + schema.types[col];
			}
			if (options.keys) {
				sql += ", PRIMARY KEY (";
				for (idx_t col = 0; col < schema.primary_key_count; col++) {
					sql += (col > 0 ? ", " : "") + RenderQuotedIdentifier(schema.primary_key_columns[col]);
				}
				sql += ")";
			}
			sql += ")";
			connection->Execute(sql).Drain();
		}
	}

	void PlanWork() {
		total_work = 0;
		for (auto table_id : DSDGEN_TABLES) {
			auto row_count = static_cast<idx_t>(GetRowCount(table_id));
			auto progress_units_per_row = GetDSDGenTableProgressUnitsPerRow(table_id);
			auto children = static_cast<int>(GetDSDGenTableChildCount(table_id, row_count, thread_count));
			for (auto step = 0; step < children; step++) {
				auto range = GetDSDGenTableRange(row_count, children, step);
				if (range.count == 0) {
					continue;
				}
				auto progress_units = range.count * progress_units_per_row;
				work_items.push_back(DSDGenWorkItem {table_id, children, step, range.offset, range.count,
				                                     progress_units_per_row, progress_units});
				total_work += progress_units;
			}
		}
		std::sort(work_items.begin(), work_items.end(), [](const DSDGenWorkItem &left, const DSDGenWorkItem &right) {
			// Run larger work items first; the rows are inserted in table order separately.
			if (left.progress_units != right.progress_units) {
				return left.progress_units > right.progress_units;
			}
			auto left_table_order = GetDSDGenTableOrder(left.table_id);
			auto right_table_order = GetDSDGenTableOrder(right.table_id);
			if (left_table_order != right_table_order) {
				return left_table_order < right_table_order;
			}
			return left.step < right.step;
		});
	}

	//! Takes the next step: inserts a finished work item, or generates the next one. The generator keeps its state
	//! per thread, so a work item runs on a single thread from start to end.
	bool GenerateStep(Context &context) {
		if (failed.load()) {
			return false;
		}
		if (TryInsertFinishedItem()) {
			return true;
		}
		auto item_idx = next_work_item.fetch_add(1);
		if (item_idx >= work_items.size()) {
			// the items still to insert are left to the threads generating or inserting them
			return false;
		}
		auto &work_item = work_items[item_idx];
		auto buffers = std::make_unique<DSDGenItemBuffers>();
		buffers->work_item = work_item;
		InitializeDSDgen(scale, work_item.children, work_item.step + 1);
		buffers->append_info.resize(DBGEN_VERSION);
		for (int table_id = CALL_CENTER; table_id < DBGEN_VERSION; table_id++) {
			if (!DSDGenPhysicalTableMatches(work_item.table_id, table_id)) {
				continue;
			}
			auto append = std::make_unique<tpcds_append_information>(context.GetFactory(), *layouts[table_id]);
			// the null bitmaps of the table definitions are per thread as well
			append->table_def = GetTDefByNumber(table_id);
			buffers->append_info[table_id] = std::move(append);
		}
		SkipToTableOffset(work_item.table_id, work_item.offset);

		auto builder_func = GetTDefFunctionByNumber(work_item.table_id);
		auto &table_append = *buffers->append_info[work_item.table_id];
		idx_t reported_rows = 0;
		for (idx_t row_idx = 0; row_idx < work_item.count; row_idx++) {
			auto row_number = static_cast<ds_key_t>(work_item.offset + row_idx + 1);
			if (builder_func((void *)&buffers->append_info, row_number)) {
				ThrowInternalError("Table generation failed");
			}
			row_stop(work_item.table_id);
			if ((row_idx + 1) % DSDGEN_PROGRESS_REPORT_ROW_INTERVAL != 0) {
				continue;
			}
			if (failed.load()) {
				return false;
			}
			generated_work.fetch_add((row_idx + 1 - reported_rows) * work_item.progress_units_per_row);
			reported_rows = row_idx + 1;
			if (table_append.RowCount() >= DSDGEN_INSERT_BATCH_ROWS) {
				TryInsertPartial(*buffers);
			}
		}
		generated_work.fetch_add((work_item.count - reported_rows) * work_item.progress_units_per_row);
		auto &inserter = *inserters[work_item.table_id];
		{
			std::lock_guard<std::mutex> guard(inserter.queue_lock);
			inserter.finished_items.push_back(std::move(buffers));
		}
		return true;
	}

	void SkipToTableOffset(int table_id, idx_t offset) {
		if (offset == 0) {
			return;
		}
		auto skip_count = static_cast<ds_key_t>(offset);
		SkipTableRows(table_id, skip_count);
		auto child_table = GetDSDGenChildTable(table_id);
		if (child_table != -1) {
			SkipTableRows(child_table, skip_count);
		}
	}

	//! Inserts the rows of every buffer of the work item through the connection of its table
	void InsertBuffers(DSDGenTableInserter &inserter, DSDGenItemBuffers &buffers) {
		auto &connection = *inserter.connection;
		for (int table_id = CALL_CENTER; table_id < DBGEN_VERSION; table_id++) {
			auto &append = buffers.append_info[table_id];
			if (!append) {
				continue;
			}
			append->FlushChunk();
			if (append->GetBuffer().GetRowCount() == 0) {
				continue;
			}
			auto statement = ParseStatement(connection, "INSERT INTO " + table_names[table_id] + " SELECT * FROM " +
			                                                DSDGEN_BUFFER_NAME);
			statement.AddCollection(DSDGEN_BUFFER_NAME, append->GetBuffer());
			connection.Execute(statement).Drain();
			append->ResetBuffer();
		}
	}

	bool IsNextStep(DSDGenTableInserter &inserter, const DSDGenWorkItem &work_item) {
		std::lock_guard<std::mutex> guard(inserter.queue_lock);
		return inserter.next_step == work_item.step;
	}

	//! Inserts the rows generated so far when the work item is the next one of its table: rows are inserted in order
	void TryInsertPartial(DSDGenItemBuffers &buffers) {
		auto &inserter = *inserters[buffers.work_item.table_id];
		std::unique_lock<std::mutex> guard(inserter.insert_lock, std::try_to_lock);
		if (!guard.owns_lock() || !IsNextStep(inserter, buffers.work_item)) {
			return;
		}
		InsertBuffers(inserter, buffers);
	}

	//! Takes the finished work item that is next in the table, if any
	std::unique_ptr<DSDGenItemBuffers> TakeInsertableItem(DSDGenTableInserter &inserter) {
		std::lock_guard<std::mutex> guard(inserter.queue_lock);
		auto &items = inserter.finished_items;
		for (auto it = items.begin(); it != items.end(); it++) {
			if ((*it)->work_item.step == inserter.next_step) {
				auto result = std::move(*it);
				items.erase(it);
				return result;
			}
		}
		return nullptr;
	}

	bool HasInsertableItem(DSDGenTableInserter &inserter) {
		std::lock_guard<std::mutex> guard(inserter.queue_lock);
		for (auto &buffers : inserter.finished_items) {
			if (buffers->work_item.step == inserter.next_step) {
				return true;
			}
		}
		return false;
	}

	//! Inserts one finished work item that is next in the table, returning whether it did. When another thread is
	//! inserting into the table, it leaves the item to that one, which keeps inserting as long as there are items.
	bool TryInsertFinishedItem(DSDGenTableInserter &inserter) {
		while (!failed.load()) {
			{
				std::unique_lock<std::mutex> guard(inserter.insert_lock, std::try_to_lock);
				if (!guard.owns_lock()) {
					return false;
				}
				auto buffers = TakeInsertableItem(inserter);
				if (buffers) {
					InsertBuffers(inserter, *buffers);
					inserted_work.fetch_add(buffers->work_item.progress_units);
					{
						std::lock_guard<std::mutex> queue_guard(inserter.queue_lock);
						inserter.next_step++;
					}
					if (--inserter.remaining_items == 0) {
						// the transaction cannot commit while the connection is open
						inserter.connection.reset();
					}
					if (--remaining_items == 0) {
						finished.store(true);
					}
					return true;
				}
			}
			// an item may have finished after the check, while its thread could not take the lock
			if (!HasInsertableItem(inserter)) {
				return false;
			}
		}
		return false;
	}

	//! Inserts one finished work item of any table, starting at a different table for each call to spread the threads
	bool TryInsertFinishedItem() {
		auto start = next_insert_table.fetch_add(1);
		for (idx_t i = 0; i < inserters.size(); i++) {
			auto &inserter = inserters[(start + i) % inserters.size()];
			if (inserter && TryInsertFinishedItem(*inserter)) {
				return true;
			}
		}
		return false;
	}

private:
	double scale;
	idx_t thread_count = 1;
	std::vector<std::string> table_names;
	std::vector<std::unique_ptr<TPCDSTableLayout>> layouts;
	std::vector<DSDGenWorkItem> work_items;
	std::atomic<idx_t> next_work_item {0};
	idx_t total_work = 0;
	std::atomic<idx_t> generated_work {0};
	std::atomic<idx_t> inserted_work {0};
	std::atomic<bool> failed {false};
	std::atomic<bool> finished {false};
	std::atomic<idx_t> remaining_items {0};
	std::atomic<idx_t> next_insert_table {0};
	//! Creates the tables, then closes
	std::unique_ptr<Connection> connection;
	//! Per table a work item was planned for; declared last, so the connections close before the buffers are destroyed
	std::vector<std::unique_ptr<DSDGenTableInserter>> inserters;
};

std::unique_ptr<DSDGenGenerator> DSDGenGenerator::Create(Context &context, const DSDGenOptions &options) {
	DSDGenWrapper::ValidateScaleFactor(options.sf);
	return std::make_unique<TPCDSDSDGenGenerator>(context, options);
}

//===--------------------------------------------------------------------===//
// Queries and answers
//===--------------------------------------------------------------------===//
void DSDGenWrapper::ValidateScaleFactor(double scale) {
	if (std::isnan(scale)) {
		ThrowInvalidInputError("DSDGen requires a valid scale factor");
	}
	if (scale > DSDGEN_MAX_SCALE_FACTOR) {
		ThrowInvalidInputError("DSDGen does not support a scale factor exceeding " +
		                       std::to_string(DSDGEN_MAX_SCALE_FACTOR) + ".");
	}
#ifdef DEBUG
	// With a scale of 778+ the following multiplication (signed int target value) during generation is:
	// r->cc_sq_ft *= r->cc_employees overflows;
	// 3,464,105 * 649 = 2,248,204,145 (max. signed int value: 2,147,483,647)
	if (scale > 777) {
		ThrowInvalidInputError("DSDGen results in a signed integer overflow with a scale exceeding 777.");
	}
#endif
}

uint32_t DSDGenWrapper::QueriesCount() {
	return TPCDS_QUERIES_COUNT;
}

std::string DSDGenWrapper::GetQuery(int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		ThrowInvalidInputError("Out of range TPC-DS query number " + std::to_string(query));
	}
	return TPCDS_QUERIES[query - 1];
}

//! Q9 compares store_sales counts against thresholds that qgen draws from a range proportional to the store_sales
//! row count, i.e. to the scale factor; scale the stored SF 1 thresholds so the CASE branches keep their selectivity
static std::string ScaleQ9Thresholds(const std::string &query, double sf) {
	std::string result;
	idx_t pos = 0;
	while (pos < query.size()) {
		auto start = query.find(") > ", pos);
		if (start == std::string::npos) {
			break;
		}
		start += 4;
		auto end = start;
		int64_t threshold = 0;
		while (end < query.size() && std::isdigit(static_cast<unsigned char>(query[end]))) {
			threshold = threshold * 10 + (query[end] - '0');
			end++;
		}
		result += query.substr(pos, start - pos);
		pos = end;
		if (end == start || query.compare(end, 5, " THEN") != 0) {
			result += query.substr(start, end - start);
			continue;
		}
		auto scaled = std::max<int64_t>(1, static_cast<int64_t>(std::llround(static_cast<double>(threshold) * sf)));
		result += std::to_string(scaled);
	}
	result += query.substr(pos);
	return result;
}

std::string DSDGenWrapper::GetQuery(int query, double sf) {
	if (std::isnan(sf) || sf <= 0 || sf > DSDGEN_MAX_SCALE_FACTOR) {
		ThrowInvalidInputError("TPC-DS queries require a scale factor between 0 and " +
		                       std::to_string(DSDGEN_MAX_SCALE_FACTOR));
	}
	auto result = GetQuery(query);
	if (query != 9) {
		return result;
	}
	return ScaleQ9Thresholds(result, sf);
}

std::string DSDGenWrapper::GetAnswer(double sf, int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		ThrowInvalidInputError("Out of range TPC-DS query number " + std::to_string(query));
	}
	if (sf == 0.01) {
		return TPCDS_ANSWERS_SF0_01[query - 1];
	} else if (sf == 1) {
		return TPCDS_ANSWERS_SF1[query - 1];
	}
	ThrowInvalidInputError("Don't have TPC-DS answers for SF " + std::to_string(sf) + "!");
}

} // namespace tpcds
