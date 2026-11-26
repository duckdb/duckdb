// The TPC-H extension, written against the stable C++ API (duckdb_cpp.hpp) only.
//
// dbgen runs SQL in the transaction of the statement calling it, through a connection that inherits it: it creates
// the tables, and inserts the generated rows by work item, in a deterministic order.

#ifndef DUCKDB_EXTENSION_NAME
#define DUCKDB_EXTENSION_NAME tpch
#endif

#include "duckdb_cpp_extension.hpp"

#include "dbgen/dbgen.hpp"

#include <algorithm>
#include <atomic>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

using namespace duckdb::cxx;

namespace {

constexpr int TPCH_QUERY_COUNT = 22;

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

std::string SQLString(std::string_view text) {
	std::string result = "'";
	for (auto c : text) {
		if (c == '\'') {
			result += '\'';
		}
		result += c;
	}
	return result + "'";
}

std::string Lower(std::string text) {
	std::transform(text.begin(), text.end(), text.begin(),
	               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
	return text;
}

//! The first value of the first row of a query, as text
std::string QueryText(Connection &conn, const std::string &sql) {
	auto result = conn.Execute(sql);
	std::string value;
	bool found = false;
	while (auto chunk = result.FetchChunk()) {
		if (!found && chunk.GetRowCount() > 0) {
			value = chunk.GetVector(0).GetValue(0).ToText();
			found = true;
		}
	}
	if (!found) {
		throw InvalidInputException("query returned no rows: " + sql);
	}
	return value;
}

//! Splits a schema path into its components as the engine does: dots separate components, a double-quoted component
//! may contain dots and doubled quotes. Returns false when the text does not parse.
bool TryParseSchemaPath(const std::string &input, std::vector<std::string> &result) {
	result.clear();
	idx_t idx = 0;
	while (idx < input.size()) {
		std::string entry;
		if (input[idx] == '"') {
			bool terminated = false;
			for (idx++; idx < input.size(); idx++) {
				if (input[idx] != '"') {
					entry += input[idx];
				} else if (idx + 1 < input.size() && input[idx + 1] == '"') {
					entry += '"';
					idx++;
				} else {
					idx++;
					terminated = true;
					break;
				}
			}
			if (!terminated || entry.empty() || (idx < input.size() && input[idx] != '.')) {
				return false;
			}
		} else {
			for (; idx < input.size() && input[idx] != '.'; idx++) {
				if (input[idx] == '"') {
					return false;
				}
				entry += input[idx];
			}
		}
		result.push_back(std::move(entry));
		if (idx < input.size()) {
			idx++;
		}
	}
	if (input.empty() || input.back() == '.') {
		result.emplace_back();
	}
	return true;
}

std::string RenderPath(const std::string &catalog, const std::vector<std::string> &schema_path) {
	auto result = RenderQuotedIdentifier(catalog);
	for (auto &part : schema_path) {
		result += "." + RenderQuotedIdentifier(part);
	}
	return result;
}

//! Whether the catalog has a schema at the (possibly nested) path. Fails for no reason other than a missing catalog.
bool SchemaExists(Connection &conn, const std::string &catalog, const std::vector<std::string> &schema_path) {
	std::string path_list;
	for (auto &part : schema_path) {
		path_list += (path_list.empty() ? "" : ", ") + SQLString(Lower(part));
	}
	auto sql = "WITH RECURSIVE paths(oid, path) AS (SELECT oid, [lower(schema_name)] FROM duckdb_schemas() WHERE "
	           "lower(database_name) = " +
	           SQLString(Lower(catalog)) +
	           " AND parent_schema_oid IS NULL UNION ALL SELECT s.oid, list_append(p.path, lower(s.schema_name)) "
	           "FROM duckdb_schemas() s JOIN paths p ON s.parent_schema_oid = p.oid) "
	           "SELECT count(*) FROM paths WHERE path = [" +
	           path_list + "]";
	return QueryText(conn, sql) != "0";
}

//! Resolves a schema argument the way the engine's functions do: an unquoted dotted name prefers a nested schema
//! path and falls back to a schema named literally like the argument. Returns the schema as a qualified SQL name.
std::string ResolveSchema(Connection &conn, const std::string &catalog, const std::string &schema_arg) {
	std::vector<std::vector<std::string>> candidates;
	std::string parse_error;
	std::vector<std::string> nested_path;
	if (!TryParseSchemaPath(schema_arg, nested_path)) {
		candidates.push_back({schema_arg});
	} else if (std::any_of(nested_path.begin(), nested_path.end(), [](const std::string &p) { return p.empty(); })) {
		parse_error = "Failed to parse schema path \"" + schema_arg +
		              "\" - schema paths cannot contain empty components. Schema names that contain a '.' or a '\"' "
		              "must be double-quoted (e.g. '\"my.schema\"')";
		candidates.push_back({schema_arg});
	} else {
		bool is_literal = nested_path.size() == 1 && nested_path[0] == schema_arg;
		candidates.push_back(nested_path);
		if (!is_literal) {
			candidates.push_back({schema_arg});
		}
	}
	if (candidates.size() > 1 || !parse_error.empty()) {
		for (auto &candidate : candidates) {
			if (SchemaExists(conn, catalog, candidate)) {
				return RenderPath(catalog, candidate);
			}
		}
		if (!parse_error.empty()) {
			throw InvalidInputException(parse_error);
		}
	}
	return RenderPath(catalog, candidates[0]);
}

void SetNoResult(TableFunction::ExecInput &input) {
	auto chunk = input.GetOutputChunk();
	for (idx_t col = 0; col < chunk.GetVectorCount(); col++) {
		chunk.GetVector(col).SetSize(0);
	}
}

//===--------------------------------------------------------------------===//
// dbgen
//===--------------------------------------------------------------------===//

struct DBGenBindData {
	double sf = 0;
	std::optional<std::string> catalog;
	std::optional<std::string> schema;
	std::string suffix;
	uint32_t children = 1;
	int step = -1;
};

//! A running dbgen call: the generator the scans of __tpch_dbgen_table read from
struct DBGenRun {
	std::unique_ptr<tpch::DBGenGenerator> generator;
	//! Rows of the driving tables generated so far, and in total
	std::atomic<idx_t> generated_rows {0};
	idx_t total_rows = 0;
};

//! The running dbgen calls by id, so that the SQL a call runs can refer to its generator
struct DBGenRunRegistry {
	std::mutex lock;
	std::map<int64_t, std::shared_ptr<DBGenRun>> runs;
	int64_t next_id = 0;

	static DBGenRunRegistry &Get() {
		static DBGenRunRegistry registry;
		return registry;
	}

	int64_t Register(std::shared_ptr<DBGenRun> run) {
		std::lock_guard<std::mutex> guard(lock);
		auto id = next_id++;
		runs.emplace(id, std::move(run));
		return id;
	}

	void Unregister(int64_t id) {
		std::lock_guard<std::mutex> guard(lock);
		runs.erase(id);
	}

	std::shared_ptr<DBGenRun> Lookup(int64_t id) {
		std::lock_guard<std::mutex> guard(lock);
		auto entry = runs.find(id);
		return entry == runs.end() ? nullptr : entry->second;
	}
};

struct DBGenGlobalState {
	//! The connection running in the transaction of the call: open until the tables are loaded
	std::unique_ptr<Connection> conn;
	std::shared_ptr<DBGenRun> run;
	int64_t run_id = -1;
	//! Per TPC-H table, the qualified name
	std::vector<std::string> table_names;
	std::atomic<bool> finished {false};

	void Finish() {
		if (run_id >= 0) {
			DBGenRunRegistry::Get().Unregister(run_id);
			run_id = -1;
		}
		// the connection must be closed before the transaction ends
		conn.reset();
		finished = true;
	}

	~DBGenGlobalState() {
		if (run_id >= 0) {
			DBGenRunRegistry::Get().Unregister(run_id);
		}
	}
};

//! The global state of a dbgen call, which is not movable
struct DBGenGlobalStateHolder {
	std::unique_ptr<DBGenGlobalState> state;
};

//! The value of a named argument, which must not be NULL. Omitted arguments carry their default value.
Value GetNamedArgument(FunctionBindInput &input, const char *name) {
	auto value = input.GetConstantArgument(*input.FindArg(name));
	if (value.IsNull()) {
		throw InvalidInputException("Cannot use NULL as function argument");
	}
	return value;
}

//! The step argument when the call passes none
constexpr uint32_t DBGEN_NO_STEP = std::numeric_limits<uint32_t>::max();

void DbgenBind(TableFunction::BindInput &input) {
	DBGenBindData data;
	data.sf = GetNamedArgument(input, "sf").Get<double>();
	auto catalog = std::string(GetNamedArgument(input, "catalog").Get<varchar_t>().view());
	if (!catalog.empty()) {
		data.catalog = catalog;
	}
	auto schema = std::string(GetNamedArgument(input, "schema").Get<varchar_t>().view());
	if (!schema.empty()) {
		data.schema = schema;
	}
	data.suffix = std::string(GetNamedArgument(input, "suffix").Get<varchar_t>().view());
	GetNamedArgument(input, "overwrite");
	data.children = GetNamedArgument(input, "children").Get<uint32_t>();
	auto step = GetNamedArgument(input, "step").Get<uint32_t>();
	if (step != DBGEN_NO_STEP) {
		data.step = static_cast<int>(step);
	}
	if (data.children != 1 && data.step == -1) {
		throw InvalidInputException("Step must be defined when children are defined");
	}
	input.AddResultColumn("Success", input.GetContext().GetFactory().ParseType("BOOLEAN"));
	input.SetBindData<DBGenBindData>(std::move(data));
}

//! Creates the table unless it exists, and checks that an existing one has the TPC-H columns
void CreateTPCHTable(Connection &conn, const tpch::TPCHTableSchema &schema, const std::string &display_name,
                     const std::string &qualified_name) {
	std::string sql = "CREATE TABLE IF NOT EXISTS " + qualified_name + " (";
	for (idx_t i = 0; i < schema.column_count; i++) {
		sql += (i > 0 ? ", " : "") + RenderQuotedIdentifier(schema.columns[i]) + " " + schema.types[i] + " NOT NULL";
	}
	conn.Execute(sql + ")").Drain();

	auto statements = conn.ParseSQL("SELECT * FROM " + qualified_name);
	auto columns = conn.Bind(statements.Next()).output;
	auto prefix = "TPC-H table \"" + display_name + "\" has an incompatible schema: ";
	if (columns.GetFieldCount() != schema.column_count) {
		throw InvalidInputException(prefix + "expected " + std::to_string(schema.column_count) + " columns but found " +
		                            std::to_string(columns.GetFieldCount()));
	}
	for (idx_t i = 0; i < schema.column_count; i++) {
		auto name = std::string(columns.GetFieldName(i));
		if (Lower(name) != schema.columns[i]) {
			throw InvalidInputException(prefix + "expected column \"" + schema.columns[i] + "\" at position " +
			                            std::to_string(i) + " but found " + name);
		}
		auto type = columns.GetFieldType(i).ToText();
		if (type != schema.types[i]) {
			throw InvalidInputException(prefix + "column \"" + name + "\" has type " + type + " but expected " +
			                            schema.types[i]);
		}
	}
}

void DbgenInitGlobal(TableFunction::InitGlobalInput &input) {
	auto &data = input.GetBindData<DBGenBindData>();
	auto &context = input.GetContext();
	auto state = std::unique_ptr<DBGenGlobalState>(new DBGenGlobalState());
	state->conn = std::unique_ptr<Connection>(new Connection(context.ConnectInTransaction()));
	auto &conn = *state->conn;

	// By default, the tables go to the current schema
	auto catalog = data.catalog ? *data.catalog : QueryText(conn, "SELECT current_database()");
	auto schema = data.schema ? *data.schema : QueryText(conn, "SELECT current_schema()");
	auto target = ResolveSchema(conn, catalog, schema);
	for (idx_t i = 0; i < tpch::TPCH_TABLE_COUNT; i++) {
		auto &table = tpch::GetTPCHTableSchema(i);
		auto table_name = std::string(table.name) + data.suffix;
		state->table_names.push_back(target + "." + RenderQuotedIdentifier(table_name));
		CreateTPCHTable(conn, table, table_name, state->table_names.back());
	}

	auto run = std::make_shared<DBGenRun>();
	run->generator = std::unique_ptr<tpch::DBGenGenerator>(
	    new tpch::DBGenGenerator(data.sf, static_cast<int>(data.children), data.step));
	for (idx_t table = 0; table < tpch::TPCH_TABLE_COUNT; table++) {
		for (auto &item : run->generator->WorkItems(table)) {
			run->total_rows += item.count;
		}
	}
	state->run = run;
	state->run_id = DBGenRunRegistry::Get().Register(std::move(run));
	if (state->run->total_rows == 0) {
		state->Finish();
	}
	input.SetGlobalState<DBGenGlobalStateHolder>(DBGenGlobalStateHolder {std::move(state)});
}

//! Loads the tables, one INSERT per table, each of which generates the table's rows in parallel
void DbgenExec(TableFunction::ExecInput &input) {
	auto &state = *input.GetGlobalState<DBGenGlobalStateHolder>().state;
	SetNoResult(input);
	if (state.finished) {
		return;
	}
	for (idx_t table = 0; table < tpch::TPCH_TABLE_COUNT; table++) {
		if (state.run->generator->WorkItems(table).empty()) {
			continue;
		}
		state.conn
		    ->Execute("INSERT INTO " + state.table_names[table] + " SELECT * FROM __tpch_dbgen_table(" +
		              std::to_string(state.run_id) + ", " + std::to_string(table) + ")")
		    .Drain();
	}
	state.Finish();
}

void DbgenProgress(TableFunction::ProgressInput &input) {
	auto holder = input.TryGetGlobalState<DBGenGlobalStateHolder>();
	auto state = holder ? holder->state.get() : nullptr;
	if (!state) {
		input.SetProgress(0);
		return;
	}
	// the engine reports 100 once the call has returned its result
	if (state->finished) {
		input.SetProgress(0.999);
		return;
	}
	auto total = state->run ? state->run->total_rows : 0;
	if (total == 0) {
		input.SetProgress(0);
		return;
	}
	auto generated = std::min<idx_t>(state->run->generated_rows, total);
	input.SetProgress(std::min(static_cast<double>(generated) / static_cast<double>(total), 0.999));
}

//===--------------------------------------------------------------------===//
// __tpch_dbgen_table: the rows of one table of a running dbgen call
//===--------------------------------------------------------------------===//

struct DBGenTableBindData {
	std::shared_ptr<DBGenRun> run;
	idx_t table;
};

struct DBGenTableGlobalState {
	std::atomic<idx_t> next_work_item {0};
};

struct DBGenTableLocalState {
	std::unique_ptr<tpch::DBGenScanner> scanner;
	//! The position of the work item: the rows of a table follow the order of its work items
	idx_t batch_index = 0;
};

void DBGenTableBind(TableFunction::BindInput &input) {
	auto run = DBGenRunRegistry::Get().Lookup(input.GetConstantArgument(0).Get<int64_t>());
	auto table = input.GetConstantArgument(1).Get<int64_t>();
	if (!run || table < 0 || table >= static_cast<int64_t>(tpch::TPCH_TABLE_COUNT)) {
		throw InvalidInputException("__tpch_dbgen_table is internal to dbgen");
	}
	auto &factory = input.GetContext().GetFactory();
	auto &schema = tpch::GetTPCHTableSchema(static_cast<idx_t>(table));
	for (idx_t col = 0; col < schema.column_count; col++) {
		input.AddResultColumn(schema.columns[col], factory.ParseType(schema.types[col]));
	}
	input.SetBindData<DBGenTableBindData>(DBGenTableBindData {std::move(run), static_cast<idx_t>(table)});
}

void DBGenTableInitGlobal(TableFunction::InitGlobalInput &input) {
	auto &data = input.GetBindData<DBGenTableBindData>();
	input.SetGlobalState<DBGenTableGlobalState>();
	input.SetMaxThreads(std::max<idx_t>(data.run->generator->WorkItems(data.table).size(), 1));
}

void DBGenTableInitLocal(TableFunction::InitLocalInput &input) {
	input.SetLocalState<DBGenTableLocalState>();
}

void DBGenTableExec(TableFunction::ExecInput &input) {
	auto &data = input.GetBindData<DBGenTableBindData>();
	auto &global = input.GetGlobalState<DBGenTableGlobalState>();
	auto &local = input.GetLocalState<DBGenTableLocalState>();
	auto &work_items = data.run->generator->WorkItems(data.table);
	auto chunk = input.GetOutputChunk();
	while (true) {
		if (!local.scanner) {
			auto work_item = global.next_work_item++;
			if (work_item >= work_items.size()) {
				SetNoResult(input);
				return;
			}
			local.scanner = std::unique_ptr<tpch::DBGenScanner>(
			    new tpch::DBGenScanner(*data.run->generator, data.table, work_items[work_item]));
			local.batch_index = work_item;
		}
		data.run->generated_rows += local.scanner->Fill(chunk);
		if (chunk.GetRowCount() > 0) {
			return;
		}
		local.scanner.reset();
	}
}

void DBGenTablePartitionData(TableFunction::PartitionDataInput &input) {
	input.SetBatchIndex(input.GetLocalState<DBGenTableLocalState>().batch_index);
}

//===--------------------------------------------------------------------===//
// tpch_queries, tpch_answers
//===--------------------------------------------------------------------===//

//! The "sf" argument, taken through **kwargs so that an omitted one can be told from a NULL one
std::optional<Value> GetScaleFactorKwarg(FunctionBindInput &input, const char *function_name) {
	auto counts = input.GetArgumentCounts();
	std::optional<Value> result;
	for (auto index = counts.NamedOffset() + counts.named_fixed; index < counts.Total(); index++) {
		auto name = Lower(input.GetArgName(index));
		if (name != "sf") {
			throw InvalidInputException(std::string(function_name) + ": unknown named parameter \"" + name + "\"");
		}
		result = input.GetConstantArgument(index);
	}
	return result;
}

struct TPCHQueriesBindData {
	//! Without a scale factor the stored queries are returned unchanged
	bool has_sf = false;
	double sf = 1;

	bool operator==(const TPCHQueriesBindData &other) const {
		return has_sf == other.has_sf && (!has_sf || sf == other.sf);
	}
};

struct TPCHScanState {
	idx_t offset = 0;
};

void TPCHQueriesBind(TableFunction::BindInput &input) {
	TPCHQueriesBindData data;
	auto counts = input.GetArgumentCounts();
	if (counts.positional_variadic > 1) {
		throw InvalidInputException("tpch_queries: expected at most one positional argument, the scale factor");
	}
	std::optional<Value> sf;
	if (counts.positional_variadic == 1) {
		sf = input.GetConstantArgument(counts.positional_fixed);
	}
	if (auto named_sf = GetScaleFactorKwarg(input, "tpch_queries")) {
		if (sf) {
			throw InvalidInputException(
			    "tpch_queries: the scale factor can be given as a positional argument or as \"sf\", not both");
		}
		sf = std::move(named_sf);
	}
	if (sf) {
		if (sf->IsNull()) {
			throw InvalidInputException("tpch_queries: cannot use NULL as scale factor");
		}
		data.has_sf = true;
		data.sf = sf->Cast(input.GetContext().GetFactory(), input.GetContext().GetFactory().ParseType("DOUBLE"))
		              .Get<double>();
	}
	auto &factory = input.GetContext().GetFactory();
	input.AddResultColumn("query_nr", factory.ParseType("INTEGER"));
	input.AddResultColumn("query", factory.ParseType("VARCHAR"));
	input.SetBindData<TPCHQueriesBindData>(data);
}

void TPCHScanInit(TableFunction::InitGlobalInput &input) {
	input.SetGlobalState<TPCHScanState>();
}

void TPCHQueriesExec(TableFunction::ExecInput &input) {
	auto &data = input.GetBindData<TPCHQueriesBindData>();
	auto &state = input.GetGlobalState<TPCHScanState>();
	auto chunk = input.GetOutputChunk();
	auto count = std::min<idx_t>(TPCH_QUERY_COUNT - state.offset, chunk.GetCapacity());
	auto query_nr = chunk.GetVector(0);
	auto query = chunk.GetVector(1);
	query_nr.SetSize(count);
	query.SetSize(count);
	for (idx_t i = 0; i < count; i++) {
		auto nr = static_cast<int>(state.offset + i + 1);
		query_nr.GetDataMutable<int32_t>()[i] = nr;
		query.AssignStringUnsafe(i, data.has_sf ? tpch::DBGenWrapper::GetQuery(nr, data.sf)
		                                        : tpch::DBGenWrapper::GetQuery(nr));
	}
	state.offset += count;
}

void TPCHAnswersBind(TableFunction::BindInput &input) {
	auto &factory = input.GetContext().GetFactory();
	input.AddResultColumn("query_nr", factory.ParseType("INTEGER"));
	input.AddResultColumn("scale_factor", factory.ParseType("DOUBLE"));
	input.AddResultColumn("answer", factory.ParseType("VARCHAR"));
}

void TPCHAnswersExec(TableFunction::ExecInput &input) {
	static constexpr double SCALE_FACTORS[] = {0.01, 0.1, 1};
	static constexpr idx_t TOTAL_ANSWERS = TPCH_QUERY_COUNT * 3;
	auto &state = input.GetGlobalState<TPCHScanState>();
	auto chunk = input.GetOutputChunk();
	auto count = std::min<idx_t>(TOTAL_ANSWERS - state.offset, chunk.GetCapacity());
	auto query_nr = chunk.GetVector(0);
	auto scale_factor = chunk.GetVector(1);
	auto answer = chunk.GetVector(2);
	query_nr.SetSize(count);
	scale_factor.SetSize(count);
	answer.SetSize(count);
	for (idx_t i = 0; i < count; i++) {
		auto offset = state.offset + i;
		auto query = static_cast<int>(offset % TPCH_QUERY_COUNT) + 1;
		auto sf = SCALE_FACTORS[offset / TPCH_QUERY_COUNT];
		query_nr.GetDataMutable<int32_t>()[i] = query;
		scale_factor.GetDataMutable<double>()[i] = sf;
		answer.AssignStringUnsafe(i, tpch::DBGenWrapper::GetAnswer(sf, query));
	}
	state.offset += count;
}

//===--------------------------------------------------------------------===//
// PRAGMA tpch
//===--------------------------------------------------------------------===//

std::string PragmaTpchQuery(PragmaFunction::QueryInput &input) {
	auto query_nr = input.GetConstantArgument(0);
	if (query_nr.IsNull()) {
		throw InvalidInputException("Cannot use NULL as argument for the TPC-H query number");
	}
	auto index = query_nr.Get<int64_t>();
	if (index <= 0 || index > TPCH_QUERY_COUNT) {
		throw InvalidInputException("Out of range TPC-H query number " + std::to_string(index));
	}
	auto sf = GetScaleFactorKwarg(input, "tpch");
	if (!sf) {
		return tpch::DBGenWrapper::GetQuery(static_cast<int>(index));
	}
	if (sf->IsNull()) {
		throw InvalidInputException("Cannot use NULL as scale factor for the TPC-H query");
	}
	return tpch::DBGenWrapper::GetQuery(static_cast<int>(index), sf->Get<double>());
}

} // namespace

DUCKDB_CPP_EXTENSION_ENTRYPOINT(Extension &extension, Context &context) {
	auto &factory = context.GetFactory();
	auto type = [&](const char *name) {
		return factory.ParseType(name);
	};

	auto dbgen = TableFunction::Create(factory);
	dbgen.SetName("dbgen")
	    .SetBindCallback(DbgenBind)
	    .SetInitGlobalCallback(DbgenInitGlobal)
	    .SetExecCallback(DbgenExec)
	    .SetProgressCallback(DbgenProgress);
	auto named = [&](FunctionSignature &signature, const char *name, const char *type_name,
	                 const Value &default_value) {
		signature.AddParameter(name, type(type_name), default_value, FunctionParameterKind::NAMED_ONLY);
	};
	auto dbgen_signature = dbgen.GetSignature();
	named(dbgen_signature, "sf", "DOUBLE", Value::Create(factory, 0.0));
	named(dbgen_signature, "overwrite", "BOOLEAN", Value::Create(factory, false));
	named(dbgen_signature, "catalog", "VARCHAR", Value::Create(factory, varchar_t("")));
	named(dbgen_signature, "schema", "VARCHAR", Value::Create(factory, varchar_t("")));
	named(dbgen_signature, "suffix", "VARCHAR", Value::Create(factory, varchar_t("")));
	named(dbgen_signature, "children", "UINTEGER", Value::Create(factory, uint32_t {1}));
	named(dbgen_signature, "step", "UINTEGER", Value::Create(factory, DBGEN_NO_STEP));
	dbgen.GetDocs().SetDescription("Generates the TPC-H tables at the given scale factor");
	extension.Register(dbgen);

	auto dbgen_table = TableFunction::Create(factory);
	dbgen_table.SetName("__tpch_dbgen_table")
	    .SetBindCallback(DBGenTableBind)
	    .SetInitGlobalCallback(DBGenTableInitGlobal)
	    .SetInitLocalCallback(DBGenTableInitLocal)
	    .SetExecCallback(DBGenTableExec)
	    .SetPartitionDataCallback(DBGenTablePartitionData);
	dbgen_table.GetSignature().AddParameter("run_id", type("BIGINT")).AddParameter("table_index", type("BIGINT"));
	dbgen_table.GetDocs().SetDescription("Generates the rows of a TPC-H table for dbgen; internal to dbgen");
	extension.Register(dbgen_table);

	auto queries = TableFunction::Create(factory);
	queries.SetName("tpch_queries")
	    .SetBindCallback(TPCHQueriesBind)
	    .SetInitGlobalCallback(TPCHScanInit)
	    .SetExecCallback(TPCHQueriesExec)
	    .SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	queries.GetSignature().AddArgs("scale_factor", type("DOUBLE")).AddKwargs("options", type("DOUBLE"));
	queries.GetDocs().SetDescription(
	    "Returns the TPC-H queries, with the substitution parameters of the scale factor if one is given");
	extension.Register(queries);

	auto answers = TableFunction::Create(factory);
	answers.SetName("tpch_answers")
	    .SetBindCallback(TPCHAnswersBind)
	    .SetInitGlobalCallback(TPCHScanInit)
	    .SetExecCallback(TPCHAnswersExec)
	    .SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	answers.GetDocs().SetDescription("Returns the answers of the TPC-H queries at scale factors 0.01, 0.1 and 1");
	extension.Register(answers);

	auto pragma = PragmaFunction::Create(factory);
	pragma.SetName("tpch").SetQueryCallback(PragmaTpchQuery);
	pragma.GetSignature()
	    .AddParameter("query_nr", type("BIGINT"), FunctionParameterKind::POSITIONAL_ONLY)
	    .AddKwargs("options", type("DOUBLE"));
	pragma.GetDocs().SetDescription("Runs a TPC-H query");
	extension.Register(pragma);
}
