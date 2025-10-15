#include "duckdb/function/table/read_duckdb.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

struct DuckDBMultiFileInfo : MultiFileReaderInterface {
	static unique_ptr<MultiFileReaderInterface> CreateInterface(ClientContext &context);

	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                    optional_ptr<TableFunctionInfo> info) override;
	bool ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
	                     BaseFileReaderOptions &options, vector<string> &expected_names,
	                     vector<LogicalType> &expected_types) override;
	bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
	                 BaseFileReaderOptions &options) override;
	void FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options, const vector<string> &expected_names,
	                      const vector<LogicalType> &expected_types) override;
	void FinalizeBindData(MultiFileBindData &multi_file_data) override;

	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override;
	void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                MultiFileBindData &bind_data) override;
	unique_ptr<GlobalTableFunctionState> InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data,
	                                                           MultiFileGlobalState &global_state) override;
	unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &, GlobalTableFunctionState &) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        BaseUnionData &union_data, const MultiFileBindData &bind_data_p) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        const OpenFileInfo &file, idx_t file_idx,
	                                        const MultiFileBindData &bind_data) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                        BaseFileReaderOptions &options,
	                                        const MultiFileOptions &file_options) override;
	void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                   LocalTableFunctionState &local_state) override;
	unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) override;
	void GetVirtualColumns(ClientContext &, MultiFileBindData &, virtual_column_map_t &result) override;
	unique_ptr<MultiFileReaderInterface> Copy() override;
	FileGlobInput GetGlobInput() override;
};

class DuckDBFileReaderOptions : public BaseFileReaderOptions {
public:
	string schema_name;
	string table_name;

	bool Matches(TableCatalogEntry &table) const;
	bool HasSelection() const;
	string PrintOptions() const;
	string GetCandidates(const vector<reference<TableCatalogEntry>> &tables) const;
};

struct DuckDBReadBindData : TableFunctionData {
	unique_ptr<DuckDBFileReaderOptions> options;
	optional_idx initial_file_cardinality;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<DuckDBReadBindData>();
		result->options = make_uniq<DuckDBFileReaderOptions>(*options);
		result->initial_file_cardinality = initial_file_cardinality;
		return std::move(result);
	}
};

struct AttachedDatabaseWrapper {
	AttachedDatabaseWrapper(ClientContext &context, shared_ptr<AttachedDatabase> attached_database_p);
	~AttachedDatabaseWrapper();

	ClientContext &context;
	shared_ptr<AttachedDatabase> attached_database;
	optional_ptr<TableCatalogEntry> table_entry;
};

class DuckDBReader : public BaseFileReader {
public:
	DuckDBReader(ClientContext &context, OpenFileInfo file, const DuckDBFileReaderOptions &options);
	~DuckDBReader() override;

public:
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	void Scan(ClientContext &context, GlobalTableFunctionState &global_state, LocalTableFunctionState &local_state,
	          DataChunk &chunk) override;
	shared_ptr<BaseUnionData> GetUnionData(idx_t file_idx) override;
	void FinishFile(ClientContext &context, GlobalTableFunctionState &gstate) override;
	double GetProgressInFile(ClientContext &context) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, const string &name) override;
	void AddVirtualColumn(column_t virtual_column_id) override;
	string GetReaderType() const override {
		return "duckdb";
	}
	optional_idx NumRows();
	AttachedDatabase &GetAttachedDatabase();
	TableCatalogEntry &GetTableEntry();

private:
	ClientContext &context;
	shared_ptr<AttachedDatabaseWrapper> db_wrapper;
	TableFunction scan_function;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	atomic<bool> finished;
	idx_t column_count;
	string schema_name;
	string table_name;
};

struct DuckDBReadGlobalState : GlobalTableFunctionState {};

struct DuckDBReadLocalState : LocalTableFunctionState {
	unique_ptr<LocalTableFunctionState> local_state;
	shared_ptr<AttachedDatabaseWrapper> attached_database;
};

string DuckDBFileReaderOptions::GetCandidates(const vector<reference<TableCatalogEntry>> &tables) const {
	if (tables.empty()) {
		return string();
	}
	case_insensitive_map_t<idx_t> table_names;
	for (auto &table : tables) {
		table_names[table.get().name]++;
	}
	vector<string> candidate_list;
	for (auto &table_ref : tables) {
		auto &table = table_ref.get();
		if (table_names[table.name] > 1) {
			// name conflicts across schemas - add the schema name
			auto &schema = table.ParentSchema();
			candidate_list.push_back(schema.name + "." + table.name);
		} else {
			candidate_list.push_back(table.name);
		}
	}
	string search_term = schema_name;
	if (!search_term.empty()) {
		search_term += ".";
	}
	search_term += table_name;
	return StringUtil::CandidatesErrorMessage(candidate_list, search_term, "Candidates");
}

bool DuckDBFileReaderOptions::HasSelection() const {
	if (!table_name.empty()) {
		return true;
	}
	if (!schema_name.empty()) {
		return true;
	}
	return false;
}

string DuckDBFileReaderOptions::PrintOptions() const {
	string options;
	if (!schema_name.empty()) {
		options += "schema_name=\"" + schema_name + "\"";
	}
	if (!options.empty()) {
		options += ", ";
	}
	if (!table_name.empty()) {
		options += "table_name=\"" + table_name + "\"";
	}
	return options;
}

bool DuckDBFileReaderOptions::Matches(TableCatalogEntry &table) const {
	if (!schema_name.empty() && !StringUtil::CIEquals(table.ParentSchema().name, schema_name)) {
		return false;
	}
	if (!table_name.empty() && !StringUtil::CIEquals(table.name, table_name)) {
		return false;
	}
	return true;
}

AttachedDatabaseWrapper::AttachedDatabaseWrapper(ClientContext &context,
                                                 shared_ptr<AttachedDatabase> attached_database_p)
    : context(context), attached_database(std::move(attached_database_p)) {
}

AttachedDatabaseWrapper::~AttachedDatabaseWrapper() {
	if (attached_database) {
		auto &db_manager = DatabaseManager::Get(context);
		db_manager.DetachDatabase(context, attached_database->GetName(), OnEntryNotFound::RETURN_NULL);
		attached_database.reset();
	}
}
DuckDBReader::DuckDBReader(ClientContext &context_p, OpenFileInfo file_p, const DuckDBFileReaderOptions &options)
    : BaseFileReader(std::move(file_p)), context(context_p), finished(false) {
	auto &attached = GetAttachedDatabase();
	auto &catalog = attached.GetCatalog();
	vector<reference<TableCatalogEntry>> tables;
	vector<reference<TableCatalogEntry>> candidate_tables;
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
		schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.type != CatalogType::TABLE_ENTRY) {
				return;
			}
			auto &table = entry.Cast<TableCatalogEntry>();
			if (options.Matches(table)) {
				tables.push_back(table);
			}
			candidate_tables.push_back(table);
		});
	});
	if (tables.size() != 1) {
		string error_msg = tables.empty() ? "does not have any tables" : "has multiple tables";
		string extra_info;
		if (!options.HasSelection()) {
			extra_info = "\nSelect a table using `table_name='<name>'";
		} else {
			extra_info = " matching " + options.PrintOptions();
		}
		string candidate_str = options.GetCandidates(candidate_tables);
		throw BinderException("Database \"%s\" %s%s%s", file.path, error_msg, extra_info, candidate_str);
	}
	auto &table = tables[0].get();
	for (auto &col : table.GetColumns().Logical()) {
		columns.emplace_back(col.Name(), col.Type());
	}
	column_count = columns.size();
	schema_name = table.ParentSchema().name;
	table_name = table.name;
	db_wrapper->table_entry = table;
}

DuckDBReader::~DuckDBReader() {
}

AttachedDatabase &DuckDBReader::GetAttachedDatabase() {
	if (!db_wrapper) {
		auto &db_manager = DatabaseManager::Get(context);
		AttachInfo info;
		info.path = file.path;
		// use invalid UTF-8 so that a conflicting database name cannot be attached by a user
		info.name = "\x80__duckdb_reader_" + info.path;

		info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		unordered_map<string, Value> attach_kv;
		AttachOptions attach_options(attach_kv, AccessMode::READ_ONLY);
		attach_options.visibility = AttachVisibility::HIDDEN;

		auto attached = db_manager.AttachDatabase(context, info, attach_options);
		db_wrapper = make_shared_ptr<AttachedDatabaseWrapper>(context, std::move(attached));
	}
	return *db_wrapper->attached_database;
}

TableCatalogEntry &DuckDBReader::GetTableEntry() {
	auto &attached = GetAttachedDatabase();
	if (!db_wrapper->table_entry) {
		auto &catalog = attached.GetCatalog();
		db_wrapper->table_entry =
		    catalog.GetEntry<TableCatalogEntry>(context, schema_name, table_name, OnEntryNotFound::THROW_EXCEPTION);
	}
	return *db_wrapper->table_entry;
}

bool DuckDBReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
                                     LocalTableFunctionState &lstate_p) {
	auto &lstate = lstate_p.Cast<DuckDBReadLocalState>();
	if (finished) {
		lstate.attached_database.reset();
		return false;
	}
	if (!global_state) {
		lstate.attached_database.reset();
		auto &table_entry = GetTableEntry();
		scan_function = table_entry.GetScanFunction(context, bind_data);
		for (auto &col : column_indexes) {
			if (col.GetPrimaryIndex() >= column_count) {
				col = ColumnIndex(COLUMN_IDENTIFIER_ROW_ID);
			} else {
				auto &column = table_entry.GetColumn(LogicalIndex(col.GetPrimaryIndex()));
				if (column.Generated()) {
					throw NotImplementedException("Unsupported: read_duckdb cannot read generated column %s",
					                              column.Name());
				}
			}
		}

		// initialize the scan over this table
		TableFunctionInitInput input(bind_data.get(), column_indexes, vector<idx_t>(), filters.get());
		global_state = scan_function.init_global(context, input);
	}
	AssignSharedPointer(lstate.attached_database, db_wrapper);
	// initialize the local scan
	ThreadContext thread(context);
	ExecutionContext exec_context(context, thread, nullptr);
	TableFunctionInitInput input(bind_data.get(), column_indexes, vector<idx_t>(), filters.get());
	lstate.local_state = scan_function.init_local(exec_context, input, global_state.get());
	return true;
}

void DuckDBReader::Scan(ClientContext &context, GlobalTableFunctionState &gstate_p, LocalTableFunctionState &lstate_p,
                        DataChunk &chunk) {
	chunk.Reset();
	auto &lstate = lstate_p.Cast<DuckDBReadLocalState>();
	TableFunctionInput input(bind_data.get(), lstate.local_state, global_state);
	scan_function.function(context, input, chunk);
	if (chunk.size() == 0) {
		finished = true;
	}
}

void DuckDBReader::FinishFile(ClientContext &context, GlobalTableFunctionState &gstate) {
	db_wrapper.reset();
}

optional_idx DuckDBReader::NumRows() {
	auto &table_entry = GetTableEntry();
	return table_entry.GetStorage().GetTotalRows();
}

unique_ptr<BaseStatistics> DuckDBReader::GetStatistics(ClientContext &context, const string &name) {
	if (!scan_function.statistics) {
		return BaseFileReader::GetStatistics(context, name);
	}
	auto &table_entry = GetTableEntry();
	if (!table_entry.ColumnExists(name)) {
		return nullptr;
	}
	return scan_function.statistics(context, bind_data.get(), table_entry.GetColumn(name).Logical().index);
}

double DuckDBReader::GetProgressInFile(ClientContext &context) {
	if (!scan_function.table_scan_progress || !global_state) {
		return BaseFileReader::GetProgressInFile(context);
	}
	return scan_function.table_scan_progress(context, bind_data.get(), global_state.get());
}

void DuckDBReader::AddVirtualColumn(column_t virtual_column_id) {
	if (virtual_column_id != COLUMN_IDENTIFIER_ROW_ID) {
		throw InternalException("Unsupported virtual column id %d for duckdb reader", virtual_column_id);
	}
}

unique_ptr<MultiFileReaderInterface> DuckDBMultiFileInfo::CreateInterface(ClientContext &context) {
	return make_uniq<DuckDBMultiFileInfo>();
}

unique_ptr<BaseFileReaderOptions> DuckDBMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                         optional_ptr<TableFunctionInfo> info) {
	return make_uniq<DuckDBFileReaderOptions>();
}

bool DuckDBMultiFileInfo::ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
                                          BaseFileReaderOptions &options, vector<string> &expected_names,
                                          vector<LogicalType> &expected_types) {
	return false;
}

bool DuckDBMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &val,
                                      MultiFileOptions &file_options, BaseFileReaderOptions &options_p) {
	auto &options = options_p.Cast<DuckDBFileReaderOptions>();
	if (key == "schema_name") {
		options.schema_name = StringValue::Get(val);
		return true;
	}
	if (key == "table_name") {
		options.table_name = StringValue::Get(val);
		return true;
	}
	return false;
}

void DuckDBMultiFileInfo::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
                                           const vector<string> &expected_names,
                                           const vector<LogicalType> &expected_types) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

void DuckDBMultiFileInfo::FinalizeBindData(MultiFileBindData &multi_file_data) {
	auto &bind_data = multi_file_data.bind_data->Cast<DuckDBReadBindData>();
	if (multi_file_data.initial_reader) {
		auto &initial_reader = multi_file_data.initial_reader->Cast<DuckDBReader>();
		bind_data.initial_file_cardinality = initial_reader.NumRows();
	}
}

unique_ptr<TableFunctionData> DuckDBMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                      unique_ptr<BaseFileReaderOptions> options_p) {
	auto result = make_uniq<DuckDBReadBindData>();
	result->options = unique_ptr_cast<BaseFileReaderOptions, DuckDBFileReaderOptions>(std::move(options_p));
	return std::move(result);
}

void DuckDBMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                                     MultiFileBindData &bind_data) {
	auto &duckdb_bind_data = bind_data.bind_data->Cast<DuckDBReadBindData>();
	bind_data.reader_bind =
	    bind_data.multi_file_reader->BindReader(context, return_types, names, *bind_data.file_list, bind_data,
	                                            *duckdb_bind_data.options, bind_data.file_options);
}

unique_ptr<GlobalTableFunctionState> DuckDBMultiFileInfo::InitializeGlobalState(ClientContext &context,
                                                                                MultiFileBindData &bind_data,
                                                                                MultiFileGlobalState &global_state) {
	return make_uniq<DuckDBReadGlobalState>();
}

unique_ptr<LocalTableFunctionState> DuckDBMultiFileInfo::InitializeLocalState(ExecutionContext &,
                                                                              GlobalTableFunctionState &) {
	return make_uniq<DuckDBReadLocalState>();
}

struct DuckDBReaderUnionData : BaseUnionData {
	explicit DuckDBReaderUnionData(OpenFileInfo file_p) : BaseUnionData(std::move(file_p)) {
	}
};

shared_ptr<BaseFileReader> DuckDBMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
                                                             BaseUnionData &union_data_p,
                                                             const MultiFileBindData &multi_bind_data) {
	auto &union_data = union_data_p.Cast<DuckDBReaderUnionData>();
	auto &bind_data = multi_bind_data.bind_data->Cast<DuckDBReadBindData>();
	return make_shared_ptr<DuckDBReader>(context, union_data.file, *bind_data.options);
}

shared_ptr<BaseFileReader> DuckDBMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
                                                             const OpenFileInfo &file, idx_t file_idx,
                                                             const MultiFileBindData &multi_bind_data) {
	auto &bind_data = multi_bind_data.bind_data->Cast<DuckDBReadBindData>();
	return make_shared_ptr<DuckDBReader>(context, file, *bind_data.options);
}

shared_ptr<BaseFileReader> DuckDBMultiFileInfo::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                             BaseFileReaderOptions &options,
                                                             const MultiFileOptions &file_options) {
	return make_shared_ptr<DuckDBReader>(context, file, options.Cast<DuckDBFileReaderOptions>());
}

shared_ptr<BaseUnionData> DuckDBReader::GetUnionData(idx_t file_idx) {
	auto result = make_uniq<DuckDBReaderUnionData>(file);
	for (auto &column : columns) {
		result->names.push_back(column.name);
		result->types.push_back(column.type);
	}
	result->reader = shared_from_this();
	return std::move(result);
}

void DuckDBMultiFileInfo::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                        LocalTableFunctionState &lstate_p) {
	auto &lstate = lstate_p.Cast<DuckDBReadLocalState>();
	lstate.attached_database.reset();
}

unique_ptr<NodeStatistics> DuckDBMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data_p, idx_t file_count) {
	auto &bind_data = bind_data_p.bind_data->Cast<DuckDBReadBindData>();
	idx_t estimated_cardinality = file_count;
	if (bind_data.initial_file_cardinality.IsValid()) {
		estimated_cardinality = file_count * bind_data.initial_file_cardinality.GetIndex();
	}
	return make_uniq<NodeStatistics>(estimated_cardinality);
}

unique_ptr<MultiFileReaderInterface> DuckDBMultiFileInfo::Copy() {
	return make_uniq<DuckDBMultiFileInfo>();
}

FileGlobInput DuckDBMultiFileInfo::GetGlobInput() {
	return FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "db");
}

void DuckDBMultiFileInfo::GetVirtualColumns(ClientContext &, MultiFileBindData &, virtual_column_map_t &result) {
	result.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::BIGINT)));
}

void ReadDuckDBAddNamedParameters(TableFunction &table_function) {
	table_function.named_parameters["schema_name"] = LogicalType::VARCHAR;
	table_function.named_parameters["table_name"] = LogicalType::VARCHAR;

	MultiFileReader::AddParameters(table_function);
}

static vector<column_t> DuckDBGetRowIdColumns(ClientContext &, optional_ptr<FunctionData>) {
	vector<column_t> result;
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX);
	result.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	return result;
}

static bool DuckDBScanPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr) {
	return true;
}

TableFunction ReadDuckDBTableFunction::GetFunction() {
	MultiFileFunction<DuckDBMultiFileInfo> read_duckdb("read_duckdb");
	read_duckdb.statistics = MultiFileFunction<DuckDBMultiFileInfo>::MultiFileScanStats;
	read_duckdb.get_row_id_columns = DuckDBGetRowIdColumns;
	read_duckdb.pushdown_expression = DuckDBScanPushdownExpression;
	read_duckdb.filter_pushdown = true;
	read_duckdb.filter_prune = true;
	read_duckdb.late_materialization = true;
	ReadDuckDBAddNamedParameters(read_duckdb);
	return static_cast<TableFunction>(read_duckdb);
}

unique_ptr<TableRef> ReadDuckDBTableFunction::ReplacementScan(ClientContext &context, ReplacementScanInput &input,
                                                              optional_ptr<ReplacementScanData>) {
	auto table_name = ReplacementScan::GetFullPath(input);
	auto lower_name = StringUtil::Lower(table_name);
	if (!StringUtil::EndsWith(lower_name, ".db") && !StringUtil::Contains(lower_name, ".db?") &&
	    !StringUtil::EndsWith(lower_name, ".ddb") && !StringUtil::Contains(lower_name, ".ddb?") &&
	    !StringUtil::EndsWith(lower_name, ".duckdb") && !StringUtil::Contains(lower_name, ".duckdb?")) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("read_duckdb", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		auto &fs = FileSystem::GetFileSystem(context);
		table_function->alias = fs.ExtractBaseName(table_name);
	}
	return std::move(table_function);
}

} // namespace duckdb
