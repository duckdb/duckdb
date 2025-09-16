#include "duckdb/function/table/read_duckdb.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

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
	FileGlobInput GetGlobInput() override;
};

class DuckDBFileReaderOptions : public BaseFileReaderOptions {
public:
	DuckDBFileReaderOptions() = default;
};

struct DuckDBReadBindData : TableFunctionData {
	unique_ptr<DuckDBFileReaderOptions> options;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<DuckDBReadBindData>();
		return std::move(result);
	}
};

class DuckDBReader : public BaseFileReader {
public:
	DuckDBReader(ClientContext &context, OpenFileInfo file);
	~DuckDBReader() override;

public:
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	void Scan(ClientContext &context, GlobalTableFunctionState &global_state, LocalTableFunctionState &local_state,
	          DataChunk &chunk) override;

	string GetReaderType() const override {
		return "duckdb";
	}

private:
	ClientContext &context;
	shared_ptr<AttachedDatabase> attached_database;
	optional_ptr<TableCatalogEntry> table_entry;
	TableFunction scan_function;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	atomic<bool> finished;
};

struct DuckDBReadGlobalState : GlobalTableFunctionState {};

struct DuckDBReadLocalState : LocalTableFunctionState {
	unique_ptr<LocalTableFunctionState> local_state;
};

DuckDBReader::DuckDBReader(ClientContext &context_p, OpenFileInfo file_p)
    : BaseFileReader(std::move(file_p)), context(context_p), finished(false) {
	auto &db_manager = DatabaseManager::Get(context);
	AttachInfo info;
	info.path = file.path;
	info.name = "__duckdb_reader_" + info.path;
	info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	unordered_map<string, Value> attach_options;
	AttachOptions options(attach_options, AccessMode::READ_ONLY);

	attached_database = db_manager.AttachDatabase(context, info, options);

	auto &catalog = attached_database->GetCatalog();
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
		schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.type != CatalogType::TABLE_ENTRY) {
				return;
			}
			auto &table = entry.Cast<TableCatalogEntry>();
			if (table_entry) {
				throw BinderException("Database \"%s\" has multiple table entries", file.path);
			}
			table_entry = table;
		});
	});
	if (!table_entry) {
		throw BinderException("Database \"%s\" does not have any tables", file.path);
	}
	for (auto &col : table_entry->GetColumns().Logical()) {
		columns.emplace_back(col.Name(), col.Type());
	}
}

DuckDBReader::~DuckDBReader() {
	if (attached_database) {
		auto &db_manager = DatabaseManager::Get(context);
		db_manager.DetachDatabase(context, attached_database->GetName(), OnEntryNotFound::RETURN_NULL);
	}
}

bool DuckDBReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
                                     LocalTableFunctionState &lstate_p) {
	auto &lstate = lstate_p.Cast<DuckDBReadLocalState>();
	if (finished) {
		return false;
	}
	if (!global_state) {
		// initialize the scan over this table
		scan_function = table_entry->GetScanFunction(context, bind_data);
		TableFunctionInitInput input(bind_data.get(), column_indexes, vector<idx_t>(), nullptr);
		global_state = scan_function.init_global(context, input);
	}
	// initialize the local scan
	ThreadContext thread(context);
	ExecutionContext exec_context(context, thread, nullptr);
	TableFunctionInitInput input(bind_data.get(), column_indexes, vector<idx_t>(), nullptr);
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
                                      MultiFileOptions &file_options, BaseFileReaderOptions &options) {
	return false;
}

void DuckDBMultiFileInfo::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
                                           const vector<string> &expected_names,
                                           const vector<LogicalType> &expected_types) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

unique_ptr<TableFunctionData> DuckDBMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                      unique_ptr<BaseFileReaderOptions> options_p) {
	auto result = make_uniq<DuckDBReadBindData>();
	result->options = unique_ptr_cast<BaseFileReaderOptions, DuckDBFileReaderOptions>(std::move(options_p));
	return result;
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

shared_ptr<BaseFileReader> DuckDBMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
                                                             BaseUnionData &union_data,
                                                             const MultiFileBindData &bind_data_p) {
	throw InternalException("Unimplemented method in DuckDBMultiFileInfo");
}

shared_ptr<BaseFileReader> DuckDBMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
                                                             const OpenFileInfo &file, idx_t file_idx,
                                                             const MultiFileBindData &bind_data) {
	return make_shared_ptr<DuckDBReader>(context, file);
}

shared_ptr<BaseFileReader> DuckDBMultiFileInfo::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                             BaseFileReaderOptions &options,
                                                             const MultiFileOptions &file_options) {
	return make_shared_ptr<DuckDBReader>(context, file);
}

void DuckDBMultiFileInfo::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                        LocalTableFunctionState &local_state) {
}

unique_ptr<NodeStatistics> DuckDBMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data_p, idx_t file_count) {
	// FIXME: get cardinality of table
	return make_uniq<NodeStatistics>(file_count);
}

FileGlobInput DuckDBMultiFileInfo::GetGlobInput() {
	return FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "db");
}

TableFunction ReadDuckDBTableFunction::GetFunction() {
	MultiFileFunction<DuckDBMultiFileInfo> read_duckdb("read_duckdb");
	return static_cast<TableFunction>(read_duckdb);
}

} // namespace duckdb
