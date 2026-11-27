#include "duckdb/function/table/read_duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"

namespace duckdb {

struct DuckDBMultiFileInfo : MultiFileReaderInterface {
	static unique_ptr<MultiFileReaderInterface> CreateInterface(ClientContext &context);

	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                    optional_ptr<TableFunctionInfo> info) override;
	bool ParseCopyOption(ClientContext &context, const Identifier &key, const vector<Value> &values,
	                     BaseFileReaderOptions &options, vector<Identifier> &expected_names,
	                     vector<LogicalType> &expected_types) override;
	bool ParseOption(ClientContext &context, const Identifier &key, const Value &val, MultiFileOptions &file_options,
	                 BaseFileReaderOptions &options) override;
	void FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
	                      const vector<Identifier> &expected_names, const vector<LogicalType> &expected_types) override;
	void FinalizeBindData(MultiFileBindData &multi_file_data) override;

	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override;
	void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<Identifier> &names,
	                MultiFileBindData &bind_data) override;
	unique_ptr<GlobalTableFunctionState> InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data,
	                                                           MultiFileGlobalState &global_state) override;
	unique_ptr<LocalTableFunctionState> InitializeLocalState(ClientContext &, GlobalTableFunctionState &) override;
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
	Identifier schema_name;
	Identifier table_name;

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

struct DuckDBReadLocalState;

class DuckDBReader : public BaseFileReader {
public:
	DuckDBReader(ClientContext &context, OpenFileInfo file, const DuckDBFileReaderOptions &options);
	~DuckDBReader() override;

public:
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;
	shared_ptr<BaseUnionData> GetUnionData(idx_t file_idx) override;
	void FinishFile(ClientContext &context, GlobalTableFunctionState &gstate) override;
	double GetProgressInFile(ClientContext &context) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, const Identifier &name) override;
	void AddVirtualColumn(column_t virtual_column_id) override;
	string GetReaderType() const override {
		return "duckdb";
	}
	optional_idx NumRows();
	AttachedDatabase &GetAttachedDatabase();
	TableCatalogEntry &GetTableEntry();

private:
	//! The filters the table scan evaluates - those on the columns that are read as the file stores them
	optional_ptr<TableFilterSet> GetScanFilters();
	//! Prepares the conversions of expression_map and the filters that apply to their results
	void InitializeConvertedColumns(ClientContext &context, DuckDBReadLocalState &lstate);
	//! Scans the table into the chunk of the file, converts the columns of expression_map and applies their filters
	void ScanConvertedColumns(ClientContext &context, TableFunctionInput &input, DuckDBReadLocalState &lstate,
	                          DataChunk &chunk);

private:
	ClientContext &context;
	shared_ptr<AttachedDatabaseWrapper> db_wrapper;
	TableFunction scan_function;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	atomic<bool> finished;
	//! The column indexes to scan - the row id is scanned if no columns are projected
	vector<ColumnIndex> scan_column_indexes;
	//! The subset of the filters that is pushed into the table scan - only set when some columns are converted
	unique_ptr<TableFilterSet> scan_filters;
	idx_t column_count;
	Identifier schema_name;
	Identifier table_name;
};

struct DuckDBReadGlobalState : GlobalTableFunctionState {};

//! A scan column the multi-file scan wants converted before its filters apply (see BaseFileReader::expression_map)
struct DuckDBConvertedColumn {
	//! The position of the scan column the conversion produces
	idx_t position;
	unique_ptr<Expression> expression;
	unique_ptr<ExpressionExecutor> executor;
	//! The columns of the file the expression reads - the scan columns starting at position
	unique_ptr<DataChunk> input;
};

struct DuckDBReadLocalState : LocalTableFunctionState {
	unique_ptr<LocalTableFunctionState> local_state;
	shared_ptr<AttachedDatabaseWrapper> attached_database;
	//! The chunk to scan into when no columns are projected (we then scan the row ids)
	DataChunk row_id_chunk;
	//! The columns of the current file that are converted after the table scan
	vector<DuckDBConvertedColumn> converted_columns;
	//! For each scan column, whether it is one of the converted columns
	vector<bool> converted_position;
	//! The filters on the converted columns, as the position of the scan column and the filter state
	vector<pair<idx_t, unique_ptr<TableFilterState>>> converted_filters;
	//! What the table scan produces, in the types of the file, when some columns are converted afterwards
	DataChunk file_chunk;
	//! Whether the previous scan into file_chunk was blocked
	bool resuming_blocked_scan = false;
};

string DuckDBFileReaderOptions::GetCandidates(const vector<reference<TableCatalogEntry>> &tables) const {
	if (tables.empty()) {
		return string();
	}
	identifier_map_t<idx_t> table_names;
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
			candidate_list.push_back(table.name.GetIdentifierName());
		}
	}
	string search_term = schema_name.GetIdentifierName();
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
	if (!schema_name.empty() && table.ParentSchema().name != schema_name) {
		return false;
	}
	if (!table_name.empty() && table.name != table_name) {
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
		auto name = attached_database->GetName();
		attached_database.reset();
		db_manager.DetachDatabase(context, name, OnEntryNotFound::RETURN_NULL);
	}
}
DuckDBReader::DuckDBReader(ClientContext &context_p, OpenFileInfo file_p, const DuckDBFileReaderOptions &options)
    : BaseFileReader(std::move(file_p)), context(context_p), finished(false) {
	auto &attached = GetAttachedDatabase();
	auto &catalog = attached.GetCatalog();
	if (!catalog.IsDuckCatalog()) {
		throw NotImplementedException("read_duckdb can only be used to read DuckDB files - \"%s\" is of type \"%s\"",
		                              catalog.GetDBPath(), catalog.GetCatalogType());
	}
	vector<reference<TableCatalogEntry>> tables;
	vector<reference<TableCatalogEntry>> candidate_tables;
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
		schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
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
		// create the child column definitions as well, so that STRUCT children are mapped by name
		columns.push_back(MultiFileColumnDefinition::CreateFromNameAndType(col.Name(), col.Type()));
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
		// a unique name per reader: each reader detaches its own attachment when it finishes
		info.name = GenerateInternalName("__duckdb_reader_");
		info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		unordered_map<string, Value> attach_kv;
		AttachOptions attach_options(context.GetContext(), attach_kv, AccessMode::READ_ONLY);
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
		db_wrapper->table_entry = catalog.GetEntry<TableCatalogEntry>(
		    context, QualifiedName(catalog.GetName(), schema_name, table_name), OnEntryNotFound::THROW_EXCEPTION);
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

		scan_column_indexes = column_indexes;
		if (scan_column_indexes.empty()) {
			// the table scan requires at least one column - scan the row id if no columns are projected
			scan_column_indexes.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
		}
		if (filters && !expression_map.empty()) {
			// a filter on a converted column is written against the converted values, not against those of the file -
			// it is applied after the conversion, the table scan only evaluates the filters on the other columns
			scan_filters = filters->Copy();
			for (auto &entry : expression_map) {
				if (scan_filters->HasFilter(entry.first)) {
					scan_filters->RemoveFilterByColumnIndex(entry.first);
				}
			}
		}
		// initialize the scan over this table
		TableFunctionInitInput input(bind_data.get(), scan_column_indexes, vector<idx_t>(), GetScanFilters());
		global_state = scan_function.init_global(context, input);
	}
	AssignSharedPointer(lstate.attached_database, db_wrapper);
	// initialize the local scan
	ThreadContext thread(context);
	ExecutionContext exec_context(context, thread, nullptr);
	TableFunctionInitInput input(bind_data.get(), scan_column_indexes, vector<idx_t>(), GetScanFilters());
	lstate.local_state = scan_function.init_local(exec_context, input, global_state.get());
	InitializeConvertedColumns(context, lstate);
	if (column_indexes.empty() && lstate.row_id_chunk.ColumnCount() == 0) {
		lstate.row_id_chunk.Initialize(context, {LogicalType::ROW_TYPE});
	}
	return true;
}

optional_ptr<TableFilterSet> DuckDBReader::GetScanFilters() {
	if (!scan_filters) {
		return filters.get();
	}
	return scan_filters->HasFilters() ? scan_filters.get() : nullptr;
}

void DuckDBReader::InitializeConvertedColumns(ClientContext &context, DuckDBReadLocalState &lstate) {
	lstate.converted_columns.clear();
	lstate.converted_position.clear();
	lstate.converted_filters.clear();
	lstate.file_chunk.Destroy();
	lstate.resuming_blocked_scan = false;
	for (auto &entry : expression_map) {
		DuckDBConvertedColumn converted;
		converted.position = entry.first.GetIndex();
		converted.expression = entry.second.expression->Copy();
		converted.executor = make_uniq<ExpressionExecutor>(context, *converted.expression);
		// the expression refers to the scan columns starting at its position, which the table scan produces as stored
		vector<LogicalType> input_types(entry.second.column_indexes.size());
		ExpressionIterator::VisitExpression<BoundReferenceExpression>(
		    *converted.expression,
		    [&](const BoundReferenceExpression &ref) { input_types[ref.Index()] = ref.GetReturnType(); });
		if (converted.position + input_types.size() > scan_column_indexes.size()) {
			throw InternalException("Converted column %llu of read_duckdb is out of range", converted.position);
		}
		converted.input = make_uniq<DataChunk>();
		converted.input->InitializeEmpty(input_types);
		lstate.converted_position.resize(scan_column_indexes.size(), false);
		lstate.converted_position[converted.position] = true;
		lstate.converted_columns.push_back(std::move(converted));
	}
	if (lstate.converted_columns.empty() || !filters) {
		return;
	}
	for (auto &entry : *filters) {
		if (expression_map.find(entry.GetIndex()) != expression_map.end()) {
			lstate.converted_filters.emplace_back(entry.GetIndex().GetIndex(),
			                                      TableFilterState::Initialize(context, entry.Filter()));
		}
	}
}

void DuckDBReader::ScanConvertedColumns(ClientContext &context, TableFunctionInput &input, DuckDBReadLocalState &lstate,
                                        DataChunk &chunk) {
	auto &file_chunk = lstate.file_chunk;
	if (file_chunk.ColumnCount() == 0) {
		// the table scan produces the columns the expressions read in the types of the file
		auto file_types = chunk.GetTypes();
		for (auto &converted : lstate.converted_columns) {
			for (idx_t i = 0; i < converted.input->ColumnCount(); i++) {
				file_types[converted.position + i] = converted.input->data[i].GetType();
			}
		}
		file_chunk.Initialize(context, file_types);
	}
	if (!lstate.resuming_blocked_scan) {
		file_chunk.Reset();
	}
	scan_function.function(context, input, file_chunk);
	lstate.resuming_blocked_scan = input.async_result.GetResultType() == AsyncResultType::BLOCKED;
	const auto count = file_chunk.size();
	if (lstate.resuming_blocked_scan || count == 0) {
		return;
	}
	for (auto &converted : lstate.converted_columns) {
		auto &converted_input = *converted.input;
		for (idx_t i = 0; i < converted_input.ColumnCount(); i++) {
			converted_input.data[i].Reference(file_chunk.data[converted.position + i]);
		}
		converted_input.SetChildCardinality(count);
		converted.executor->ExecuteExpression(converted_input, chunk.data[converted.position]);
	}
	// the other columns are produced as stored
	for (idx_t i = 0; i < file_chunk.ColumnCount(); i++) {
		if (!lstate.converted_position[i]) {
			chunk.data[i].Reference(file_chunk.data[i]);
		}
	}
	chunk.SetChildCardinality(count);

	// apply the filters on the converted columns
	idx_t remaining = count;
	SelectionVector selection;
	selection.Initialize(nullptr);
	for (auto &filter : lstate.converted_filters) {
		if (remaining == 0) {
			break;
		}
		ColumnSegment::FilterSelection(selection, chunk.data[filter.first], *filter.second, count, remaining);
	}
	if (remaining == 0) {
		chunk.Reset();
	} else if (remaining != count) {
		chunk.Slice(selection, remaining);
	}
}

AsyncResult DuckDBReader::Scan(ClientContext &context, GlobalTableFunctionState &gstate_p,
                               LocalTableFunctionState &lstate_p, DataChunk &chunk) {
	auto &lstate = lstate_p.Cast<DuckDBReadLocalState>();
	TableFunctionInput input(bind_data.get(), lstate.local_state, global_state);

	if (!scan_function.function) {
		throw InternalException("DuckDBReader works only with simple table functions");
	} else {
		input.async_result = AsyncResultType::IMPLICIT;
		input.results_execution_mode = AsyncResultsExecutionMode::TASK_EXECUTOR;
		if (column_indexes.empty()) {
			// no columns are projected - scan the row ids and only emit the count
			lstate.row_id_chunk.Reset();
			scan_function.function(context, input, lstate.row_id_chunk);
			chunk.SetChildCardinality(lstate.row_id_chunk.size());
		} else if (!lstate.converted_columns.empty()) {
			ScanConvertedColumns(context, input, lstate, chunk);
		} else {
			scan_function.function(context, input, chunk);
		}

		if (input.async_result.GetResultType() == AsyncResultType::BLOCKED) {
			return std::move(input.async_result);
		}
		// the filters on converted columns can reject every row the table scan produced - the scan ends when the table
		// scan produces nothing
		auto scan_count = lstate.converted_columns.empty() ? chunk.size() : lstate.file_chunk.size();
		auto source_result = AsyncResult::GetSourceResultType(input.async_result.GetResultType(), scan_count);
		if (source_result == SourceResultType::FINISHED) {
			finished = true;
		}
		return source_result;
	}
}

void DuckDBReader::FinishFile(ClientContext &context, GlobalTableFunctionState &gstate) {
	db_wrapper.reset();
}

optional_idx DuckDBReader::NumRows() {
	auto &table_entry = GetTableEntry();
	return table_entry.GetStorage().GetTotalRows();
}

unique_ptr<BaseStatistics> DuckDBReader::GetStatistics(ClientContext &context, const Identifier &name) {
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
	if (virtual_column_id != COLUMN_IDENTIFIER_ROW_ID && virtual_column_id != COLUMN_IDENTIFIER_ROW_NUMBER) {
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

bool DuckDBMultiFileInfo::ParseCopyOption(ClientContext &context, const Identifier &key, const vector<Value> &values,
                                          BaseFileReaderOptions &options, vector<Identifier> &expected_names,
                                          vector<LogicalType> &expected_types) {
	return false;
}

bool DuckDBMultiFileInfo::ParseOption(ClientContext &context, const Identifier &key, const Value &val,
                                      MultiFileOptions &file_options, BaseFileReaderOptions &options_p) {
	auto &options = options_p.Cast<DuckDBFileReaderOptions>();
	if ((key == "schema_name" || key == "table_name") && val.IsNull()) {
		throw BinderException("read_duckdb parameter %s cannot be NULL", key);
	}
	if (key == "schema_name") {
		options.schema_name = Identifier(StringValue::Get(val));
		return true;
	}
	if (key == "table_name") {
		options.table_name = Identifier(StringValue::Get(val));
		return true;
	}
	return false;
}

void DuckDBMultiFileInfo::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options,
                                           const vector<Identifier> &expected_names,
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

void DuckDBMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types,
                                     vector<Identifier> &names, MultiFileBindData &bind_data) {
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

unique_ptr<LocalTableFunctionState> DuckDBMultiFileInfo::InitializeLocalState(ClientContext &,
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
		result->names.push_back(column.name.GetIdentifierName());
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
	result.insert(make_pair(COLUMN_IDENTIFIER_ROW_NUMBER, TableColumn("row_number", LogicalType::BIGINT)));
}

void ReadDuckDBAddNamedParameters(TableFunction &table_function) {
	// extends the options MultiFileFunction declares
	table_function.GetSignature().ExtendTypedKwargs([](TypedKwargs &options) {
		options.Add("schema_name", LogicalType::VARCHAR).Add("table_name", LogicalType::VARCHAR);
	});
}

static vector<column_t> DuckDBGetRowIdColumns(ClientContext &, optional_ptr<const FunctionData>) {
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
	read_duckdb.statistics_extended = MultiFileFunction<DuckDBMultiFileInfo>::MultiFileScanStatsExtended;
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
	children.push_back(ConstantExpression::String(table_name));
	table_function->function = make_uniq<FunctionExpression>("read_duckdb", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		auto &fs = FileSystem::GetFileSystem(context);
		table_function->alias = Identifier(fs.ExtractBaseName(table_name));
	}
	return std::move(table_function);
}

} // namespace duckdb
