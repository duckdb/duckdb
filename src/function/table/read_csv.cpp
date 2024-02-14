#include "duckdb/function/table/read_csv.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/table_function/global_csv_state.hpp"
#include "duckdb/execution/operator/csv_scanner/util/csv_error.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/csv_scanner/table_function/csv_file_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/base_scanner.hpp"

#include "duckdb/execution/operator/csv_scanner/scanner/string_value_scanner.hpp"

#include <limits>

namespace duckdb {

unique_ptr<CSVFileHandle> ReadCSV::OpenCSV(const string &file_path, FileCompressionType compression,
                                           ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto &allocator = BufferAllocator::Get(context);
	return CSVFileHandle::OpenFile(fs, allocator, file_path, compression);
}

ReadCSVData::ReadCSVData() {
}

void ReadCSVData::FinalizeRead(ClientContext &context) {
	BaseCSVData::Finalize();
	if (!options.rejects_recovery_columns.empty()) {
		for (auto &recovery_col : options.rejects_recovery_columns) {
			bool found = false;
			for (idx_t col_idx = 0; col_idx < return_names.size(); col_idx++) {
				if (StringUtil::CIEquals(return_names[col_idx], recovery_col)) {
					options.rejects_recovery_column_ids.push_back(col_idx);
					found = true;
					break;
				}
			}
			if (!found) {
				throw BinderException("Unsupported parameter for REJECTS_RECOVERY_COLUMNS: column \"%s\" not found",
				                      recovery_col);
			}
		}
	}
}

static unique_ptr<FunctionData> ReadCSVBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<ReadCSVData>();
	auto &options = result->options;
	result->files = MultiFileReader::GetFileList(context, input.inputs[0], "CSV");

	options.FromNamedParameters(input.named_parameters, context, return_types, names);

	// Validate rejects_table options
	if (!options.rejects_table_name.empty()) {
		if (!options.ignore_errors) {
			throw BinderException("REJECTS_TABLE option is only supported when IGNORE_ERRORS is set to true");
		}
		if (options.file_options.union_by_name) {
			throw BinderException("REJECTS_TABLE option is not supported when UNION_BY_NAME is set to true");
		}
	}

	if (options.rejects_limit != 0) {
		if (options.rejects_table_name.empty()) {
			throw BinderException("REJECTS_LIMIT option is only supported when REJECTS_TABLE is set to a table name");
		}
	}

	if (!options.rejects_recovery_columns.empty() && options.rejects_table_name.empty()) {
		throw BinderException(
		    "REJECTS_RECOVERY_COLUMNS option is only supported when REJECTS_TABLE is set to a table name");
	}

	options.file_options.AutoDetectHivePartitioning(result->files, context);

	if (!options.auto_detect && return_types.empty()) {
		throw BinderException("read_csv requires columns to be specified through the 'columns' option. Use "
		                      "read_csv_auto or set read_csv(..., "
		                      "AUTO_DETECT=TRUE) to automatically guess columns.");
	}
	if (options.auto_detect && !options.file_options.union_by_name) {
		options.file_path = result->files[0];
		result->buffer_manager = make_shared<CSVBufferManager>(context, options, result->files[0], 0);
		CSVSniffer sniffer(options, result->buffer_manager, CSVStateMachineCache::Get(context),
		                   {&return_types, &names});
		auto sniffer_result = sniffer.SniffCSV();
		if (names.empty()) {
			names = sniffer_result.names;
			return_types = sniffer_result.return_types;
		}
		result->csv_types = return_types;
		result->csv_names = names;
	}

	D_ASSERT(return_types.size() == names.size());
	result->options.dialect_options.num_cols = names.size();
	if (options.file_options.union_by_name) {
		result->reader_bind =
		    MultiFileReader::BindUnionReader<CSVFileScan>(context, return_types, names, *result, options);
		if (result->union_readers.size() > 1) {
			result->column_info.emplace_back(result->initial_reader->names, result->initial_reader->types);
			for (idx_t i = 1; i < result->union_readers.size(); i++) {
				result->column_info.emplace_back(result->union_readers[i]->names, result->union_readers[i]->types);
			}
		}
		if (!options.sql_types_per_column.empty()) {
			auto exception = CSVError::ColumnTypesError(options.sql_types_per_column, names);
			if (!exception.error_message.empty()) {
				throw BinderException(exception.error_message);
			}
			for (idx_t i = 0; i < names.size(); i++) {
				auto it = options.sql_types_per_column.find(names[i]);
				if (it != options.sql_types_per_column.end()) {
					return_types[i] = options.sql_type_list[it->second];
				}
			}
		}
		result->csv_types = return_types;
		result->csv_names = names;
	} else {
		result->csv_types = return_types;
		result->csv_names = names;
		result->reader_bind = MultiFileReader::BindOptions(options.file_options, result->files, return_types, names);
	}
	result->return_types = return_types;
	result->return_names = names;

	result->FinalizeRead(context);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Read CSV Local State
//===--------------------------------------------------------------------===//
struct CSVLocalState : public LocalTableFunctionState {
public:
	explicit CSVLocalState(unique_ptr<StringValueScanner> csv_reader_p) : csv_reader(std::move(csv_reader_p)) {
	}

	//! The CSV reader
	unique_ptr<StringValueScanner> csv_reader;
	bool done = false;
};

//===--------------------------------------------------------------------===//
// Read CSV Functions
//===--------------------------------------------------------------------===//
static unique_ptr<GlobalTableFunctionState> ReadCSVInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadCSVData>();

	// Create the temporary rejects table
	auto rejects_table = bind_data.options.rejects_table_name;
	if (!rejects_table.empty()) {
		CSVRejectsTable::GetOrCreate(context, rejects_table)->InitializeTable(context, bind_data);
	}
	if (bind_data.files.empty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return nullptr;
	}
	return make_uniq<CSVGlobalState>(context, bind_data.buffer_manager, bind_data.options,
	                                 context.db->NumberOfThreads(), bind_data.files, input.column_ids, bind_data);
}

unique_ptr<LocalTableFunctionState> ReadCSVInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state_p) {
	if (!global_state_p) {
		return nullptr;
	}
	auto &global_state = global_state_p->Cast<CSVGlobalState>();
	auto csv_scanner = global_state.Next();
	if (!csv_scanner) {
		global_state.DecrementThread();
	}
	return make_uniq<CSVLocalState>(std::move(csv_scanner));
}

static void ReadCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ReadCSVData>();
	if (!data_p.global_state) {
		return;
	}
	auto &csv_global_state = data_p.global_state->Cast<CSVGlobalState>();
	auto &csv_local_state = data_p.local_state->Cast<CSVLocalState>();

	if (!csv_local_state.csv_reader) {
		// no csv_reader was set, this can happen when a filename-based filter has filtered out all possible files
		return;
	}
	do {
		if (output.size() != 0) {
			MultiFileReader::FinalizeChunk(bind_data.reader_bind,
			                               csv_local_state.csv_reader->csv_file_scan->reader_data, output);
			break;
		}
		if (csv_local_state.csv_reader->FinishedIterator()) {
			csv_local_state.csv_reader->csv_file_scan->error_handler->Insert(
			    csv_local_state.csv_reader->GetBoundaryIndex(), csv_local_state.csv_reader->GetLinesRead());
			csv_local_state.csv_reader = csv_global_state.Next();
			if (!csv_local_state.csv_reader) {
				csv_global_state.DecrementThread();
				break;
			}
		}
		csv_local_state.csv_reader->Flush(output);

	} while (true);
}

static idx_t CSVReaderGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                    LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &data = local_state->Cast<CSVLocalState>();
	return data.csv_reader->scanner_idx;
}

void ReadCSVTableFunction::ReadCSVAddNamedParameters(TableFunction &table_function) {
	table_function.named_parameters["sep"] = LogicalType::VARCHAR;
	table_function.named_parameters["delim"] = LogicalType::VARCHAR;
	table_function.named_parameters["quote"] = LogicalType::VARCHAR;
	table_function.named_parameters["new_line"] = LogicalType::VARCHAR;
	table_function.named_parameters["escape"] = LogicalType::VARCHAR;
	table_function.named_parameters["nullstr"] = LogicalType::VARCHAR;
	table_function.named_parameters["columns"] = LogicalType::ANY;
	table_function.named_parameters["auto_type_candidates"] = LogicalType::ANY;
	table_function.named_parameters["header"] = LogicalType::BOOLEAN;
	table_function.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
	table_function.named_parameters["sample_size"] = LogicalType::BIGINT;
	table_function.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
	table_function.named_parameters["dateformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestampformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["normalize_names"] = LogicalType::BOOLEAN;
	table_function.named_parameters["compression"] = LogicalType::VARCHAR;
	table_function.named_parameters["skip"] = LogicalType::BIGINT;
	table_function.named_parameters["max_line_size"] = LogicalType::VARCHAR;
	table_function.named_parameters["maximum_line_size"] = LogicalType::VARCHAR;
	table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	table_function.named_parameters["rejects_table"] = LogicalType::VARCHAR;
	table_function.named_parameters["rejects_limit"] = LogicalType::BIGINT;
	table_function.named_parameters["rejects_recovery_columns"] = LogicalType::LIST(LogicalType::VARCHAR);
	table_function.named_parameters["buffer_size"] = LogicalType::UBIGINT;
	table_function.named_parameters["decimal_separator"] = LogicalType::VARCHAR;
	table_function.named_parameters["parallel"] = LogicalType::BOOLEAN;
	table_function.named_parameters["null_padding"] = LogicalType::BOOLEAN;
	table_function.named_parameters["allow_quoted_nulls"] = LogicalType::BOOLEAN;
	table_function.named_parameters["column_types"] = LogicalType::ANY;
	table_function.named_parameters["dtypes"] = LogicalType::ANY;
	table_function.named_parameters["types"] = LogicalType::ANY;
	table_function.named_parameters["names"] = LogicalType::LIST(LogicalType::VARCHAR);
	table_function.named_parameters["column_names"] = LogicalType::LIST(LogicalType::VARCHAR);
	table_function.named_parameters["parallel"] = LogicalType::BOOLEAN;
	MultiFileReader::AddParameters(table_function);
}

double CSVReaderProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	auto &data = global_state->Cast<CSVGlobalState>();
	return data.GetProgress(bind_data);
}

void CSVComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                              vector<unique_ptr<Expression>> &filters) {
	auto &data = bind_data_p->Cast<ReadCSVData>();
	auto reset_reader =
	    MultiFileReader::ComplexFilterPushdown(context, data.files, data.options.file_options, get, filters);
	if (reset_reader) {
		MultiFileReader::PruneReaders(data);
	}
}

unique_ptr<NodeStatistics> CSVReaderCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	idx_t per_file_cardinality = 0;
	if (bind_data.buffer_manager && bind_data.buffer_manager->file_handle) {
		auto estimated_row_width = (bind_data.csv_types.size() * 5);
		per_file_cardinality = bind_data.buffer_manager->file_handle->FileSize() / estimated_row_width;
	} else {
		// determined through the scientific method as the average amount of rows in a CSV file
		per_file_cardinality = 42;
	}
	return make_uniq<NodeStatistics>(bind_data.files.size() * per_file_cardinality);
}

static void CSVReaderSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	serializer.WriteProperty(100, "extra_info", function.extra_info);
	serializer.WriteProperty(101, "csv_data", &bind_data);
}

static unique_ptr<FunctionData> CSVReaderDeserialize(Deserializer &deserializer, TableFunction &function) {
	unique_ptr<ReadCSVData> result;
	deserializer.ReadProperty(100, "extra_info", function.extra_info);
	deserializer.ReadProperty(101, "csv_data", result);
	return std::move(result);
}

TableFunction ReadCSVTableFunction::GetFunction() {
	TableFunction read_csv("read_csv", {LogicalType::VARCHAR}, ReadCSVFunction, ReadCSVBind, ReadCSVInitGlobal,
	                       ReadCSVInitLocal);
	read_csv.table_scan_progress = CSVReaderProgress;
	read_csv.pushdown_complex_filter = CSVComplexFilterPushdown;
	read_csv.serialize = CSVReaderSerialize;
	read_csv.deserialize = CSVReaderDeserialize;
	read_csv.get_batch_index = CSVReaderGetBatchIndex;
	read_csv.cardinality = CSVReaderCardinality;
	read_csv.projection_pushdown = true;
	ReadCSVAddNamedParameters(read_csv);
	return read_csv;
}

TableFunction ReadCSVTableFunction::GetAutoFunction() {
	auto read_csv_auto = ReadCSVTableFunction::GetFunction();
	read_csv_auto.name = "read_csv_auto";
	read_csv_auto.bind = ReadCSVBind;
	return read_csv_auto;
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(MultiFileReader::CreateFunctionSet(ReadCSVTableFunction::GetFunction()));
	set.AddFunction(MultiFileReader::CreateFunctionSet(ReadCSVTableFunction::GetAutoFunction()));
}

unique_ptr<TableRef> ReadCSVReplacement(ClientContext &context, const string &table_name, ReplacementScanData *data) {
	auto lower_name = StringUtil::Lower(table_name);
	// remove any compression
	if (StringUtil::EndsWith(lower_name, ".gz")) {
		lower_name = lower_name.substr(0, lower_name.size() - 3);
	} else if (StringUtil::EndsWith(lower_name, ".zst")) {
		if (!Catalog::TryAutoLoad(context, "parquet")) {
			throw MissingExtensionException("parquet extension is required for reading zst compressed file");
		}
		lower_name = lower_name.substr(0, lower_name.size() - 4);
	}
	if (!StringUtil::EndsWith(lower_name, ".csv") && !StringUtil::Contains(lower_name, ".csv?") &&
	    !StringUtil::EndsWith(lower_name, ".tsv") && !StringUtil::Contains(lower_name, ".tsv?")) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("read_csv_auto", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		auto &fs = FileSystem::GetFileSystem(context);
		table_function->alias = fs.ExtractBaseName(table_name);
	}

	return std::move(table_function);
}

void BuiltinFunctions::RegisterReadFunctions() {
	CSVCopyFunction::RegisterFunction(*this);
	ReadCSVTableFunction::RegisterFunction(*this);
	auto &config = DBConfig::GetConfig(*transaction.db);
	config.replacement_scans.emplace_back(ReadCSVReplacement);
}

} // namespace duckdb
