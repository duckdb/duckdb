#include "duckdb/function/table/read_csv.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/execution/operator/csv_scanner/global_csv_state.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_error.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
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
#include "duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/base_scanner.hpp"

#include "duckdb/execution/operator/csv_scanner/string_value_scanner.hpp"

#include <limits>
#include "duckdb/execution/operator/csv_scanner/csv_schema.hpp"
#include "duckdb/common/multi_file_reader_function.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_multi_file_info.hpp"

namespace duckdb {

SerializedCSVReaderOptions::SerializedCSVReaderOptions(CSVReaderOptions options_p,
                                                       MultiFileReaderOptions file_options_p)
    : options(std::move(options_p)), file_options(std::move(file_options_p)) {
}

SerializedCSVReaderOptions::SerializedCSVReaderOptions(CSVOption<char> single_byte_delimiter,
                                                       const CSVOption<string> &multi_byte_delimiter)
    : options(single_byte_delimiter, multi_byte_delimiter) {
}

unique_ptr<CSVFileHandle> ReadCSV::OpenCSV(const string &file_path, const CSVReaderOptions &options,
                                           ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto &allocator = BufferAllocator::Get(context);
	auto &db_config = DBConfig::GetConfig(context);
	return CSVFileHandle::OpenFile(db_config, fs, allocator, file_path, options);
}

ReadCSVData::ReadCSVData() {
}

void ReadCSVData::FinalizeRead(ClientContext &context) {
	BaseCSVData::Finalize();
}

void ReadCSVTableFunction::ReadCSVAddNamedParameters(TableFunction &table_function) {
	table_function.named_parameters["sep"] = LogicalType::VARCHAR;
	table_function.named_parameters["delim"] = LogicalType::VARCHAR;
	table_function.named_parameters["quote"] = LogicalType::VARCHAR;
	table_function.named_parameters["new_line"] = LogicalType::VARCHAR;
	table_function.named_parameters["escape"] = LogicalType::VARCHAR;
	table_function.named_parameters["nullstr"] = LogicalType::ANY;
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
	table_function.named_parameters["store_rejects"] = LogicalType::BOOLEAN;
	table_function.named_parameters["rejects_table"] = LogicalType::VARCHAR;
	table_function.named_parameters["rejects_scan"] = LogicalType::VARCHAR;
	table_function.named_parameters["rejects_limit"] = LogicalType::BIGINT;
	table_function.named_parameters["force_not_null"] = LogicalType::LIST(LogicalType::VARCHAR);
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
	table_function.named_parameters["comment"] = LogicalType::VARCHAR;
	table_function.named_parameters["encoding"] = LogicalType::VARCHAR;
	table_function.named_parameters["strict_mode"] = LogicalType::BOOLEAN;

	MultiFileReader::AddParameters(table_function);
}

static void CSVReaderSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	throw NotImplementedException("CSVReaderSerialize not implemented");
	// auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	// auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	// serializer.WriteProperty(100, "extra_info", function.extra_info);
	// // generate the serialized data
	// SerializedReadCSVData serialized_data;
	// serialized_data.return_types = serialized_data.csv_types = bind_data.types;
	// serialized_data.return_names = serialized_data.csv_names = bind_data.names;
	// serialized_data.files = bind_data.file_list->GetAllFiles();
	// serialized_data.filename_col_idx = csv_data.filename_col_idx;
	// serialized_data.options.options = csv_data.options;
	// serialized_data.options.file_options = bind_data.file_options;
	// serialized_data.reader_bind = bind_data.reader_bind;
	// serializer.WriteProperty(101, "csv_data", serialized_data);
}

static unique_ptr<FunctionData> CSVReaderDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("CSVReaderDeserialize not implemented");
	// auto &context = deserializer.Get<ClientContext &>();
	// SerializedReadCSVData serialized_data;
	// deserializer.ReadProperty(100, "extra_info", function.extra_info);
	// deserializer.ReadProperty(101, "csv_data", serialized_data);
	//
	// vector<Value> file_path;
	// for (auto &path : serialized_data.files) {
	// 	file_path.emplace_back(path);
	// }
	// auto multi_file_reader = MultiFileReader::Create(function);
	// auto file_list = multi_file_reader->CreateFileList(context, Value::LIST(LogicalType::VARCHAR, file_path),
	//                                                    FileGlobOptions::ALLOW_EMPTY);
	// auto csv_options = make_uniq<CSVFileReaderOptions>();
	// csv_options->options = std::move(serialized_data.options.options);
	//
	// auto bind_data = MultiFileReaderFunction<CSVMultiFileInfo>::MultiFileBindInternal(
	//     context, std::move(multi_file_reader), std::move(file_list), serialized_data.return_types,
	//     serialized_data.return_names, std::move(serialized_data.options.file_options), std::move(csv_options));
	// return bind_data;
}

TableFunction ReadCSVTableFunction::GetFunction() {
	MultiFileReaderFunction<CSVMultiFileInfo> read_csv("read_csv");
	read_csv.serialize = CSVReaderSerialize;
	read_csv.deserialize = CSVReaderDeserialize;
	read_csv.type_pushdown = MultiFileReaderFunction<CSVMultiFileInfo>::PushdownType;
	ReadCSVAddNamedParameters(read_csv);
	return static_cast<TableFunction>(read_csv);
}

TableFunction ReadCSVTableFunction::GetAutoFunction() {
	auto read_csv_auto = ReadCSVTableFunction::GetFunction();
	read_csv_auto.name = "read_csv_auto";
	return read_csv_auto;
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(MultiFileReader::CreateFunctionSet(ReadCSVTableFunction::GetFunction()));
	set.AddFunction(MultiFileReader::CreateFunctionSet(ReadCSVTableFunction::GetAutoFunction()));
}

unique_ptr<TableRef> ReadCSVReplacement(ClientContext &context, ReplacementScanInput &input,
                                        optional_ptr<ReplacementScanData> data) {
	auto table_name = ReplacementScan::GetFullPath(input);
	auto lower_name = StringUtil::Lower(table_name);
	// remove any compression
	if (StringUtil::EndsWith(lower_name, CompressionExtensionFromType(FileCompressionType::GZIP))) {
		lower_name = lower_name.substr(0, lower_name.size() - 3);
	} else if (StringUtil::EndsWith(lower_name, CompressionExtensionFromType(FileCompressionType::ZSTD))) {
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
