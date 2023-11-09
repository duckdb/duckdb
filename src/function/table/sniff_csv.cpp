#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table/range.hpp"
#include "duckdb/execution/operator/scan/csv/base_csv_reader.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"

namespace duckdb {

struct CSVSniffFunctionData : public TableFunctionData {
	CSVSniffFunctionData() {
	}
	string path;
	int64_t sample_size = 20480;
	// The CSV reader options
	CSVReaderOptions options;
	// Return Types of CSV (If given by the user)
	vector<LogicalType> return_types_csv;
	// Column Names of CSV (If given by the user)
	vector<string> names_csv;
};

struct CSVSniffGlobalState : public GlobalTableFunctionState {
	CSVSniffGlobalState() {
	}
	bool done = false;
};

static unique_ptr<GlobalTableFunctionState> CSVSniffInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CSVSniffGlobalState>();
}

static unique_ptr<FunctionData> CSVSniffBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CSVSniffFunctionData>();
	result->path = input.inputs[0].ToString();
	result->options.FromNamedParameters(input.named_parameters, context, result->return_types_csv, result->names_csv);
	// We want to return the whole CSV Configuration
	// 1. Delimiter
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Delimiter");
	// 2. Quote
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Quote");
	// 3. Escape
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Escape");
	// 4. NewLine Delimiter
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("NewLine Delimiter");
	// 5. Skip Rows
	return_types.emplace_back(LogicalType::UINTEGER);
	names.emplace_back("Skip Rows");
	// 6. Has Header
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Has Header");
	// 7. List<Struct<Column-Name:Types>>
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Columns");
	// 8. Date Format
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Date Format");
	// 9. Timestamp Format
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Timestamp Format");
	// 10. CSV read function with all the options used
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("User Arguments");
	// 11. CSV read function with all the options used
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Prompt");
	return std::move(result);
}

string NewLineIdentifierToString(NewLineIdentifier identifier) {
	switch (identifier) {
	case NewLineIdentifier::SINGLE:
		return "\\n";
	case NewLineIdentifier::CARRY_ON:
		return "\\r\\n";
	default:
		throw InternalException("Invalid Newline Detected.");
	}
}

string FormatOptions(char opt) {
	if (opt == '\'') {
		return "''";
	}
	string result;
	result += opt;
	return result;
}

static void CSVSniffFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<CSVSniffGlobalState>();
	// Are we done?
	if (global_state.done) {
		return;
	}
	const CSVSniffFunctionData &data = data_p.bind_data->Cast<CSVSniffFunctionData>();
	// We must run the sniffer.
	CSVStateMachineCache state_machine_cache;
	CSVReaderOptions options;
	options.file_path = data.path;
	options.sample_size_chunks = data.sample_size / STANDARD_VECTOR_SIZE + 1;
	auto file_handle = BaseCSVReader::OpenCSV(context, options);
	auto buffer_manager = make_shared<CSVBufferManager>(context, std::move(file_handle), options);
	CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
	auto sniffer_result = sniffer.SniffCSV();
	string str_opt;
	string separator = ", ";
	// Set output
	output.SetCardinality(1);

	// 1. Delimiter
	str_opt = options.dialect_options.state_machine_options.delimiter;
	output.SetValue(0, 0, str_opt);
	// 2. Quote
	str_opt = options.dialect_options.state_machine_options.quote;
	output.SetValue(1, 0, str_opt);
	// 3. Escape
	str_opt = options.dialect_options.state_machine_options.escape;
	output.SetValue(2, 0, str_opt);
	// 4. NewLine Delimiter
	auto new_line_identifier = NewLineIdentifierToString(options.dialect_options.new_line);
	output.SetValue(3, 0, new_line_identifier);
	// 5. Skip Rows
	output.SetValue(4, 0, Value::UINTEGER(options.dialect_options.skip_rows));
	// 6. Has Header
	output.SetValue(5, 0, Value::BOOLEAN(options.dialect_options.header));
	// 7. List<Struct<Column-Name:Types>> {'col1': 'INTEGER', 'col2': 'VARCHAR'}
	std::ostringstream columns;
	columns << "{";
	for (idx_t i = 0; i < sniffer_result.return_types.size(); i++) {
		columns << "'" << sniffer_result.names[i] << "': '" << sniffer_result.return_types[i].ToString() << "'";
		if (i != sniffer_result.return_types.size() - 1) {
			columns << separator;
		}
	}
	columns << "}";
	output.SetValue(6, 0, columns.str());
	// 8. Date Format
	if (options.dialect_options.has_format[LogicalType::DATE] &&
	    options.dialect_options.date_format.find(LogicalType::DATE) != options.dialect_options.date_format.end()) {
		output.SetValue(7, 0, options.dialect_options.date_format[LogicalType::DATE].format_specifier);
	} else {
		output.SetValue(7, 0, Value());
	}
	// 9. Timestamp Format
	if (options.dialect_options.has_format[LogicalType::TIMESTAMP] &&
	    options.dialect_options.date_format.find(LogicalType::TIMESTAMP) != options.dialect_options.date_format.end()) {
		output.SetValue(8, 0, options.dialect_options.date_format[LogicalType::TIMESTAMP].format_specifier);
	} else {
		output.SetValue(8, 0, Value());
	}

	// 10. The Extra User Arguments
	if (data.options.user_defined_parameters.empty()) {
		output.SetValue(9, 0, Value());
	} else {
		output.SetValue(9, 0, Value(data.options.user_defined_parameters));
	}

	// 11. csv_read string
	std::ostringstream csv_read;

	// Base, Path and auto_detect=false
	csv_read << "FROM read_csv('" << data.path << "'" << separator << "auto_detect=false" << separator;
	// 10.1. Delimiter
	csv_read << "delim="
	         << "'" << FormatOptions(options.dialect_options.state_machine_options.delimiter) << "'" << separator;
	// 11.2. Quote
	csv_read << "quote="
	         << "'" << FormatOptions(options.dialect_options.state_machine_options.quote) << "'" << separator;
	// 11.3. Escape
	csv_read << "escape="
	         << "'" << FormatOptions(options.dialect_options.state_machine_options.escape) << "'" << separator;
	// 11.4. NewLine Delimiter
	if (new_line_identifier != "mix") {
		csv_read << "new_line="
		         << "'" << new_line_identifier << "'" << separator;
	}
	// 11.5. Skip Rows
	csv_read << "skip=" << options.dialect_options.skip_rows << separator;
	// 11.6. Has Header
	csv_read << "header=" << options.dialect_options.header << separator;
	// 11.7. column={'col1': 'INTEGER', 'col2': 'VARCHAR'}
	csv_read << "columns=" << columns.str();
	// 11.8. Date Format
	if (options.dialect_options.has_format[LogicalType::DATE] &&
	    options.dialect_options.date_format.find(LogicalType::DATE) != options.dialect_options.date_format.end()) {
		if (!options.dialect_options.date_format[LogicalType::DATE].format_specifier.empty()) {
			csv_read << separator << "dateformat="
			         << "'" << options.dialect_options.date_format[LogicalType::DATE].format_specifier << "'";
		}
	}
	// 11.9. Timestamp Format
	if (options.dialect_options.has_format[LogicalType::TIMESTAMP] &&
	    options.dialect_options.date_format.find(LogicalType::TIMESTAMP) != options.dialect_options.date_format.end()) {
		if (!options.dialect_options.date_format[LogicalType::TIMESTAMP].format_specifier.empty()) {
			csv_read << separator << "timestampformat="
			         << "'" << options.dialect_options.date_format[LogicalType::TIMESTAMP].format_specifier << "'";
		}
	}
	csv_read << ");";
	output.SetValue(10, 0, csv_read.str());
	global_state.done = true;
}

void CSVSnifferFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction csv_sniffer("sniff_csv", {LogicalType::VARCHAR}, CSVSniffFunction, CSVSniffBind, CSVSniffInitGlobal);
	// Accept same options as the actual csv reader
	ReadCSVTableFunction::ReadCSVAddNamedParameters(csv_sniffer);
	set.AddFunction(csv_sniffer);
}
} // namespace duckdb
