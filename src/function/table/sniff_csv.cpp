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
	return "";
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
	auto sniffer_options = data.options;
	sniffer_options.file_path = data.path;

	CSVStateMachineCache state_machine_cache;
	auto file_handle = BaseCSVReader::OpenCSV(context, sniffer_options);
	auto buffer_manager = make_shared<CSVBufferManager>(context, std::move(file_handle), sniffer_options);
	CSVSniffer sniffer(sniffer_options, buffer_manager, state_machine_cache);
	auto sniffer_result = sniffer.SniffCSV();
	string str_opt;
	string separator = ", ";
	// Set output
	output.SetCardinality(1);

	// 1. Delimiter
	str_opt = sniffer_options.dialect_options.state_machine_options.delimiter.GetValue();
	output.SetValue(0, 0, str_opt);
	// 2. Quote
	str_opt = sniffer_options.dialect_options.state_machine_options.quote.GetValue();
	output.SetValue(1, 0, str_opt);
	// 3. Escape
	str_opt = sniffer_options.dialect_options.state_machine_options.escape.GetValue();
	output.SetValue(2, 0, str_opt);
	// 4. NewLine Delimiter
	auto new_line_identifier = NewLineIdentifierToString(sniffer_options.dialect_options.new_line.GetValue());
	output.SetValue(3, 0, new_line_identifier);
	// 5. Skip Rows
	output.SetValue(4, 0, Value::UINTEGER(sniffer_options.dialect_options.skip_rows.GetValue()));
	// 6. Has Header
	output.SetValue(5, 0, Value::BOOLEAN(sniffer_options.dialect_options.header.GetValue()));
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
	output.SetValue(7, 0, sniffer_options.dialect_options.date_format[LogicalType::DATE].GetValue().format_specifier);

	// 9. Timestamp Format
	output.SetValue(8, 0,
	                sniffer_options.dialect_options.date_format[LogicalType::TIMESTAMP].GetValue().format_specifier);

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
	if (!sniffer_options.dialect_options.state_machine_options.delimiter.IsSetByUser()) {
		csv_read << "delim="
		         << "'" << FormatOptions(sniffer_options.dialect_options.state_machine_options.delimiter.GetValue())
		         << "'" << separator;
	}
	// 11.2. Quote
	if (!sniffer_options.dialect_options.header.IsSetByUser()) {
		csv_read << "quote="
		         << "'" << FormatOptions(sniffer_options.dialect_options.state_machine_options.quote.GetValue()) << "'"
		         << separator;
	}
	// 11.3. Escape
	if (!sniffer_options.dialect_options.state_machine_options.escape.IsSetByUser()) {
		csv_read << "escape="
		         << "'" << FormatOptions(sniffer_options.dialect_options.state_machine_options.escape.GetValue()) << "'"
		         << separator;
	}
	// 11.4. NewLine Delimiter
	if (!sniffer_options.dialect_options.new_line.IsSetByUser()) {
		if (new_line_identifier != "mix") {
			csv_read << "new_line="
			         << "'" << new_line_identifier << "'" << separator;
		}
	}
	// 11.5. Skip Rows
	if (!sniffer_options.dialect_options.skip_rows.IsSetByUser()) {
		csv_read << "skip=" << sniffer_options.dialect_options.skip_rows.GetValue() << separator;
	}
	// 11.6. Has Header
	if (!sniffer_options.dialect_options.header.IsSetByUser()) {
		csv_read << "header=" << sniffer_options.dialect_options.header.GetValue() << separator;
	}
	// 11.7. column={'col1': 'INTEGER', 'col2': 'VARCHAR'}
	csv_read << "columns=" << columns.str();
	// 11.8. Date Format
	if (!sniffer_options.dialect_options.date_format[LogicalType::DATE].IsSetByUser()) {
		if (!sniffer_options.dialect_options.date_format[LogicalType::DATE].GetValue().format_specifier.empty()) {
			csv_read << separator << "dateformat="
			         << "'"
			         << sniffer_options.dialect_options.date_format[LogicalType::DATE].GetValue().format_specifier
			         << "'";
		}
	}
	// 11.9. Timestamp Format
	if (!sniffer_options.dialect_options.date_format[LogicalType::TIMESTAMP].IsSetByUser()) {
		if (!sniffer_options.dialect_options.date_format[LogicalType::TIMESTAMP].GetValue().format_specifier.empty()) {
			csv_read << separator << "timestampformat="
			         << "'"
			         << sniffer_options.dialect_options.date_format[LogicalType::TIMESTAMP].GetValue().format_specifier
			         << "'";
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
