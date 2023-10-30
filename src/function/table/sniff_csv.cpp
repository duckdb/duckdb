#include "duckdb/function/table/csv_sniffer_function.hpp"
namespace duckdb {

struct CSVFunctionData : public TableFunctionData {
	CSVFunctionData() {
	}
	string path;
	uint64_t sample_size = 20480;
};

static unique_ptr<FunctionData> CSVSniffBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CSVFunctionData>();
	result->path = input.inputs[0].ToString();
	if (input.named_parameters.size() == 1) {
		auto loption = StringUtil::Lower(input.named_parameters.begin()->first);
		if (loption == "sample_size") {
			result->sample_size = UBigIntValue::Get(input.named_parameters.begin()->second);
		} else {
			throw InvalidInputException("Invalid sniff_csv named parameter: %s . Only sample_size is accepted",
			                            loption);
		}
	}
	if (input.named_parameters.size() > 1) {
		throw InvalidInputException(
		    "Invalid number of named parameters for sniff_csv function, only sample_size is accepted");
	}
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
	auto internal_struct = LogicalType::STRUCT({{"Name", LogicalType::VARCHAR}, {"Type", LogicalType::VARCHAR}});
	return_types.emplace_back(internal_struct);
	names.emplace_back("Columns");
	// 8. Date Format
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Date Format");
	// 9. Timestamp Format
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Timestamp Format");
	// 10. CSV read function with all the options used
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Prompt");
	return std::move(result);
}

string NewLineIdentifierToString(NewLineIdentifier identifier) {
	switch (identifier) {
	case NewLineIdentifier::SINGLE:
		return "single";
	case NewLineIdentifier::CARRY_ON:
		return "carry_on";
	case NewLineIdentifier::MIX:
		return "mix";
	case NewLineIdentifier::NOT_SET:
		throw InternalException("NewLine Identifier must always be set after running the CSV sniffer");
	}
}

static void CSVSniffFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	const CSVFunctionData &data = data_p.bind_data->Cast<CSVFunctionData>();
	// We must run the sniffer.
	CSVStateMachineCache state_machine_cache;
	CSVReaderOptions options;
	options.file_path = data.path;
	options.sample_size_chunks = data.sample_size / STANDARD_VECTOR_SIZE + 1;
	auto file_handle = BaseCSVReader::OpenCSV(context, options);
	auto buffer_manager = make_shared<CSVBufferManager>(context, std::move(file_handle), options);
	CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
	auto sniffer_result = sniffer.SniffCSV();

	// Set output
	output.SetCardinality(1);

	// 1. Delimiter
	output.SetValue(0, 0, options.dialect_options.state_machine_options.delimiter);
	// 2. Quote
	output.SetValue(0, 1, options.dialect_options.state_machine_options.quote);
	// 3. Escape
	output.SetValue(0, 2, options.dialect_options.state_machine_options.escape);
	// 4. NewLine Delimiter
	auto new_line_identifier = NewLineIdentifierToString(options.dialect_options.new_line);
	output.SetValue(0, 3, new_line_identifier);
	// 5. Skip Rows
	output.SetValue(0, 4, Value::UINTEGER(options.dialect_options.skip_rows));
	// 6. Has Header
	output.SetValue(0, 5, Value::BOOLEAN(options.dialect_options.header));
	// 7. List<Struct<Column-Name:Types>>
	vector<Value> top_list;
	for (idx_t i = 0; i < sniffer_result.return_types.size(); i++) {
		top_list.emplace_back(
		    Value::STRUCT({{"Name", sniffer_result.names[i]}, {"Type", sniffer_result.return_types[i].ToString()}}));
	}
	output.SetValue(0, 6, Value::LIST(top_list));
	// 8. Date Format
	if (options.dialect_options.date_format.find(LogicalType::DATE) != options.dialect_options.date_format.end()) {
		output.SetValue(0, 7, options.dialect_options.date_format[LogicalType::DATE].format_string);
	} else {
		output.SetValue(0, 7, "");
	}
	// 9. Timestamp Format
	if (options.dialect_options.date_format.find(LogicalType::TIMESTAMP) != options.dialect_options.date_format.end()) {
		output.SetValue(0, 8, options.dialect_options.date_format[LogicalType::TIMESTAMP].format_string);
	} else {
		output.SetValue(0, 8, "");
	}
	// 10. csv_read string
	std::ostringstream csv_read;
	string separator = ", ";
	// Base, Path and auto_detect=false
	csv_read << "FROM read_csv('" << data.path << "'" << separator << "auto_detect=false";
	// 1. Delimiter
	csv_read << "delim="
	         << "'" << options.dialect_options.state_machine_options.delimiter << "'" << separator;
	// 2. Quote
	csv_read << "quote="
	         << "'" << options.dialect_options.state_machine_options.quote << "'" << separator;
	// 3. Escape
	csv_read << "escape="
	         << "'" << options.dialect_options.state_machine_options.escape << "'" << separator;
	// 4. NewLine Delimiter
	csv_read << "new_line="
	         << "'" << new_line_identifier << "'" << separator;
	// 5. Skip Rows
	csv_read << "skip=" << options.dialect_options.skip_rows << separator;
	// 6. Has Header
	csv_read << "header="
	         << "'" << options.dialect_options.header << "'" << separator;
	// 7. column={'col1': 'INTEGER', 'col2': 'VARCHAR'}
	std::ostringstream columns;
	columns << "columns={";
	for (idx_t i = 0; i < sniffer_result.return_types.size(); i++) {
		columns << "'" << sniffer_result.names[i] << "'" << separator << "'"
		        << sniffer_result.return_types[i].ToString() << "'}";
		if (i != sniffer_result.return_types.size() - 1) {
			columns << separator;
		}
	}
	csv_read << columns.str();
	// 8. Date Format
	if (options.dialect_options.date_format.find(LogicalType::DATE) != options.dialect_options.date_format.end()) {
		csv_read << separator << "dateformat="
		         << "'" << options.dialect_options.date_format[LogicalType::DATE].format_string << "'";
	}
	// 9. Timestamp Format
	if (options.dialect_options.date_format.find(LogicalType::TIMESTAMP) != options.dialect_options.date_format.end()) {
		csv_read << separator << "timestampformat="
		         << "'" << options.dialect_options.date_format[LogicalType::TIMESTAMP].format_string << "'";
	}
	csv_read << ");";
	output.SetValue(0, 9, csv_read.str());
}

void CSVSnifferFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction csv_sniffer("sniff_csv", {LogicalType::VARCHAR}, CSVSniffFunction, CSVSniffBind);
	set.AddFunction(csv_sniffer);
}
} // namespace duckdb
