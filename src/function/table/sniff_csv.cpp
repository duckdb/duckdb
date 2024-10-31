#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_buffer_manager.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table/range.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_handle.hpp"
#include "duckdb/function/table/read_csv.hpp"

namespace duckdb {

struct CSVSniffFunctionData : public TableFunctionData {
	CSVSniffFunctionData() {
	}
	string path;
	// The CSV reader options
	CSVReaderOptions options;
	// Return Types of CSV (If given by the user)
	vector<LogicalType> return_types_csv;
	// Column Names of CSV (If given by the user)
	vector<string> names_csv;
	// If we want to force the match of the sniffer types
	bool force_match = true;
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
	auto it = input.named_parameters.find("auto_detect");
	if (it != input.named_parameters.end()) {
		if (!it->second.GetValue<bool>()) {
			throw InvalidInputException("sniff_csv function does not accept auto_detect variable set to false");
		}
		// otherwise remove it
		input.named_parameters.erase("auto_detect");
	}

	// If we want to force the match of the sniffer
	it = input.named_parameters.find("force_match");
	if (it != input.named_parameters.end()) {
		result->force_match = it->second.GetValue<bool>();
		input.named_parameters.erase("force_match");
	}
	result->options.FromNamedParameters(input.named_parameters, context);
	result->options.Verify();

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
	names.emplace_back("NewLineDelimiter");
	// 5. Comment
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Comment");
	// 6. Skip Rows
	return_types.emplace_back(LogicalType::UINTEGER);
	names.emplace_back("SkipRows");
	// 7. Has Header
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("HasHeader");
	// 8. List<Struct<Column-Name:Types>>
	child_list_t<LogicalType> struct_children {{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}};
	auto list_child = LogicalType::STRUCT(struct_children);
	return_types.emplace_back(LogicalType::LIST(list_child));
	names.emplace_back("Columns");
	// 9. Date Format
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("DateFormat");
	// 10. Timestamp Format
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("TimestampFormat");
	// 11. CSV read function with all the options used
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("UserArguments");
	// 12. CSV read function with all the options used
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Prompt");
	return std::move(result);
}

string FormatOptions(char opt) {
	if (opt == '\'') {
		return "''";
	}
	if (opt == '\0') {
		return "";
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
	auto &fs = duckdb::FileSystem::GetFileSystem(context);

	auto paths = fs.GlobFiles(data.path, context, FileGlobOptions::DISALLOW_EMPTY);
	if (paths.size() > 1) {
		throw NotImplementedException("sniff_csv does not operate on more than one file yet");
	}

	// We must run the sniffer.
	auto sniffer_options = data.options;
	sniffer_options.file_path = paths[0];

	auto buffer_manager = make_shared_ptr<CSVBufferManager>(context, sniffer_options, sniffer_options.file_path, 0);
	if (sniffer_options.name_list.empty()) {
		sniffer_options.name_list = data.names_csv;
	}

	if (sniffer_options.sql_type_list.empty()) {
		sniffer_options.sql_type_list = data.return_types_csv;
	}
	CSVSniffer sniffer(sniffer_options, buffer_manager, CSVStateMachineCache::Get(context));
	auto sniffer_result = sniffer.SniffCSV(data.force_match);
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
	auto new_line_identifier = sniffer_options.NewLineIdentifierToString();
	output.SetValue(3, 0, new_line_identifier);
	// 5. Comment
	str_opt = sniffer_options.dialect_options.state_machine_options.comment.GetValue();
	output.SetValue(4, 0, str_opt);
	// 6. Skip Rows
	output.SetValue(5, 0, Value::UINTEGER(NumericCast<uint32_t>(sniffer_options.dialect_options.skip_rows.GetValue())));
	// 7. Has Header
	auto has_header = Value::BOOLEAN(sniffer_options.dialect_options.header.GetValue()).ToString();
	output.SetValue(6, 0, has_header);
	// 8. List<Struct<Column-Name:Types>> {'col1': 'INTEGER', 'col2': 'VARCHAR'}
	vector<Value> values;
	std::ostringstream columns;
	columns << "{";
	for (idx_t i = 0; i < sniffer_result.return_types.size(); i++) {
		child_list_t<Value> struct_children {{"name", sniffer_result.names[i]},
		                                     {"type", {sniffer_result.return_types[i].ToString()}}};
		values.emplace_back(Value::STRUCT(struct_children));
		columns << "'" << sniffer_result.names[i] << "': '" << sniffer_result.return_types[i].ToString() << "'";
		if (i != sniffer_result.return_types.size() - 1) {
			columns << separator;
		}
	}
	columns << "}";
	output.SetValue(7, 0, Value::LIST(values));
	// 9. Date Format
	auto date_format = sniffer_options.dialect_options.date_format[LogicalType::DATE].GetValue();
	if (!date_format.Empty()) {
		output.SetValue(8, 0, date_format.format_specifier);
	} else {
		bool has_date = false;
		for (auto &c_type : sniffer_result.return_types) {
			// Must be ISO 8601
			if (c_type.id() == LogicalTypeId::DATE) {
				output.SetValue(8, 0, Value("%Y-%m-%d"));
				has_date = true;
			}
		}
		if (!has_date) {
			output.SetValue(8, 0, Value(nullptr));
		}
	}

	// 10. Timestamp Format
	auto timestamp_format = sniffer_options.dialect_options.date_format[LogicalType::TIMESTAMP].GetValue();
	if (!timestamp_format.Empty()) {
		output.SetValue(9, 0, timestamp_format.format_specifier);
	} else {
		output.SetValue(9, 0, Value(nullptr));
	}

	// 11. The Extra User Arguments
	if (data.options.user_defined_parameters.empty()) {
		output.SetValue(10, 0, Value());
	} else {
		output.SetValue(10, 0, Value(data.options.user_defined_parameters));
	}

	// 12. csv_read string
	std::ostringstream csv_read;

	// Base, Path and auto_detect=false
	csv_read << "FROM read_csv('" << paths[0] << "'" << separator << "auto_detect=false" << separator;
	// 10.1. Delimiter
	if (!sniffer_options.dialect_options.state_machine_options.delimiter.IsSetByUser()) {
		csv_read << "delim="
		         << "'" << FormatOptions(sniffer_options.dialect_options.state_machine_options.delimiter.GetValue())
		         << "'" << separator;
	}
	// 11.2. Quote
	if (!sniffer_options.dialect_options.state_machine_options.quote.IsSetByUser()) {
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
	if (!sniffer_options.dialect_options.state_machine_options.new_line.IsSetByUser()) {
		if (new_line_identifier != "mix") {
			csv_read << "new_line="
			         << "'" << new_line_identifier << "'" << separator;
		}
	}
	// 11.5. Skip Rows
	if (!sniffer_options.dialect_options.skip_rows.IsSetByUser()) {
		csv_read << "skip=" << sniffer_options.dialect_options.skip_rows.GetValue() << separator;
	}

	// 11.6. Comment
	if (!sniffer_options.dialect_options.state_machine_options.comment.IsSetByUser()) {
		csv_read << "comment="
		         << "'" << FormatOptions(sniffer_options.dialect_options.state_machine_options.comment.GetValue())
		         << "'" << separator;
	}

	// 11.7. Has Header
	if (!sniffer_options.dialect_options.header.IsSetByUser()) {
		csv_read << "header=" << has_header << separator;
	}
	// 11.8. column={'col1': 'INTEGER', 'col2': 'VARCHAR'}
	csv_read << "columns=" << columns.str();
	// 11.9. Date Format
	if (!sniffer_options.dialect_options.date_format[LogicalType::DATE].IsSetByUser()) {
		if (!sniffer_options.dialect_options.date_format[LogicalType::DATE].GetValue().format_specifier.empty()) {
			csv_read << separator << "dateformat="
			         << "'"
			         << sniffer_options.dialect_options.date_format[LogicalType::DATE].GetValue().format_specifier
			         << "'";
		} else {
			for (auto &c_type : sniffer_result.return_types) {
				// Must be ISO 8601
				if (c_type.id() == LogicalTypeId::DATE) {
					csv_read << separator << "dateformat="
					         << "'%Y-%m-%d'";
					break;
				}
			}
		}
	}
	// 11.10. Timestamp Format
	if (!sniffer_options.dialect_options.date_format[LogicalType::TIMESTAMP].IsSetByUser()) {
		if (!sniffer_options.dialect_options.date_format[LogicalType::TIMESTAMP].GetValue().format_specifier.empty()) {
			csv_read << separator << "timestampformat="
			         << "'"
			         << sniffer_options.dialect_options.date_format[LogicalType::TIMESTAMP].GetValue().format_specifier
			         << "'";
		}
	}
	// 11.11 User Arguments
	if (!data.options.user_defined_parameters.empty()) {
		csv_read << separator << data.options.user_defined_parameters;
	}
	csv_read << ");";
	output.SetValue(11, 0, csv_read.str());
	global_state.done = true;
}

void CSVSnifferFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction csv_sniffer("sniff_csv", {LogicalType::VARCHAR}, CSVSniffFunction, CSVSniffBind, CSVSniffInitGlobal);
	// Accept same options as the actual csv reader
	ReadCSVTableFunction::ReadCSVAddNamedParameters(csv_sniffer);
	csv_sniffer.named_parameters["force_match"] = LogicalType::BOOLEAN;
	set.AddFunction(csv_sniffer);
}
} // namespace duckdb
