#include "duckdb/function/table/csv_sniffer_function.hpp"
namespace duckdb{

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
		} else{
			throw InvalidInputException ("Invalid sniff_csv named parameter: %s . Only sample_size is accepted",loption);
		}
	}
	if (input.named_parameters.size() > 1){
		throw InvalidInputException ("Invalid number of named parameters for sniff_csv function, only sample_size is accepted");
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
	auto internal_struct = LogicalType::STRUCT({{"Name",LogicalType::VARCHAR}, {"Type",LogicalType::VARCHAR}});
	return_types.emplace_back(internal_struct);
	names.emplace_back("Columns");
	// 8. CSV read function with all the options used
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Prompt");
	return std::move(result);
}

string NewLineIdentifierToString(NewLineIdentifier identifier){
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
	options.sample_size_chunks = data.sample_size/STANDARD_VECTOR_SIZE + 1;
	auto file_handle = BaseCSVReader::OpenCSV(context, options);
	auto buffer_manager = make_shared<CSVBufferManager>(context, std::move(file_handle), options);
	CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
	auto sniffer_result = sniffer.SniffCSV();

	// Set output
	output.SetCardinality(1);

	// 1. Delimiter
	output.SetValue(0,0,options.dialect_options.state_machine_options.delimiter);
	// 2. Quote
	output.SetValue(0,1,options.dialect_options.state_machine_options.quote);
	// 3. Escape
	output.SetValue(0,2,options.dialect_options.state_machine_options.escape);
	// 4. NewLine Delimiter
	output.SetValue(0,3,NewLineIdentifierToString(options.dialect_options.new_line));
	// 5. Skip Rows
	output.SetValue(0,4,NewLineIdentifierToString(options.dialect_options.new_line));
	// 6. Has Header
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Has Header");
	// 7. List<Struct<Column-Name:Types>>
	auto internal_struct = LogicalType::STRUCT({{"Name",LogicalType::VARCHAR}, {"Type",LogicalType::VARCHAR}});
	return_types.emplace_back(internal_struct);
	names.emplace_back("Columns");
	// 8. CSV read function with all the options used
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Prompt");



}



void CSVSnifferFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction csv_sniffer("sniff_csv", {LogicalType::VARCHAR},
	                    CSVSniffFunction, CSVSniffBind);
	set.AddFunction(csv_sniffer);
}
}
