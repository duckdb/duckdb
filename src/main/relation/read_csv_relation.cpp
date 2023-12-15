#include "duckdb/main/relation/read_csv_relation.hpp"

#include "duckdb/execution/operator/scan/csv/buffered_csv_reader.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

void ReadCSVRelation::InitializeAlias(const vector<string> &input) {
	D_ASSERT(!input.empty());
	const auto &csv_file = input[0];
	alias = StringUtil::Split(csv_file, ".")[0];
}

static Value CreateValueFromFileList(const vector<string> &file_list) {
	vector<Value> files;
	for (auto &file : file_list) {
		files.push_back(file);
	}
	return Value::LIST(std::move(files));
}

ReadCSVRelation::ReadCSVRelation(const std::shared_ptr<ClientContext> &context, const vector<string> &input,
                                 named_parameter_map_t &&options, string alias_p)
    : TableFunctionRelation(context, "read_csv_auto", {CreateValueFromFileList(input)}, nullptr, false),
      alias(std::move(alias_p)) {

	InitializeAlias(input);

	auto file_list = CreateValueFromFileList(input);
	auto files = MultiFileReader::GetFileList(*context, file_list, "CSV");
	D_ASSERT(!files.empty());

	auto &file_name = files[0];
	CSVReaderOptions csv_options;
	csv_options.file_path = file_name;
	vector<string> empty;

	vector<LogicalType> unused_types;
	vector<string> unused_names;
	csv_options.FromNamedParameters(options, *context, unused_types, unused_names);

	// Run the auto-detect, populating the options with the detected settings
	auto bm_file_handle = BaseCSVReader::OpenCSV(*context, csv_options);
	auto buffer_manager = make_shared<CSVBufferManager>(*context, std::move(bm_file_handle), csv_options);
	CSVStateMachineCache state_machine_cache;
	CSVSniffer sniffer(csv_options, buffer_manager, state_machine_cache);
	auto sniffer_result = sniffer.SniffCSV();
	auto &types = sniffer_result.return_types;
	auto &names = sniffer_result.names;
	for (idx_t i = 0; i < types.size(); i++) {
		columns.emplace_back(names[i], types[i]);
	}

	// After sniffing we can consider these set, so they are exported as named parameters
	// FIXME: This is horribly hacky, should be refactored at some point
	csv_options.dialect_options.state_machine_options.escape.ChangeSetByUserTrue();
	csv_options.dialect_options.state_machine_options.delimiter.ChangeSetByUserTrue();
	csv_options.dialect_options.state_machine_options.quote.ChangeSetByUserTrue();
	csv_options.dialect_options.header.ChangeSetByUserTrue();
	csv_options.dialect_options.skip_rows.ChangeSetByUserTrue();

	// Capture the options potentially set/altered by the auto detection phase
	csv_options.ToNamedParameters(options);

	// No need to auto-detect again
	options["auto_detect"] = Value::BOOLEAN(false);
	SetNamedParameters(std::move(options));

	child_list_t<Value> column_names;
	for (idx_t i = 0; i < columns.size(); i++) {
		column_names.push_back(make_pair(columns[i].Name(), Value(columns[i].Type().ToString())));
	}

	AddNamedParameter("columns", Value::STRUCT(std::move(column_names)));
}

string ReadCSVRelation::GetAlias() {
	return alias;
}

} // namespace duckdb
