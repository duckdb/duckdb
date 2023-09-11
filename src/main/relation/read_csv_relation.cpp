#include "duckdb/main/relation/read_csv_relation.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/scan/csv/buffered_csv_reader.hpp"
#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

ReadCSVRelation::ReadCSVRelation(const shared_ptr<ClientContext> &context, const string &csv_file,
                                 vector<ColumnDefinition> columns_p, string alias_p)
    : TableFunctionRelation(context, "read_csv", {Value(csv_file)}, nullptr, false), alias(std::move(alias_p)),
      auto_detect(false) {

	if (alias.empty()) {
		alias = StringUtil::Split(csv_file, ".")[0];
	}

	columns = std::move(columns_p);

	child_list_t<Value> column_names;
	for (idx_t i = 0; i < columns.size(); i++) {
		column_names.push_back(make_pair(columns[i].Name(), Value(columns[i].Type().ToString())));
	}

	AddNamedParameter("columns", Value::STRUCT(std::move(column_names)));
}

ReadCSVRelation::ReadCSVRelation(const shared_ptr<ClientContext> &context, const string &csv_file,
                                 CSVReaderOptions options, string alias_p)
    : TableFunctionRelation(context, "read_csv_auto", {Value(csv_file)}, nullptr, false), alias(std::move(alias_p)),
      auto_detect(true) {

	if (alias.empty()) {
		alias = StringUtil::Split(csv_file, ".")[0];
	}

	// Force auto_detect for this constructor
	options.auto_detect = true;
	auto bm_file_handle = BaseCSVReader::OpenCSV(*context, options);
	auto buffer_manager = make_shared<CSVBufferManager>(*context, std::move(bm_file_handle), options);
	CSVStateMachineCache state_machine_cache;
	CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
	auto sniffer_result = sniffer.SniffCSV();
	auto &types = sniffer_result.return_types;
	auto &names = sniffer_result.names;
	for (idx_t i = 0; i < types.size(); i++) {
		columns.emplace_back(names[i], types[i]);
	}

	AddNamedParameter("auto_detect", Value::BOOLEAN(true));
}

string ReadCSVRelation::GetAlias() {
	return alias;
}

} // namespace duckdb
