#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "json_transform.hpp"

namespace duckdb {

static BoundStatement CopyToJSONPlan(Binder &binder, CopyStatement &stmt) {
	auto stmt_copy = stmt.Copy();
	auto &copy = (CopyStatement &)*stmt_copy;

	auto &select_stmt = (SelectNode &)*copy.select_statement;
	vector<unique_ptr<ParsedExpression>> struct_pack_child;
	// TODO: strptime for date format! (need to get this to work for inserts too)
	struct_pack_child.emplace_back(make_unique<FunctionExpression>("struct_pack", std::move(select_stmt.select_list)));
	select_stmt.select_list.clear();
	select_stmt.select_list.emplace_back(make_unique<FunctionExpression>("to_json", std::move(struct_pack_child)));

	auto &info = *copy.info;
	info.format = "csv";
	info.options["quote"] = {""};
	info.options["escape"] = {""};
	info.options["delimiter"] = {"\n"};

	return binder.Bind(*stmt_copy);
}

static unique_ptr<FunctionData> CopyFromJSONBind(ClientContext &context, CopyInfo &info, vector<string> &expected_names,
                                                 vector<LogicalType> &expected_types) {
	auto bind_data = make_unique<JSONScanData>();
	// TODO: check info.options?? - what options can we do, really?

	bind_data->file_paths.emplace_back(info.file_path);
	bind_data->names = expected_names;

	auto it = info.options.find("dateformat");
	if (it == info.options.end()) {
		it = info.options.find("date_format");
	}
	if (it != info.options.end()) {
		bind_data->date_format = StringValue::Get(it->second.back());
	}

	it = info.options.find("timestampformat");
	if (it == info.options.end()) {
		it = info.options.find("timestamp_format");
	}
	if (it != info.options.end()) {
		bind_data->timestamp_format = StringValue::Get(it->second.back());
	}

	bind_data->InitializeFormats();

	bind_data->transform_options = JSONTransformOptions(true, true, true, true);
	bind_data->transform_options.from_file = true;

	return std::move(bind_data);
}

CreateCopyFunctionInfo JSONFunctions::GetJSONCopyFunction() {
	CopyFunction function("json");
	function.extension = "json";

	function.plan = CopyToJSONPlan;

	function.copy_from_bind = CopyFromJSONBind;
	function.copy_from_function = JSONFunctions::GetReadJSONTableFunction(
	    false, make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::AUTO_DETECT, false));

	return CreateCopyFunctionInfo(function);
}

} // namespace duckdb
