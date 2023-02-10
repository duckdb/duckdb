#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
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
	auto &info = *copy.info;

	// strftime if the user specified a format TODO: deal with date/timestamp within nested types
	auto date_it = info.options.find("dateformat");
	auto timestamp_it = info.options.find("timestampformat");

	// Bind the select statement of the original to resolve the types
	auto dummy_binder = Binder::CreateBinder(binder.context, &binder, true);
	auto bound_original = dummy_binder->Bind(*stmt.select_statement);
	D_ASSERT(bound_original.types.size() == select_stmt.select_list.size());
	const idx_t num_cols = bound_original.types.size();

	// This loop also makes sure the columns have an alias (needed for struct_pack)
	vector<unique_ptr<ParsedExpression>> strftime_children;
	for (idx_t i = 0; i < num_cols; i++) {
		strftime_children.clear();
		auto &col = select_stmt.select_list[i];
		auto name = col->GetName();
		if (bound_original.types[i] == LogicalTypeId::DATE && date_it != info.options.end()) {
			strftime_children.emplace_back(std::move(col));
			strftime_children.emplace_back(make_unique<ConstantExpression>(date_it->second.back()));
			col = make_unique<FunctionExpression>("strftime", std::move(strftime_children));
		} else if (bound_original.types[i] == LogicalTypeId::TIMESTAMP && timestamp_it != info.options.end()) {
			strftime_children.emplace_back(std::move(col));
			strftime_children.emplace_back(make_unique<ConstantExpression>(timestamp_it->second.back()));
			col = make_unique<FunctionExpression>("strftime", std::move(strftime_children));
		}
		col->alias = name;
	}

	// Now create the struct_pack/to_json to create a JSON object per row
	vector<unique_ptr<ParsedExpression>> struct_pack_child;
	struct_pack_child.emplace_back(make_unique<FunctionExpression>("struct_pack", std::move(select_stmt.select_list)));
	select_stmt.select_list.clear();
	select_stmt.select_list.emplace_back(make_unique<FunctionExpression>("to_json", std::move(struct_pack_child)));

	// Now we can just use the CSV writer
	info.format = "csv";
	info.options["quote"] = {""};
	info.options["escape"] = {""};
	info.options["delimiter"] = {"\n"};
	info.options["header"] = {0};

	return binder.Bind(*stmt_copy);
}

static unique_ptr<FunctionData> CopyFromJSONBind(ClientContext &context, CopyInfo &info, vector<string> &expected_names,
                                                 vector<LogicalType> &expected_types) {
	auto bind_data = make_unique<JSONScanData>();

	bind_data->file_paths.emplace_back(info.file_path);
	bind_data->names = expected_names;
	for (idx_t col_idx = 0; col_idx < expected_names.size(); col_idx++) {
		bind_data->valid_cols.emplace_back(col_idx);
	}

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
