#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "json_transform.hpp"

namespace duckdb {

static BoundStatement CopyToJSONPlan(Binder &binder, CopyStatement &stmt) {
	auto stmt_copy = stmt.Copy();
	auto &copy = (CopyStatement &)*stmt_copy;
	auto &info = *copy.info;

	// Bind the select statement of the original to resolve the types
	auto dummy_binder = Binder::CreateBinder(binder.context, &binder, true);
	auto bound_original = dummy_binder->Bind(*stmt.select_statement);

	// Create new SelectNode with the original SelectNode as a subquery in the FROM clause
	auto select_stmt = make_unique<SelectStatement>();
	select_stmt->node = std::move(copy.select_statement);
	auto subquery_ref = make_unique<SubqueryRef>(std::move(select_stmt));
	copy.select_statement = make_unique_base<QueryNode, SelectNode>();
	auto &new_select_node = (SelectNode &)*copy.select_statement;
	new_select_node.from_table = std::move(subquery_ref);

	// Create new select list
	vector<unique_ptr<ParsedExpression>> select_list;
	select_list.reserve(bound_original.types.size());

	// strftime if the user specified a format (loop also gives columns a name, needed for struct_pack)
	// TODO: deal with date/timestamp within nested types
	const auto date_it = info.options.find("dateformat");
	const auto timestamp_it = info.options.find("timestampformat");
	vector<unique_ptr<ParsedExpression>> strftime_children;
	for (idx_t col_idx = 0; col_idx < bound_original.types.size(); col_idx++) {
		auto column = make_unique_base<ParsedExpression, PositionalReferenceExpression>(col_idx + 1);
		strftime_children.clear();
		const auto &type = bound_original.types[col_idx];
		const auto &name = bound_original.names[col_idx];
		if (date_it != info.options.end() && type == LogicalTypeId::DATE) {
			strftime_children.emplace_back(std::move(column));
			strftime_children.emplace_back(make_unique<ConstantExpression>(date_it->second.back()));
			column = make_unique<FunctionExpression>("strftime", std::move(strftime_children));
		} else if (timestamp_it != info.options.end() && type == LogicalTypeId::TIMESTAMP) {
			strftime_children.emplace_back(std::move(column));
			strftime_children.emplace_back(make_unique<ConstantExpression>(timestamp_it->second.back()));
			column = make_unique<FunctionExpression>("strftime", std::move(strftime_children));
		}
		column->alias = name;
		select_list.emplace_back(std::move(column));
	}

	// Now create the struct_pack/to_json to create a JSON object per row
	auto &select_node = (SelectNode &)*copy.select_statement;
	vector<unique_ptr<ParsedExpression>> struct_pack_child;
	struct_pack_child.emplace_back(make_unique<FunctionExpression>("struct_pack", std::move(select_list)));
	select_node.select_list.emplace_back(make_unique<FunctionExpression>("to_json", std::move(struct_pack_child)));

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
	    false,
	    make_shared<JSONScanInfo>(JSONScanType::READ_JSON, JSONFormat::AUTO_DETECT, JSONRecordType::RECORDS, false));

	return CreateCopyFunctionInfo(function);
}

} // namespace duckdb
