#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/helper.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "json_transform.hpp"
#include "json_multi_file_info.hpp"

namespace duckdb {

static void ThrowJSONCopyParameterException(const string &loption) {
	throw BinderException("COPY (FORMAT JSON) parameter %s expects a single argument.", loption);
}

static BoundStatement CopyToJSONPlan(Binder &binder, CopyStatement &stmt) {
	auto stmt_copy = stmt.Copy();
	auto &copy = stmt_copy->Cast<CopyStatement>();
	auto &copied_info = *copy.info;

	// Parse the options, creating options for the CSV writer while doing so
	string date_format;
	string timestamp_format;
	// We insert the JSON file extension here so it works properly with PER_THREAD_OUTPUT/FILE_SIZE_BYTES etc.
	case_insensitive_map_t<vector<Value>> csv_copy_options {{"file_extension", {"json"}}};
	for (const auto &kv : copied_info.options) {
		const auto &loption = StringUtil::Lower(kv.first);
		if (loption == "dateformat" || loption == "date_format") {
			if (kv.second.size() != 1) {
				ThrowJSONCopyParameterException(loption);
			}
			date_format = StringValue::Get(kv.second.back());
		} else if (loption == "timestampformat" || loption == "timestamp_format") {
			if (kv.second.size() != 1) {
				ThrowJSONCopyParameterException(loption);
			}
			timestamp_format = StringValue::Get(kv.second.back());
		} else if (loption == "array") {
			if (kv.second.size() > 1) {
				ThrowJSONCopyParameterException(loption);
			}
			if (kv.second.empty() || BooleanValue::Get(kv.second.back().DefaultCastAs(LogicalTypeId::BOOLEAN))) {
				csv_copy_options["prefix"] = {"[\n\t"};
				csv_copy_options["suffix"] = {"\n]\n"};
				csv_copy_options["new_line"] = {",\n\t"};
			}
		} else if (loption == "compression" || loption == "encoding" || loption == "per_thread_output" ||
		           loption == "file_size_bytes" || loption == "use_tmp_file" || loption == "overwrite_or_ignore" ||
		           loption == "filename_pattern" || loption == "file_extension") {
			// We support these base options
			csv_copy_options.insert(kv);
		} else {
			throw BinderException("Unknown option for COPY ... TO ... (FORMAT JSON): \"%s\".", loption);
		}
	}

	// Bind the select statement of the original to resolve the types
	auto dummy_binder = Binder::CreateBinder(binder.context, &binder);
	auto bound_original = dummy_binder->Bind(*stmt.info->select_statement);

	// Create new SelectNode with the original SelectNode as a subquery in the FROM clause
	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(copied_info.select_statement);
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(select_stmt));

	copied_info.select_statement = make_uniq_base<QueryNode, SelectNode>();
	auto &select_node = copied_info.select_statement->Cast<SelectNode>();
	select_node.from_table = std::move(subquery_ref);

	// Create new select list
	vector<unique_ptr<ParsedExpression>> select_list;
	select_list.reserve(bound_original.types.size());

	// strftime if the user specified a format (loop also gives columns a name, needed for struct_pack)
	// TODO: deal with date/timestamp within nested types
	vector<unique_ptr<ParsedExpression>> strftime_children;
	for (idx_t col_idx = 0; col_idx < bound_original.types.size(); col_idx++) {
		auto column = make_uniq_base<ParsedExpression, PositionalReferenceExpression>(col_idx + 1);
		strftime_children = vector<unique_ptr<ParsedExpression>>();
		const auto &type = bound_original.types[col_idx];
		const auto &name = bound_original.names[col_idx];
		if (!date_format.empty() && type == LogicalTypeId::DATE) {
			strftime_children.emplace_back(std::move(column));
			strftime_children.emplace_back(make_uniq<ConstantExpression>(date_format));
			column = make_uniq<FunctionExpression>("strftime", std::move(strftime_children));
		} else if (!timestamp_format.empty() && type == LogicalTypeId::TIMESTAMP) {
			strftime_children.emplace_back(std::move(column));
			strftime_children.emplace_back(make_uniq<ConstantExpression>(timestamp_format));
			column = make_uniq<FunctionExpression>("strftime", std::move(strftime_children));
		}
		column->SetAlias(name);
		select_list.emplace_back(std::move(column));
	}

	// Now create the struct_pack/to_json to create a JSON object per row
	vector<unique_ptr<ParsedExpression>> struct_pack_child;
	struct_pack_child.emplace_back(make_uniq<FunctionExpression>("struct_pack", std::move(select_list)));
	select_node.select_list.emplace_back(make_uniq<FunctionExpression>("to_json", std::move(struct_pack_child)));

	// Now we can just use the CSV writer
	copied_info.format = "csv";
	copied_info.options = std::move(csv_copy_options);
	copied_info.options["quote"] = {""};
	copied_info.options["escape"] = {""};
	copied_info.options["delimiter"] = {"\n"};
	copied_info.options["header"] = {{0}};

	return binder.Bind(*stmt_copy);
}

CopyFunction JSONFunctions::GetJSONCopyFunction() {
	CopyFunction function("json");
	function.extension = "json";

	function.plan = CopyToJSONPlan;

	function.copy_from_bind = MultiFileReaderFunction<JSONMultiFileInfo>::MultiFileBindCopy;
	function.copy_from_function = JSONFunctions::GetReadJSONTableFunction(make_shared_ptr<JSONScanInfo>(
	    JSONScanType::READ_JSON, JSONFormat::NEWLINE_DELIMITED, JSONRecordType::RECORDS, false));

	return function;
}

} // namespace duckdb
