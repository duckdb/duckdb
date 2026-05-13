#include "duckdb/function/copy_function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/helper.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"
#include "json_transform.hpp"
#include "json_multi_file_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

static void ThrowJSONCopyParameterException(const string &loption) {
	throw BinderException("COPY (FORMAT JSON) parameter %s expects a single argument.", loption);
}

static BoundStatement CopyToJSONPlan(Binder &binder, CopyStatement &stmt) {
	static const unordered_set<string> SUPPORTED_BASE_OPTIONS {
	    "compression", "encoding", "use_tmp_file", "overwrite_or_ignore", "overwrite", "append", "filename_pattern",
	    "file_extension", "per_thread_output", "file_size_bytes",
	    // "partition_by", unsupported
	    "return_files", "preserve_order", "return_stats", "write_partition_columns", "write_empty_file",
	    "hive_file_pattern"};

	auto &copy_info = *stmt.info;

	// Parse the options, creating options for the CSV writer while doing so
	string date_format;
	string timestamp_format;
	// We insert the JSON file extension here so it works properly with PER_THREAD_OUTPUT/FILE_SIZE_BYTES etc.
	case_insensitive_map_t<vector<Value>> csv_copy_options {{"file_extension", {"json"}}};
	for (const auto &kv : copy_info.options) {
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
		} else if (loption == "file_extension") {
			// Since we set the file extension to "json" above, we need to override it
			csv_copy_options["file_extension"] = {StringValue::Get(kv.second.back())};
		} else if (SUPPORTED_BASE_OPTIONS.find(loption) != SUPPORTED_BASE_OPTIONS.end()) {
			// We support these base options
			csv_copy_options.insert(kv);
		} else {
			throw BinderException("Unknown option for COPY ... TO ... (FORMAT JSON): \"%s\".", loption);
		}
	}

	// Run the following query to convert everything into a single JSON column, then invoke the CSV writer
	// SELECT TO_JSON(STRUCT_PACK(*COLUMNS(*))) FROM <source>

	auto inner_select_stmt = make_uniq<SelectStatement>();
	inner_select_stmt->node = std::move(copy_info.select_statement);

	unique_ptr<SubqueryRef> source_ref;
	if (!date_format.empty() || !timestamp_format.empty()) {
		// if we have date_format or timestamp_format defined, we use the json_copy macros to apply them
		// e.g. SELECT json_copy_strftime_if_date(COLUMNS(*), date_format) FROM <source>
		auto strftime_node = make_uniq<SelectNode>();
		strftime_node->from_table = make_uniq<SubqueryRef>(std::move(inner_select_stmt));

		unique_ptr<ParsedExpression> expr;
		auto columns_star = make_uniq<StarExpression>();
		columns_star->columns = true;
		expr = std::move(columns_star);
		if (!date_format.empty()) {
			// apply date format
			vector<unique_ptr<ParsedExpression>> args;
			args.push_back(std::move(expr));
			args.push_back(make_uniq<ConstantExpression>(Value(date_format)));
			auto strftime_date = make_uniq<FunctionExpression>("json_copy_strftime_if_date", std::move(args));
			expr = std::move(strftime_date);
		}
		if (!timestamp_format.empty()) {
			// apply timestamp format
			vector<unique_ptr<ParsedExpression>> args;
			args.push_back(std::move(expr));
			args.push_back(make_uniq<ConstantExpression>(Value(timestamp_format)));
			auto strftime_ts = make_uniq<FunctionExpression>("json_copy_strftime_if_timestamp", std::move(args));
			expr = std::move(strftime_ts);
		}
		strftime_node->select_list.push_back(std::move(expr));
		auto strftime_stmt = make_uniq<SelectStatement>();
		strftime_stmt->node = std::move(strftime_node);
		source_ref = make_uniq<SubqueryRef>(std::move(strftime_stmt));
	} else {
		source_ref = make_uniq<SubqueryRef>(std::move(inner_select_stmt));
	}

	// Build outer: SELECT TO_JSON(STRUCT_PACK(*COLUMNS(*))) FROM <source_ref>
	copy_info.select_statement = make_uniq_base<QueryNode, SelectNode>();
	auto &select_node = copy_info.select_statement->Cast<SelectNode>();
	select_node.from_table = std::move(source_ref);

	auto columns_star = make_uniq<StarExpression>();
	columns_star->columns = true;
	auto unpack = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK);
	unpack->children.push_back(std::move(columns_star));

	vector<unique_ptr<ParsedExpression>> struct_pack_args;
	struct_pack_args.push_back(std::move(unpack));
	auto struct_pack = make_uniq<FunctionExpression>("struct_pack", std::move(struct_pack_args));

	vector<unique_ptr<ParsedExpression>> to_json_args;
	to_json_args.push_back(std::move(struct_pack));
	select_node.select_list.push_back(make_uniq<FunctionExpression>("to_json", std::move(to_json_args)));

	// Now we can just use the CSV writer
	copy_info.format = "csv";
	copy_info.options = std::move(csv_copy_options);
	copy_info.options["quote"] = {""};
	copy_info.options["escape"] = {""};
	copy_info.options["delimiter"] = {"\n"};
	copy_info.options["header"] = {{0}};

	return binder.Bind(stmt);
}

CopyFunction JSONFunctions::GetJSONCopyFunction() {
	CopyFunction function("json");
	function.extension = "json";

	function.plan = CopyToJSONPlan;

	function.copy_from_bind = MultiFileFunction<JSONMultiFileInfo>::MultiFileBindCopy;
	function.copy_from_function = JSONFunctions::GetReadJSONTableFunction(make_shared_ptr<JSONScanInfo>(
	    JSONScanType::READ_JSON, JSONFormat::AUTO_DETECT, JSONRecordType::RECORDS, false));

	return function;
}

} // namespace duckdb
