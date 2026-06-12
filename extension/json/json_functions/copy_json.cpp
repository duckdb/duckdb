#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/bind_helpers.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
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

static void ThrowJSONCopyNullException(const string &loption) {
	throw BinderException("COPY (FORMAT JSON) parameter \"%s\" cannot be NULL.", loption);
}

static void ThrowJSONCopyTypeException(const string &loption, const Value &value, const string &expected_type) {
	throw BinderException("COPY (FORMAT JSON) parameter \"%s\" expects a %s argument, but got %s.", loption,
	                      expected_type, value.type());
}

static const Value &GetSingleJSONCopyValue(const string &loption, const vector<Value> &values) {
	if (values.size() != 1) {
		ThrowJSONCopyParameterException(loption);
	}
	if (values.back().IsNull()) {
		ThrowJSONCopyNullException(loption);
	}
	return values.back();
}

static string GetSingleJSONCopyString(const string &loption, const vector<Value> &values) {
	auto &value = GetSingleJSONCopyValue(loption, values);
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		ThrowJSONCopyTypeException(loption, value, "VARCHAR");
	}
	return StringValue::Get(value);
}

static bool GetJSONCopyBoolean(Binder &binder, const string &loption, const vector<Value> &values) {
	if (values.size() > 1) {
		throw InvalidInputException("Copy option \"%s\" did not expect a list as argument", loption);
	}
	if (values.empty()) {
		return true;
	}
	if (values.back().IsNull()) {
		ThrowJSONCopyNullException(loption);
	}
	return values.back().CastAs(binder.context, LogicalType::BOOLEAN).GetValue<bool>();
}

static unique_ptr<SubqueryRef> PushJSONFormatProjection(unique_ptr<SubqueryRef> source_ref,
                                                        const Identifier &function_name, const string &format) {
	auto format_node = make_uniq<SelectNode>();
	format_node->from_table = std::move(source_ref);

	auto columns_star = make_uniq<StarExpression>();
	columns_star->IsColumnsMutable() = true;
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(std::move(columns_star));
	args.push_back(make_uniq<ConstantExpression>(Value(format)));
	format_node->select_list.push_back(make_uniq<FunctionExpression>(function_name, std::move(args)));

	auto format_stmt = make_uniq<SelectStatement>();
	format_stmt->node = std::move(format_node);
	return make_uniq<SubqueryRef>(std::move(format_stmt));
}

static BoundStatement CopyToJSONPlan(Binder &binder, CopyStatement &stmt) {
	static const unordered_set<string> SUPPORTED_BASE_OPTIONS {
	    "compression",      "encoding",         "use_tmp_file",   "overwrite_or_ignore", "overwrite",
	    "append",           "filename_pattern", "file_extension", "per_thread_output",   "file_size_bytes",
	    "partition_by",     "return_files",     "preserve_order", "return_stats",        "write_partition_columns",
	    "write_empty_file", "hive_file_pattern"};

	auto &copy_info = *stmt.info;

	// Parse the options, creating options for the CSV writer while doing so
	string date_format;
	string timestamp_format;
	// Partition columns are kept as separate columns (instead of being packed into the JSON object), so that the
	// COPY writer can partition on them. By default they are excluded from the written JSON, matching the behavior
	// of the other formats. WRITE_PARTITION_COLUMNS keeps them inside the JSON object instead.
	vector<string> partition_columns;
	bool write_partition_columns = false;
	vector<Identifier> original_column_names;
	// We insert the JSON file extension here so it works properly with PER_THREAD_OUTPUT/FILE_SIZE_BYTES etc.
	case_insensitive_map_t<vector<Value>> csv_copy_options {{"file_extension", {"json"}}};
	for (const auto &kv : copy_info.options) {
		const auto &loption = StringUtil::Lower(kv.first);
		if (loption == "dateformat" || loption == "date_format") {
			date_format = GetSingleJSONCopyString(loption, kv.second);
		} else if (loption == "timestampformat" || loption == "timestamp_format") {
			timestamp_format = GetSingleJSONCopyString(loption, kv.second);
		} else if (loption == "array") {
			if (kv.second.size() > 1) {
				ThrowJSONCopyParameterException(loption);
			}
			if (!kv.second.empty() && kv.second.back().IsNull()) {
				ThrowJSONCopyNullException(loption);
			}
			if (kv.second.empty() || BooleanValue::Get(kv.second.back().DefaultCastAs(LogicalTypeId::BOOLEAN))) {
				csv_copy_options["prefix"] = {"[\n\t"};
				csv_copy_options["suffix"] = {"\n]\n"};
				csv_copy_options["new_line"] = {",\n\t"};
			}
		} else if (loption == "file_extension") {
			// Since we set the file extension to "json" above, we need to override it
			csv_copy_options["file_extension"] = {GetSingleJSONCopyString(loption, kv.second)};
		} else if (loption == "partition_by") {
			for (const auto &val : kv.second) {
				if (val.IsNull()) {
					ThrowJSONCopyNullException(loption);
				}
			}
			if (original_column_names.empty()) {
				auto node_copy = copy_info.select_statement->Copy();
				auto child_binder = Binder::CreateBinder(binder.context, &binder);
				auto bound = child_binder->Bind(*node_copy);
				original_column_names = bound.names;
			}
			auto converted = ConvertVectorToValue(vector<Value>(kv.second));
			auto partition_indices = ParseColumnsOrdered(converted, original_column_names, loption);
			for (auto &partition_index : partition_indices) {
				partition_columns.emplace_back(original_column_names[partition_index]);
			}
			vector<Value> csv_partition_columns;
			for (const auto &partition_column : partition_columns) {
				csv_partition_columns.push_back(partition_column);
			}
			csv_copy_options["partition_by"] = std::move(csv_partition_columns);
		} else if (loption == "write_partition_columns") {
			// Handled below by keeping the partition columns inside the JSON object. We do not forward this to the
			// CSV writer, as that would write the (separate) partition columns as their own JSON lines.
			write_partition_columns = GetJSONCopyBoolean(binder, loption, kv.second);
		} else if (SUPPORTED_BASE_OPTIONS.find(loption) != SUPPORTED_BASE_OPTIONS.end()) {
			if (!kv.second.empty() && kv.second.back().IsNull()) {
				ThrowJSONCopyNullException(loption);
			}
			// We support these base options
			csv_copy_options.insert(kv);
		} else {
			throw BinderException("Unknown option for COPY ... TO ... (FORMAT JSON): \"%s\".", loption);
		}
	}
	if (!write_partition_columns && !partition_columns.empty() &&
	    partition_columns.size() == original_column_names.size()) {
		throw NotImplementedException("No column to write as all columns are specified as partition columns. "
		                              "WRITE_PARTITION_COLUMNS option can be used to write partition columns.");
	}

	// Run the following query to convert everything into a single JSON column, then invoke the CSV writer
	// SELECT TO_JSON(STRUCT_PACK(*COLUMNS(*))) FROM <source>

	auto inner_select_stmt = make_uniq<SelectStatement>();
	inner_select_stmt->node = std::move(copy_info.select_statement);

	auto source_ref = make_uniq<SubqueryRef>(std::move(inner_select_stmt));
	if (!date_format.empty()) {
		source_ref = PushJSONFormatProjection(std::move(source_ref), "json_copy_strftime_if_date", date_format);
	}
	if (!timestamp_format.empty()) {
		source_ref =
		    PushJSONFormatProjection(std::move(source_ref), "json_copy_strftime_if_timestamp", timestamp_format);
	}

	// Build outer: SELECT TO_JSON(STRUCT_PACK(*COLUMNS(*))) FROM <source_ref>
	copy_info.select_statement = make_uniq_base<QueryNode, SelectNode>();
	auto &select_node = copy_info.select_statement->Cast<SelectNode>();
	select_node.from_table = std::move(source_ref);

	auto columns_star = make_uniq<StarExpression>();
	columns_star->IsColumnsMutable() = true;
	if (!write_partition_columns) {
		// Exclude the partition columns from the JSON object - they are kept as separate columns below
		for (const auto &partition_column : partition_columns) {
			columns_star->ExcludeListMutable().insert(QualifiedColumnName(Identifier(partition_column)));
		}
	}
	auto unpack = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK);
	unpack->GetChildrenMutable().push_back(std::move(columns_star));

	vector<unique_ptr<ParsedExpression>> struct_pack_args;
	struct_pack_args.push_back(std::move(unpack));
	auto struct_pack = make_uniq<FunctionExpression>("struct_pack", std::move(struct_pack_args));

	vector<unique_ptr<ParsedExpression>> to_json_args;
	to_json_args.push_back(std::move(struct_pack));
	select_node.select_list.push_back(make_uniq<FunctionExpression>("to_json", std::move(to_json_args)));

	// Keep the partition columns as separate columns so the COPY writer can partition on them. The writer routes rows
	// into the right files based on these columns but does not write them to disk (WRITE_PARTITION_COLUMNS is handled
	// above by keeping the columns inside the JSON object instead).
	for (const auto &partition_column : partition_columns) {
		select_node.select_list.push_back(make_uniq<ColumnRefExpression>(Identifier(partition_column)));
	}

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
