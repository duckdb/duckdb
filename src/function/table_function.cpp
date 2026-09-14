#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"

namespace duckdb {

GlobalTableFunctionState::~GlobalTableFunctionState() {
}

LocalTableFunctionState::~LocalTableFunctionState() {
}

PartitionStatistics::PartitionStatistics() : row_start(0), count(0), count_type(CountType::COUNT_APPROXIMATE) {
}

TableFunctionInfo::~TableFunctionInfo() {
}

TableFunction::TableFunction(Identifier name, const vector<LogicalType> &arguments, table_function_t function_,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), arguments), bind(bind), bind_replace(nullptr),
      bind_operator(nullptr), init_global(init_global), init_local(init_local), function(function_),
      in_out_function(nullptr), in_out_function_final(nullptr), statistics(nullptr), statistics_extended(nullptr),
      dependency(nullptr), cardinality(nullptr), get_metrics(nullptr), pushdown_complex_filter(nullptr),
      pushdown_expression(nullptr), combine_schema(nullptr), claim_batch(nullptr), finish_batch(nullptr),
      supports_read_ahead(nullptr), schedule_io(nullptr), to_string(nullptr), table_scan_progress(nullptr),
      get_partition_data(nullptr), get_bind_info(nullptr), projection_expression_pushdown(nullptr),
      get_multi_file_reader(nullptr), supports_pushdown_type(nullptr), supports_pushdown_extract(nullptr),
      is_repeatable(nullptr), get_partition_info(nullptr), get_partition_stats(nullptr), get_virtual_columns(nullptr),
      get_row_id_columns(nullptr), set_scan_order(nullptr), serialize(nullptr), deserialize(nullptr),
      projection_pushdown(false), supports_cast_map(false), filter_pushdown(false), filter_prune(false),
      sampling_pushdown(false), late_materialization(false),
      return_type(TableFunctionReturnType::TABLE_RETURNING_FUNCTION) {
}

TableFunction::TableFunction(Identifier name, const vector<LogicalType> &arguments, std::nullptr_t function_,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), arguments), bind(bind), bind_replace(nullptr),
      bind_operator(nullptr), init_global(init_global), init_local(init_local), function(nullptr),
      in_out_function(nullptr), in_out_function_final(nullptr), statistics(nullptr), statistics_extended(nullptr),
      dependency(nullptr), cardinality(nullptr), get_metrics(nullptr), pushdown_complex_filter(nullptr),
      pushdown_expression(nullptr), combine_schema(nullptr), claim_batch(nullptr), finish_batch(nullptr),
      supports_read_ahead(nullptr), schedule_io(nullptr), to_string(nullptr), table_scan_progress(nullptr),
      get_partition_data(nullptr), get_bind_info(nullptr), projection_expression_pushdown(nullptr),
      get_multi_file_reader(nullptr), supports_pushdown_type(nullptr), supports_pushdown_extract(nullptr),
      is_repeatable(nullptr), get_partition_info(nullptr), get_partition_stats(nullptr), get_virtual_columns(nullptr),
      get_row_id_columns(nullptr), set_scan_order(nullptr), serialize(nullptr), deserialize(nullptr),
      projection_pushdown(false), supports_cast_map(false), filter_pushdown(false), filter_prune(false),
      sampling_pushdown(false), late_materialization(false),
      return_type(TableFunctionReturnType::TABLE_RETURNING_FUNCTION) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, table_function_t function_,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : TableFunction("", arguments, function_, bind, init_global, init_local) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, std::nullptr_t function_, table_function_bind_t bind,
                             table_function_init_global_t init_global, table_function_init_local_t init_local)
    : TableFunction("", arguments, function_, bind, init_global, init_local) {
}

TableFunction::TableFunction() : TableFunction("", {}, nullptr, nullptr, nullptr, nullptr) {
}

bool TableFunction::operator==(const TableFunction &rhs) const {
	return name == rhs.name && arguments == rhs.GetArguments() && varargs == rhs.GetVarArgs() && bind == rhs.bind &&
	       bind_replace == rhs.bind_replace && bind_operator == rhs.bind_operator && init_global == rhs.init_global &&
	       init_local == rhs.init_local && function == rhs.function && in_out_function == rhs.in_out_function &&
	       in_out_function_final == rhs.in_out_function_final && statistics == rhs.statistics &&
	       dependency == rhs.dependency && cardinality == rhs.cardinality &&
	       pushdown_complex_filter == rhs.pushdown_complex_filter && pushdown_expression == rhs.pushdown_expression &&
	       to_string == rhs.to_string && to_sql == rhs.to_sql && table_scan_progress == rhs.table_scan_progress &&
	       get_partition_data == rhs.get_partition_data && get_bind_info == rhs.get_bind_info &&
	       projection_expression_pushdown == rhs.projection_expression_pushdown &&
	       get_multi_file_reader == rhs.get_multi_file_reader && supports_pushdown_type == rhs.supports_pushdown_type &&
	       is_repeatable == rhs.is_repeatable && get_partition_info == rhs.get_partition_info &&
	       get_partition_stats == rhs.get_partition_stats && get_virtual_columns == rhs.get_virtual_columns &&
	       get_row_id_columns == rhs.get_row_id_columns && serialize == rhs.serialize &&
	       deserialize == rhs.deserialize && verify_serialization == rhs.verify_serialization &&
	       projection_pushdown == rhs.projection_pushdown && filter_pushdown == rhs.filter_pushdown &&
	       filter_prune == rhs.filter_prune && sampling_pushdown == rhs.sampling_pushdown &&
	       late_materialization == rhs.late_materialization && return_type == rhs.return_type &&
	       global_initialization == rhs.global_initialization;
}

bool TableFunction::operator!=(const TableFunction &rhs) const {
	return !(*this == rhs);
}

static string SQLFunctionCallGuard(const LogicalGet &get, bool has_input) {
	if (has_input && !get.function.in_out_function) {
		return "input_child";
	}
	if (has_input && (get.input_table_types.empty() || get.input_table_types.size() > get.children[0]->types.size())) {
		return "input_columns";
	}
	if (get.ordinality_idx.IsValid() &&
	    (!get.table_function_ref ||
	     get.table_function_ref->Cast<TableFunctionRef>().with_ordinality != OrdinalityType::WITH_ORDINALITY)) {
		return "ordinality";
	}
	if (!get.scan_partition_indices.empty()) {
		return "scan_partitions";
	}
	if (!get.table_function_ref) {
		auto name = get.function.GetQualifiedName();
		if (!SQLExportHelpers::IsValidIdentifier(name.Catalog())) {
			return "catalog_identifier";
		}
		if (!SQLExportHelpers::IsValidIdentifier(name.Schema())) {
			return "schema_identifier";
		}
		for (auto &component : name.Path()) {
			if (!SQLExportHelpers::IsValidIdentifier(component)) {
				return "function_identifier";
			}
		}
	}
	return string();
}

static bool AppendTableFunctionColumnPath(vector<Identifier> &path, const LogicalType &parent_type,
                                          const ColumnIndex &index, LogicalType &source_type) {
	Identifier name;
	LogicalType child_type = LogicalType::INVALID;
	if (parent_type.id() == LogicalTypeId::STRUCT) {
		auto &children = StructType::GetChildTypes(parent_type);
		if (index.HasPrimaryIndex()) {
			if (index.GetPrimaryIndex() >= children.size()) {
				return false;
			}
			name = children[index.GetPrimaryIndex()].first;
			child_type = children[index.GetPrimaryIndex()].second;
		} else {
			name = Identifier(index.GetFieldName());
			for (auto &child : children) {
				if (child.first == name) {
					child_type = child.second;
					break;
				}
			}
		}
	} else if (parent_type.id() == LogicalTypeId::VARIANT && !index.HasPrimaryIndex()) {
		name = Identifier(index.GetFieldName());
		child_type = parent_type;
	} else {
		return false;
	}
	path.push_back(std::move(name));
	if (index.GetChildIndexes().empty()) {
		source_type = std::move(child_type);
		return true;
	}
	if (index.GetChildIndexes().size() != 1 || child_type.id() == LogicalTypeId::INVALID) {
		return false;
	}
	return AppendTableFunctionColumnPath(path, child_type, index.GetChildIndexes()[0], source_type);
}

static unique_ptr<ParsedExpression> TableFunctionColumn(const LogicalGet &get, const TableFunctionRef &function_ref,
                                                        const ColumnIndex &index) {
	if (index.IsVirtualColumn()) {
		if (index.IsEmptyColumn()) {
			return ConstantExpression::FromValue(Value::INTEGER(1));
		}
		auto entry = get.virtual_columns.find(index.GetPrimaryIndex());
		if (entry == get.virtual_columns.end()) {
			return nullptr;
		}
		return make_uniq<ColumnRefExpression>(entry->second.name, function_ref.alias);
	}
	auto primary_index = index.GetPrimaryIndex();
	if (primary_index >= function_ref.column_name_alias.size() || primary_index >= get.returned_types.size()) {
		return nullptr;
	}
	vector<Identifier> path {function_ref.alias, function_ref.column_name_alias[primary_index]};
	if (!index.IsPushdownExtract()) {
		auto column = make_uniq<ColumnRefExpression>(std::move(path));
		if (index.HasType() &&
		    !SQLExportHelpers::SQLTypesMatch(get.returned_types[primary_index], index.GetScanType())) {
			return make_uniq<CastExpression>(index.GetScanType(), std::move(column));
		}
		return std::move(column);
	}
	LogicalType source_type;
	if (index.GetChildIndexes().size() != 1 ||
	    !AppendTableFunctionColumnPath(path, get.returned_types[primary_index], index.GetChildIndexes()[0],
	                                   source_type)) {
		return nullptr;
	}
	auto column = make_uniq<ColumnRefExpression>(std::move(path));
	if (!SQLExportHelpers::SQLTypesMatch(source_type, index.GetScanType())) {
		return make_uniq<CastExpression>(index.GetScanType(), std::move(column));
	}
	return std::move(column);
}

static TableFunctionToSQLResult SQLFunctionCall(ClientContext &context, const LogicalGet &get,
                                                unique_ptr<TableRef> input, const Identifier &relation_alias,
                                                bool source_ordinality) {
	auto guard = SQLFunctionCallGuard(get, input != nullptr);
	if (!guard.empty()) {
		return {nullptr, std::move(guard)};
	}
	unique_ptr<TableRef> function;
	const auto &signature = get.function.GetArguments();
	const bool table_parameter = std::find(signature.begin(), signature.end(), LogicalType::TABLE) != signature.end();
	if (input && table_parameter) {
		if (!get.table_function_ref || !get.projected_input.empty() ||
		    input->column_name_alias.size() < get.input_table_types.size() ||
		    get.input_table_names.size() != get.input_table_types.size()) {
			return {nullptr, "to_sql_callback_declined_without_guard"};
		}
		function = get.table_function_ref->Copy();
		auto &call = function->Cast<TableFunctionRef>().function->Cast<FunctionExpression>();
		optional_ptr<SubqueryExpression> subquery;
		for (auto &argument : call.GetArgumentsMutable()) {
			auto &expression = argument.GetExpressionMutable();
			if (expression->GetExpressionClass() == ExpressionClass::SUBQUERY) {
				if (subquery) {
					return {nullptr, "to_sql_callback_declined_without_guard"};
				}
				subquery = expression->Cast<SubqueryExpression>();
			}
		}
		if (!subquery) {
			return {nullptr, "to_sql_callback_declined_without_guard"};
		}
		auto query = make_uniq<SelectNode>();
		for (idx_t i = 0; i < get.input_table_types.size(); i++) {
			auto column = make_uniq<ColumnRefExpression>(input->column_name_alias[i], input->alias);
			column->SetAlias(get.input_table_names[i]);
			query->select_list.push_back(std::move(column));
		}
		query->from_table = std::move(input);
		subquery->SubqueryMutable()->node = std::move(query);
	} else if (input) {
		if (input->column_name_alias.size() < get.children[0]->types.size()) {
			return {nullptr, "to_sql_callback_declined_without_guard"};
		}
		vector<unique_ptr<ParsedExpression>> arguments;
		for (idx_t i = 0; i < get.input_table_types.size(); i++) {
			arguments.push_back(make_uniq<ColumnRefExpression>(input->column_name_alias[i], input->alias));
		}
		auto function_ref = make_uniq<TableFunctionRef>();
		function_ref->function = make_uniq<FunctionExpression>(get.function.GetQualifiedName(), std::move(arguments));
		if (get.ordinality_idx.IsValid()) {
			function_ref->with_ordinality = OrdinalityType::WITH_ORDINALITY;
		}
		function = std::move(function_ref);
	} else if (get.table_function_ref) {
		function = get.table_function_ref->Copy();
	} else {
		auto name = get.function.GetQualifiedName();
		vector<unique_ptr<ParsedExpression>> parameters;
		for (auto &parameter : get.parameters) {
			if (!SQLExportHelpers::IsSQLRepresentableType(parameter.type())) {
				return {nullptr, "positional_parameter_type"};
			}
			auto exported = BoundExpressionSQLExporter::Export(BoundConstantExpression(parameter), {});
			if (exported.HasError()) {
				return {nullptr, "positional_parameter_expression"};
			}
			parameters.push_back(std::move(exported.GetValue()));
		}
		for (auto &parameter : get.named_parameters) {
			if (!SQLExportHelpers::IsValidIdentifier(parameter.first)) {
				return {nullptr, "named_parameter_identifier"};
			}
			if (!SQLExportHelpers::IsSQLRepresentableType(parameter.second.type())) {
				return {nullptr, "named_parameter_type"};
			}
			auto exported = BoundExpressionSQLExporter::Export(BoundConstantExpression(parameter.second), {});
			if (exported.HasError()) {
				return {nullptr, "named_parameter_expression"};
			}
			parameters.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
			                                                     make_uniq<ColumnRefExpression>(parameter.first),
			                                                     std::move(exported.GetValue())));
		}
		auto function_ref = make_uniq<TableFunctionRef>();
		function_ref->function = make_uniq<FunctionExpression>(name, std::move(parameters));
		function = std::move(function_ref);
	}
	auto &function_ref = function->Cast<TableFunctionRef>();
	function_ref.with_ordinality = get.ordinality_idx.IsValid() || source_ordinality
	                                   ? OrdinalityType::WITH_ORDINALITY
	                                   : OrdinalityType::WITHOUT_ORDINALITY;
	function_ref.alias = relation_alias;
	function_ref.column_name_alias.clear();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		function_ref.column_name_alias.emplace_back("column" + std::to_string(i));
	}
	if (source_ordinality) {
		function_ref.column_name_alias.emplace_back("column" + std::to_string(get.returned_types.size()));
	}
	auto select = make_uniq<SelectNode>();
	for (auto &index : get.GetColumnIds()) {
		auto column = TableFunctionColumn(get, function_ref, index);
		if (!column) {
			return {nullptr, "to_sql_callback_declined_without_guard"};
		}
		select->select_list.push_back(std::move(column));
	}
	for (auto index : get.projected_input) {
		select->select_list.push_back(make_uniq<ColumnRefExpression>(input->column_name_alias[index], input->alias));
	}
	if (source_ordinality) {
		select->select_list.push_back(
		    make_uniq<ColumnRefExpression>(function_ref.column_name_alias.back(), function_ref.alias));
	}
	if (get.extra_info.file_filter_expressions) {
		BoundExpressionSQLExportContext export_context;
		export_context.client_context = &context;
		export_context.resolve_binding = [&](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
			auto index = binding.column_index.GetIndex();
			if (binding.table_index != TableIndex(0) || index >= get.returned_types.size()) {
				return {};
			}
			return ResolvedSQLColumnReference {{function_ref.alias, function_ref.column_name_alias[index]},
			                                   get.returned_types[index]};
		};
		for (auto &predicate : *get.extra_info.file_filter_expressions) {
			auto exported = BoundExpressionSQLExporter::Export(*predicate, export_context);
			if (exported.HasError()) {
				return {nullptr, "to_sql_callback_declined_without_guard"};
			}
			if (select->where_clause) {
				select->where_clause = make_uniq<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(select->where_clause), std::move(exported.GetValue()));
			} else {
				select->where_clause = std::move(exported.GetValue());
			}
		}
	}

	if (!input) {
		select->from_table = std::move(function);
		return {std::move(select), {}};
	}
	auto join = make_uniq<JoinRef>(JoinRefType::CROSS);
	join->left = std::move(input);
	join->right = std::move(function);
	select->from_table = std::move(join);
	return {std::move(select), {}};
}

TableFunctionToSQLResult TableFunction::ToSQLFunctionCall(ClientContext &context, const LogicalGet &get,
                                                          unique_ptr<TableRef> input,
                                                          const Identifier &relation_alias) {
	return SQLFunctionCall(context, get, std::move(input), relation_alias, false);
}

TableFunctionToSQLResult TableFunction::ToSQLFunctionCallWithOrdinality(ClientContext &context, const LogicalGet &get,
                                                                        unique_ptr<TableRef> input,
                                                                        const Identifier &relation_alias) {
	return SQLFunctionCall(context, get, std::move(input), relation_alias, true);
}

bool TableFunction::Equal(const TableFunction &rhs) const {
	// number of types
	if (this->GetArguments().size() != rhs.GetArguments().size()) {
		return false;
	}
	// argument types
	for (idx_t i = 0; i < this->GetArguments().size(); ++i) {
		if (this->GetArguments()[i] != rhs.GetArguments()[i]) {
			return false;
		}
	}
	// varargs
	if (this->GetVarArgs() != rhs.GetVarArgs()) {
		return false;
	}

	return true; // they are equal
}

bool TableFunctionInput::HandleBlocked(AsyncResult &blocked_result) {
	D_ASSERT(blocked_result.GetResultType() == AsyncResultType::BLOCKED);
	switch (results_execution_mode) {
	case AsyncResultsExecutionMode::TASK_EXECUTOR:
		async_result = std::move(blocked_result);
		return true;
	case AsyncResultsExecutionMode::SYNCHRONOUS:
		// run the I/O synchronously, then loop again to resume
		blocked_result.ExecuteTasksSynchronously();
		if (blocked_result.GetResultType() != AsyncResultType::HAVE_MORE_OUTPUT) {
			throw InternalException("Unexpected behaviour from ExecuteTasksSynchronously");
		}
		return false;
	default:
		throw InternalException("Unexpected AsyncResultsExecutionMode in HandleBlocked");
	}
}

} // namespace duckdb
