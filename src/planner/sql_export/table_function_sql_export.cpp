#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
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

static string SQLFunctionCallGuard(const LogicalGet &get, bool has_input) {
	const auto argument_count = get.function.GetArguments().size();
	const bool has_expected_parameters =
	    get.function.HasVarArgs() ? get.parameters.size() >= argument_count : get.parameters.size() == argument_count;
	if (!has_input && !has_expected_parameters) {
		return "positional_parameters";
	}
	if (has_input && !get.function.in_out_function) {
		return "input_child";
	}
	if (has_input && (get.input_table_types.empty() || get.input_table_types.size() > get.children[0]->types.size())) {
		return "input_columns";
	}
	if (get.ordinality_idx.IsValid() && get.source_ordinality != OrdinalityType::WITH_ORDINALITY) {
		return "ordinality";
	}
	if (!get.scan_partition_indices.empty()) {
		return "scan_partitions";
	}
	{
		auto name = get.function.GetQualifiedName();
		if (name.Catalog().empty()) {
			return "catalog_identifier";
		}
		if (name.Schema().empty()) {
			return "schema_identifier";
		}
		for (auto &component : name.Path()) {
			if (component.empty()) {
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
		if (index.HasType() && !get.returned_types[primary_index].EqualsIncludingCollation(index.GetScanType())) {
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
	if (!source_type.EqualsIncludingCollation(index.GetScanType())) {
		return make_uniq<CastExpression>(index.GetScanType(), std::move(column));
	}
	return std::move(column);
}

TableFunctionToSQLResult TableFunction::ToSQLFunctionCall(ClientContext &context, const LogicalGet &get,
                                                          TableFunctionToSQLInput request) {
	auto input = std::move(request.child);
	const auto &relation_alias = request.relation_alias;
	auto source_ordinality = request.source_ordinality;
	auto guard = SQLFunctionCallGuard(get, input != nullptr);
	if (!guard.empty()) {
		return {nullptr, std::move(guard)};
	}
	auto function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> parameters;
	const auto &signature = get.function.GetArguments();
	const bool table_parameter = std::find(signature.begin(), signature.end(), LogicalType::TABLE) != signature.end();
	if (input && !table_parameter) {
		if (input->column_name_alias.size() < get.input_table_types.size()) {
			return {nullptr, "input_columns"};
		}
		for (idx_t i = 0; i < get.input_table_types.size(); i++) {
			parameters.push_back(make_uniq<ColumnRefExpression>(input->column_name_alias[i], input->alias));
		}
	} else {
		bool consumed_input = false;
		for (idx_t i = 0; i < get.parameters.size(); i++) {
			if (i < signature.size() && signature[i] == LogicalType::TABLE) {
				const bool has_available_input = input && !consumed_input && get.projected_input.empty();
				const bool has_input_columns = has_available_input &&
				                               input->column_name_alias.size() >= get.input_table_types.size() &&
				                               get.input_table_names.size() == get.input_table_types.size();
				if (!has_input_columns) {
					return {nullptr, "table_parameter"};
				}
				auto query = make_uniq<SelectNode>();
				for (idx_t column_idx = 0; column_idx < get.input_table_types.size(); column_idx++) {
					auto column = make_uniq<ColumnRefExpression>(input->column_name_alias[column_idx], input->alias);
					column->SetAlias(get.input_table_names[column_idx]);
					query->select_list.push_back(std::move(column));
				}
				query->from_table = std::move(input);
				auto subquery = make_uniq<SubqueryExpression>();
				subquery->GetSubqueryTypeMutable() = SubqueryType::SCALAR;
				subquery->SubqueryMutable() = make_uniq<SelectStatement>();
				subquery->SubqueryMutable()->node = std::move(query);
				parameters.push_back(std::move(subquery));
				consumed_input = true;
				continue;
			}
			auto exported = BoundExpressionSQLExporter::Export(BoundConstantExpression(get.parameters[i]), {});
			if (exported.HasError()) {
				return {nullptr, "positional_parameter_expression"};
			}
			parameters.push_back(std::move(exported.GetValue()));
		}
		if (input) {
			return {nullptr, "table_parameter"};
		}
	}
	for (auto &parameter : get.named_parameters) {
		auto exported = BoundExpressionSQLExporter::Export(BoundConstantExpression(parameter.second), {});
		if (exported.HasError()) {
			return {nullptr, "named_parameter_expression"};
		}
		parameters.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
		                                                     make_uniq<ColumnRefExpression>(parameter.first),
		                                                     std::move(exported.GetValue())));
	}
	function->function = make_uniq<FunctionExpression>(get.function.GetQualifiedName(), std::move(parameters));
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
	if (request.file_filters) {
		BoundExpressionSQLExportContext export_context;
		export_context.client_context = &context;
		export_context.resolve_binding = [&](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
			auto index = binding.column_index.GetIndex();
			if (binding.table_index != TableIndex(TableFunctionToSQLInput::FILE_FILTER_TABLE_INDEX) ||
			    index >= get.returned_types.size()) {
				return {};
			}
			return ResolvedSQLColumnReference {{function_ref.alias, function_ref.column_name_alias[index]},
			                                   get.returned_types[index]};
		};
		for (auto &predicate : *request.file_filters) {
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

} // namespace duckdb
