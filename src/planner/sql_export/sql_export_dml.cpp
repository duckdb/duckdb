#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/sql_export/logical_plan_sql_exporter_internal.hpp"

namespace duckdb {

static optional<LogicalPlanVerificationIssue> DMLBoundaryIssue(const LogicalPlanSQLExportContext &context,
                                                               const LogicalOperator &op,
                                                               const LogicalPlanVerificationPath &path) {
	if (context.Ancestors().size() != 1) {
		return LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "dml_subplan", "A data-modifying operator below the plan root cannot be exported");
	}
	if (op.children.size() != 1) {
		return LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "dml_source", "A data-modifying operator without a source relation cannot be exported");
	}
	return optional<LogicalPlanVerificationIssue>();
}

//! The single row-id column SQL can reference on the modified table; multi-column row ids have no SQL name.
static LogicalPlanVerificationResult<Identifier> ExportedRowIdName(const TableCatalogEntry &table,
                                                                   const LogicalPlanVerificationPath &path) {
	auto row_id_columns = table.GetRowIdColumns();
	auto virtual_columns = table.GetVirtualColumns();
	auto entry = row_id_columns.size() == 1 ? virtual_columns.find(row_id_columns[0]) : virtual_columns.end();
	// A physical column with the row-id name would capture the generated references.
	if (entry == virtual_columns.end() || table.ColumnExists(entry->second.name)) {
		return LogicalPlanVerificationResult<Identifier>::Failure({LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "dml_row_id", "The modified table does not expose a single named row-id column")});
	}
	return LogicalPlanVerificationResult<Identifier>::Success(Identifier(entry->second.name));
}

static unique_ptr<TableRef> ModifiedTableRef(const TableCatalogEntry &table) {
	auto reference = make_uniq<BaseTableRef>();
	reference->SetQualifiedName(table.schema.GetQualifiedName(table.name));
	return std::move(reference);
}

LogicalPlanSQLExportResult LogicalInsert::ToSQL(LogicalPlanSQLExportContext &context,
                                                const LogicalPlanVerificationPath &path) {
	if (auto issue = DMLBoundaryIssue(context, *this, path)) {
		return LogicalPlanSQLExportResult::Failure({std::move(*issue)});
	}
	if (return_chunk) {
		return LogicalPlanSQLExportResult::Failure({LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "dml_returning", "INSERT ... RETURNING cannot be exported")});
	}
	if (on_conflict_info.action_type != OnConflictAction::THROW || !insert_values.empty()) {
		return LogicalPlanSQLExportResult::Failure({LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "insert_on_conflict", "INSERT ... ON CONFLICT cannot be exported")});
	}
	auto fields = LogicalPlanSQLExportHelpers::CreateFields(*this, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields);
	}
	auto child = context.Export(*children[0], LogicalPlanSQLExportHelpers::PlanChildPath(path, 0));
	if (child.HasError()) {
		return child;
	}
	// The bound source produces every physical column in declaration order, with defaults filled in.
	vector<Identifier> target_columns;
	for (auto &column : table.GetColumns().Physical()) {
		target_columns.push_back(column.Name());
	}
	if (child.GetValue().fields.size() != target_columns.size()) {
		return LogicalPlanSQLExportResult::Failure({LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "insert_source", "The INSERT source does not cover the table's physical columns")});
	}
	auto node = make_uniq<InsertQueryNode>();
	node->qualified_name = table.schema.GetQualifiedName(table.name);
	node->columns = std::move(target_columns);
	node->select_statement = make_uniq<SelectStatement>();
	node->select_statement->node = std::move(child.GetValue().query);
	return LogicalPlanSQLExportResult::Success({std::move(node), std::move(fields.GetValue())});
}

LogicalPlanSQLExportResult LogicalDelete::ToSQL(LogicalPlanSQLExportContext &context,
                                                const LogicalPlanVerificationPath &path) {
	if (auto issue = DMLBoundaryIssue(context, *this, path)) {
		return LogicalPlanSQLExportResult::Failure({std::move(*issue)});
	}
	if (return_chunk) {
		return LogicalPlanSQLExportResult::Failure({LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "dml_returning", "DELETE ... RETURNING cannot be exported")});
	}
	auto row_id_name = ExportedRowIdName(table, path);
	if (row_id_name.HasError()) {
		return LogicalPlanSQLExportResult::Failure(row_id_name);
	}
	auto fields = LogicalPlanSQLExportHelpers::CreateFields(*this, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields);
	}
	// The row-id reference the operator deletes by carries the row-id column's name as its alias.
	optional_ptr<const BoundColumnRefExpression> row_id_reference;
	bool ambiguous = false;
	for (auto &expression : expressions) {
		if (expression->GetAlias() != row_id_name.GetValue()) {
			continue;
		}
		ambiguous |= static_cast<bool>(row_id_reference);
		if (expression->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			row_id_reference = &expression->Cast<BoundColumnRefExpression>();
		}
	}
	if (!row_id_reference || ambiguous || row_id_reference->Depth() != 0) {
		return LogicalPlanSQLExportResult::Failure({LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "dml_row_id", "The deleted row-id column was not found in the plan")});
	}
	auto child = context.ExportChild(*children[0], LogicalPlanSQLExportHelpers::PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child);
	}
	auto &child_fields = child.GetValue().relation.fields;
	optional_idx row_id_field;
	for (idx_t i = 0; i < child_fields.size(); i++) {
		if (child_fields[i].source_binding == row_id_reference->Binding()) {
			row_id_field = i;
			break;
		}
	}
	if (!row_id_field.IsValid()) {
		return LogicalPlanSQLExportResult::Failure({LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "dml_row_id", "The deleted row-id column was not found in the exported source")});
	}
	auto plain = LogicalPlanSQLExportHelpers::PlainScope(*child.GetValue().relation.query);
	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(
	    LogicalPlanSQLExportHelpers::ChildColumn(child.GetValue(), row_id_field.GetIndex(), plain));
	LogicalPlanSQLExportHelpers::SetChildScope(*select, std::move(child.GetValue()), plain);
	auto subquery = make_uniq<SubqueryExpression>();
	subquery->SubqueryMutable() = make_uniq<SelectStatement>();
	subquery->SubqueryMutable()->node = std::move(select);
	subquery->GetSubqueryTypeMutable() = SubqueryType::ANY;
	subquery->GetComparisonTypeMutable() = ExpressionType::COMPARE_EQUAL;
	subquery->GetChildMutable() = make_uniq<ColumnRefExpression>(row_id_name.GetValue());
	auto node = make_uniq<DeleteQueryNode>();
	node->table = ModifiedTableRef(table);
	node->condition = std::move(subquery);
	return LogicalPlanSQLExportResult::Success({std::move(node), std::move(fields.GetValue())});
}

LogicalPlanSQLExportResult LogicalUpdate::ToSQL(LogicalPlanSQLExportContext &context,
                                                const LogicalPlanVerificationPath &path) {
	if (auto issue = DMLBoundaryIssue(context, *this, path)) {
		return LogicalPlanSQLExportResult::Failure({std::move(*issue)});
	}
	if (return_chunk || capture_old_rows) {
		return LogicalPlanSQLExportResult::Failure({LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "dml_returning", "UPDATE ... RETURNING cannot be exported")});
	}
	auto row_id_name = ExportedRowIdName(table, path);
	if (row_id_name.HasError()) {
		return LogicalPlanSQLExportResult::Failure(row_id_name);
	}
	auto fields = LogicalPlanSQLExportHelpers::CreateFields(*this, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields);
	}
	// Keep the table's own name reserved so the generated source alias cannot capture it.
	context.NextRelationAlias(table.name);
	auto child = context.ExportChild(*children[0], LogicalPlanSQLExportHelpers::PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child);
	}
	auto &child_fields = child.GetValue().relation.fields;
	if (expressions.size() != columns.size() || child_fields.size() <= columns.size()) {
		return LogicalPlanSQLExportResult::Failure({LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(
		    path, "dml_row_id", "The updated row-id column was not found in the exported source")});
	}
	// The bound source keeps the row id as its final column.
	auto row_id_field = child_fields.size() - 1;
	auto expression_context =
	    LogicalPlanSQLExportHelpers::CreateBindingContext(context.GetClientContext(), {child.GetValue()});
	vector<reference<const Expression>> set_expressions;
	for (auto &expression : expressions) {
		set_expressions.push_back(*expression);
	}
	auto set_info = make_uniq<UpdateSetInfo>();
	for (idx_t i = 0; i < columns.size(); i++) {
		auto exported = context.ExportExpression(*this, set_expressions, i, expression_context, path);
		if (exported.HasError()) {
			return LogicalPlanSQLExportResult::Failure(exported);
		}
		set_info->columns.push_back(table.GetColumns().GetColumn(columns[i]).Name());
		set_info->expressions.push_back(std::move(exported.GetValue()));
	}
	set_info->condition = make_uniq<ComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>(row_id_name.GetValue(), table.name),
	    LogicalPlanSQLExportHelpers::ChildColumn(child.GetValue(), row_id_field));
	auto node = make_uniq<UpdateQueryNode>();
	node->table = ModifiedTableRef(table);
	node->from_table = LogicalPlanSQLExportHelpers::CreateSubquery(std::move(child.GetValue()));
	node->set_info = std::move(set_info);
	return LogicalPlanSQLExportResult::Success({std::move(node), std::move(fields.GetValue())});
}

} // namespace duckdb
