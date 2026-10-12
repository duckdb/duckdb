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
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/sql_export/logical_plan_sql_exporter_internal.hpp"

namespace duckdb {

static LogicalPlanVerificationIssue UnsupportedDML(const LogicalPlanVerificationPath &path, string feature,
                                                   string message) {
	return LogicalPlanSQLExportHelpers::PlanUnsupportedFeature(path, std::move(feature), std::move(message));
}

//! The single row-id column SQL can reference on the modified table; multi-column row ids have no SQL name,
//! and a physical column with the row-id name would capture the generated references.
static LogicalPlanVerificationResult<Identifier> ExportedRowIdName(const TableCatalogEntry &table,
                                                                   const LogicalPlanVerificationPath &path) {
	auto row_id_columns = table.GetRowIdColumns();
	auto virtual_columns = table.GetVirtualColumns();
	auto entry = row_id_columns.size() == 1 ? virtual_columns.find(row_id_columns[0]) : virtual_columns.end();
	if (entry == virtual_columns.end() || table.ColumnExists(entry->second.name)) {
		return LogicalPlanVerificationResult<Identifier>::Failure(
		    {UnsupportedDML(path, "dml_row_id", "The modified table does not expose a single named row-id column")});
	}
	return LogicalPlanVerificationResult<Identifier>::Success(Identifier(entry->second.name));
}

static unique_ptr<TableRef> ModifiedTableRef(const TableCatalogEntry &table) {
	auto reference = make_uniq<BaseTableRef>();
	reference->SetQualifiedName(table.schema.GetQualifiedName(table.name));
	return std::move(reference);
}

static LogicalPlanSQLExportResult ExportInsertNode(LogicalInsert &op, LogicalPlanSQLExportContext &context,
                                                   const LogicalPlanVerificationPath &path) {
	if (op.on_conflict_info.action_type != OnConflictAction::THROW || !op.insert_values.empty()) {
		return LogicalPlanSQLExportResult::Failure(
		    {UnsupportedDML(path, "insert_on_conflict", "INSERT ... ON CONFLICT cannot be exported")});
	}
	if (op.children.size() != 1) {
		return LogicalPlanSQLExportResult::Failure(
		    {UnsupportedDML(path, "dml_source", "INSERT without a source relation cannot be exported")});
	}
	auto fields = LogicalPlanSQLExportHelpers::CreateFields(op, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields);
	}
	auto child = context.Export(*op.children[0], LogicalPlanSQLExportHelpers::PlanChildPath(path, 0));
	if (child.HasError()) {
		return child;
	}
	// The bound source produces every physical column in declaration order, with defaults filled in.
	vector<Identifier> target_columns;
	for (auto &column : op.table.GetColumns().Physical()) {
		target_columns.push_back(column.Name());
	}
	if (child.GetValue().fields.size() != target_columns.size()) {
		return LogicalPlanSQLExportResult::Failure(
		    {UnsupportedDML(path, "insert_source", "The INSERT source does not cover the table's physical columns")});
	}
	auto node = make_uniq<InsertQueryNode>();
	node->qualified_name = op.table.schema.GetQualifiedName(op.table.name);
	node->columns = std::move(target_columns);
	node->select_statement = make_uniq<SelectStatement>();
	node->select_statement->node = std::move(child.GetValue().query);
	return LogicalPlanSQLExportResult::Success({std::move(node), std::move(fields.GetValue())});
}

static LogicalPlanSQLExportResult ExportDeleteNode(LogicalDelete &op, LogicalPlanSQLExportContext &context,
                                                   const LogicalPlanVerificationPath &path) {
	if (op.children.size() != 1) {
		return LogicalPlanSQLExportResult::Failure(
		    {UnsupportedDML(path, "dml_source", "DELETE without a source relation cannot be exported")});
	}
	auto row_id_name = ExportedRowIdName(op.table, path);
	if (row_id_name.HasError()) {
		return LogicalPlanSQLExportResult::Failure(row_id_name);
	}
	auto fields = LogicalPlanSQLExportHelpers::CreateFields(op, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields);
	}
	// The row-id reference the operator deletes by carries the row-id column's name as its alias.
	optional_ptr<const BoundColumnRefExpression> row_id_reference;
	bool ambiguous = false;
	for (auto &expression : op.expressions) {
		if (expression->GetAlias() != row_id_name.GetValue()) {
			continue;
		}
		ambiguous |= static_cast<bool>(row_id_reference);
		if (expression->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			row_id_reference = &expression->Cast<BoundColumnRefExpression>();
		}
	}
	if (!row_id_reference || ambiguous || row_id_reference->Depth() != 0) {
		return LogicalPlanSQLExportResult::Failure(
		    {UnsupportedDML(path, "dml_row_id", "The deleted row-id column was not found in the plan")});
	}
	auto child = context.ExportChild(*op.children[0], LogicalPlanSQLExportHelpers::PlanChildPath(path, 0));
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
		return LogicalPlanSQLExportResult::Failure(
		    {UnsupportedDML(path, "dml_row_id", "The deleted row-id column was not found in the exported source")});
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
	node->table = ModifiedTableRef(op.table);
	node->condition = std::move(subquery);
	return LogicalPlanSQLExportResult::Success({std::move(node), std::move(fields.GetValue())});
}

static LogicalPlanSQLExportResult ExportUpdateNode(LogicalUpdate &op, LogicalPlanSQLExportContext &context,
                                                   const LogicalPlanVerificationPath &path) {
	if (op.children.size() != 1) {
		return LogicalPlanSQLExportResult::Failure(
		    {UnsupportedDML(path, "dml_source", "UPDATE without a source relation cannot be exported")});
	}
	auto row_id_name = ExportedRowIdName(op.table, path);
	if (row_id_name.HasError()) {
		return LogicalPlanSQLExportResult::Failure(row_id_name);
	}
	auto fields = LogicalPlanSQLExportHelpers::CreateFields(op, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields);
	}
	// Keep the table's own name reserved so the generated source alias cannot capture it.
	context.NextRelationAlias(op.table.name);
	auto child = context.ExportChild(*op.children[0], LogicalPlanSQLExportHelpers::PlanChildPath(path, 0));
	if (child.HasError()) {
		return LogicalPlanSQLExportResult::Failure(child);
	}
	auto &child_fields = child.GetValue().relation.fields;
	if (op.expressions.size() != op.columns.size() || child_fields.size() <= op.columns.size()) {
		return LogicalPlanSQLExportResult::Failure(
		    {UnsupportedDML(path, "dml_row_id", "The updated row-id column was not found in the exported source")});
	}
	// The bound source keeps the row id as its final column.
	auto row_id_field = child_fields.size() - 1;
	auto expression_context =
	    LogicalPlanSQLExportHelpers::CreateBindingContext(context.GetClientContext(), {child.GetValue()});
	vector<reference<const Expression>> set_expressions;
	for (auto &expression : op.expressions) {
		set_expressions.push_back(*expression);
	}
	auto set_info = make_uniq<UpdateSetInfo>();
	for (idx_t i = 0; i < op.columns.size(); i++) {
		auto exported = context.ExportExpression(op, set_expressions, i, expression_context, path);
		if (exported.HasError()) {
			return LogicalPlanSQLExportResult::Failure(exported);
		}
		set_info->columns.push_back(op.table.GetColumns().GetColumn(op.columns[i]).Name());
		set_info->expressions.push_back(std::move(exported.GetValue()));
	}
	set_info->condition = make_uniq<ComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>(row_id_name.GetValue(), op.table.name),
	    LogicalPlanSQLExportHelpers::ChildColumn(child.GetValue(), row_id_field));
	auto node = make_uniq<UpdateQueryNode>();
	node->table = ModifiedTableRef(op.table);
	node->from_table = LogicalPlanSQLExportHelpers::CreateSubquery(std::move(child.GetValue()));
	node->set_info = std::move(set_info);
	return LogicalPlanSQLExportResult::Success({std::move(node), std::move(fields.GetValue())});
}

static optional<LogicalPlanVerificationIssue> DMLStatementIssue(const LogicalOperator &op,
                                                                const LogicalPlanVerificationPath &path) {
	if (op.type == LogicalOperatorType::LOGICAL_UPDATE && op.Cast<LogicalUpdate>().capture_old_rows) {
		return UnsupportedDML(path, "dml_old_rows", "UPDATE with OLD row capture cannot be exported");
	}
	// A bare operator never returns modified rows; that shape only exists under a RETURNING projection.
	bool return_chunk = op.type == LogicalOperatorType::LOGICAL_INSERT   ? op.Cast<LogicalInsert>().return_chunk
	                    : op.type == LogicalOperatorType::LOGICAL_UPDATE ? op.Cast<LogicalUpdate>().return_chunk
	                                                                     : op.Cast<LogicalDelete>().return_chunk;
	if (return_chunk) {
		return UnsupportedDML(path, "dml_returning", "A data-modifying operator returning rows cannot be exported");
	}
	return optional<LogicalPlanVerificationIssue>();
}

static optional<LogicalPlanVerificationIssue> DMLBoundaryIssue(const LogicalPlanSQLExportContext &context,
                                                               const LogicalOperator &op,
                                                               const LogicalPlanVerificationPath &path) {
	if (context.Ancestors().size() != 1) {
		return UnsupportedDML(path, "dml_subplan", "A data-modifying operator below the plan root cannot be exported");
	}
	return DMLStatementIssue(op, path);
}

LogicalPlanSQLExportResult LogicalInsert::ToSQL(LogicalPlanSQLExportContext &context,
                                                const LogicalPlanVerificationPath &path) {
	if (auto issue = DMLBoundaryIssue(context, *this, path)) {
		return LogicalPlanSQLExportResult::Failure({std::move(*issue)});
	}
	return ExportInsertNode(*this, context, path);
}

LogicalPlanSQLExportResult LogicalDelete::ToSQL(LogicalPlanSQLExportContext &context,
                                                const LogicalPlanVerificationPath &path) {
	if (auto issue = DMLBoundaryIssue(context, *this, path)) {
		return LogicalPlanSQLExportResult::Failure({std::move(*issue)});
	}
	return ExportDeleteNode(*this, context, path);
}

LogicalPlanSQLExportResult LogicalUpdate::ToSQL(LogicalPlanSQLExportContext &context,
                                                const LogicalPlanVerificationPath &path) {
	if (auto issue = DMLBoundaryIssue(context, *this, path)) {
		return LogicalPlanSQLExportResult::Failure({std::move(*issue)});
	}
	return ExportUpdateNode(*this, context, path);
}

//! An identity projection over a data-modifying body only forwards its columns under new bindings.
static bool IsIdentityProjectionOverDML(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION || op.children.size() != 1 ||
	    !SQLExportDMLHelpers::MatchDMLBody(*op.children[0])) {
		return false;
	}
	auto child_fields = LogicalPlanSQLExportHelpers::CreateFields(*op.children[0], LogicalPlanVerificationPath());
	return child_fields.IsSuccess() &&
	       LogicalPlanSQLExportHelpers::IsIdentityProjection(op.Cast<LogicalProjection>(), child_fields.GetValue());
}

bool SQLExportDMLHelpers::MatchDMLBody(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_UPDATE:
	case LogicalOperatorType::LOGICAL_DELETE:
		return true;
	default:
		return MatchReturningRoot(op) || IsIdentityProjectionOverDML(op);
	}
}

bool SQLExportDMLHelpers::IsDMLNode(const QueryNode &query) {
	switch (query.type) {
	case QueryNodeType::INSERT_QUERY_NODE:
	case QueryNodeType::UPDATE_QUERY_NODE:
	case QueryNodeType::DELETE_QUERY_NODE:
		return true;
	default:
		return false;
	}
}

LogicalPlanSQLExportResult SQLExportDMLHelpers::ExportBody(LogicalPlanSQLExportContext &context, LogicalOperator &op,
                                                           const LogicalPlanVerificationPath &path) {
	if (auto returning = MatchReturningRoot(op)) {
		return ExportReturning(context, *returning, optional<vector<Identifier>>(), path);
	}
	if (IsIdentityProjectionOverDML(op)) {
		auto fields = LogicalPlanSQLExportHelpers::CreateFields(op, path);
		if (fields.HasError()) {
			return LogicalPlanSQLExportResult::Failure(fields);
		}
		LogicalPlanSQLExportContext::AncestorScope projection_scope(context, op);
		auto exported = ExportBody(context, *op.children[0], LogicalPlanSQLExportHelpers::PlanChildPath(path, 0));
		if (exported.HasError()) {
			return exported;
		}
		// the projection only renames the forwarded columns' bindings
		D_ASSERT(exported.GetValue().fields.size() == fields.GetValue().size());
		exported.GetValue().fields = std::move(fields.GetValue());
		return exported;
	}
	if (auto issue = DMLStatementIssue(op, path)) {
		return LogicalPlanSQLExportResult::Failure({std::move(*issue)});
	}
	LogicalPlanSQLExportContext::AncestorScope op_scope(context, op);
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
		return ExportInsertNode(op.Cast<LogicalInsert>(), context, path);
	case LogicalOperatorType::LOGICAL_UPDATE:
		return ExportUpdateNode(op.Cast<LogicalUpdate>(), context, path);
	default:
		return ExportDeleteNode(op.Cast<LogicalDelete>(), context, path);
	}
}

optional_ptr<LogicalProjection> SQLExportDMLHelpers::MatchReturningRoot(LogicalOperator &root) {
	if (root.type != LogicalOperatorType::LOGICAL_PROJECTION || root.children.size() != 1) {
		return nullptr;
	}
	auto &child = *root.children[0];
	switch (child.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
		if (!child.Cast<LogicalInsert>().return_chunk) {
			return nullptr;
		}
		break;
	case LogicalOperatorType::LOGICAL_UPDATE:
		if (!child.Cast<LogicalUpdate>().return_chunk) {
			return nullptr;
		}
		break;
	case LogicalOperatorType::LOGICAL_DELETE:
		if (!child.Cast<LogicalDelete>().return_chunk) {
			return nullptr;
		}
		break;
	default:
		return nullptr;
	}
	return &root.Cast<LogicalProjection>();
}

//! Binding context resolving the modified rows a RETURNING projection references: the table's physical
//! columns bind positionally, and each referenced virtual column (e.g. rowid) is appended behind them
//! carrying the column name it was bound from.
static LogicalPlanVerificationResult<BoundExpressionSQLExportContext>
ReturningBindingContext(ClientContext &client, const LogicalProjection &projection, const TableCatalogEntry &table,
                        TableIndex table_index, const LogicalPlanVerificationPath &path) {
	column_binding_map_t<ResolvedSQLColumnReference> entries;
	idx_t physical_count = 0;
	for (auto &column : table.GetColumns().Physical()) {
		entries.emplace(ColumnBinding(table_index, ProjectionIndex(physical_count++)),
		                ResolvedSQLColumnReference {{column.Name()}, column.Type()});
	}
	auto virtual_columns = table.GetVirtualColumns();
	optional<LogicalPlanVerificationIssue> unsupported;
	for (auto &expression : projection.expressions) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(*expression, [&](const BoundColumnRefExpression
		                                                                                   &column) {
			if (unsupported || column.Binding().table_index != table_index ||
			    column.Binding().column_index < physical_count) {
				return;
			}
			for (auto &entry : virtual_columns) {
				if (entry.second.name != column.GetAlias() || table.ColumnExists(entry.second.name) ||
				    !entry.second.type.EqualsIncludingCollation(column.GetReturnType())) {
					continue;
				}
				entries.emplace(column.Binding(), ResolvedSQLColumnReference {{entry.second.name}, entry.second.type});
				return;
			}
			unsupported = UnsupportedDML(path, "dml_returning_reference",
			                             "RETURNING references a column SQL cannot name on the modified table");
		});
	}
	if (unsupported) {
		return LogicalPlanVerificationResult<BoundExpressionSQLExportContext>::Failure({std::move(*unsupported)});
	}
	BoundExpressionSQLExportContext result;
	result.client_context = &client;
	result.discard_optimizer_metadata = true;
	result.resolve_binding =
	    [entries = std::move(entries)](const ColumnBinding &binding) -> optional<ResolvedSQLColumnReference> {
		auto entry = entries.find(binding);
		if (entry != entries.end()) {
			return entry->second;
		}
		return {};
	};
	return LogicalPlanVerificationResult<BoundExpressionSQLExportContext>::Success(std::move(result));
}

LogicalPlanSQLExportResult SQLExportDMLHelpers::ExportReturning(LogicalPlanSQLExportContext &context,
                                                                LogicalProjection &root,
                                                                const optional<vector<Identifier>> &output_names,
                                                                const LogicalPlanVerificationPath &path) {
	auto fields = LogicalPlanSQLExportHelpers::CreateFields(root, path);
	if (fields.HasError()) {
		return LogicalPlanSQLExportResult::Failure(fields);
	}
	auto &op = *root.children[0];
	auto child_path = LogicalPlanSQLExportHelpers::PlanChildPath(path, 0);
	LogicalPlanSQLExportContext::AncestorScope root_scope(context, root);
	LogicalPlanSQLExportContext::AncestorScope op_scope(context, op);
	optional_ptr<const TableCatalogEntry> table;
	TableIndex table_index;
	auto exported = [&]() -> LogicalPlanSQLExportResult {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_INSERT: {
			auto &insert = op.Cast<LogicalInsert>();
			table = &insert.table;
			table_index = insert.table_index;
			return ExportInsertNode(insert, context, child_path);
		}
		case LogicalOperatorType::LOGICAL_UPDATE: {
			auto &update = op.Cast<LogicalUpdate>();
			if (update.capture_old_rows) {
				return LogicalPlanSQLExportResult::Failure(
				    {UnsupportedDML(child_path, "dml_old_rows", "UPDATE with OLD row capture cannot be exported")});
			}
			table = &update.table;
			table_index = update.table_index;
			return ExportUpdateNode(update, context, child_path);
		}
		default: {
			auto &del = op.Cast<LogicalDelete>();
			table = &del.table;
			table_index = del.table_index;
			return ExportDeleteNode(del, context, child_path);
		}
		}
	}();
	if (exported.HasError()) {
		return exported;
	}
	auto expression_context = ReturningBindingContext(context.GetClientContext(), root, *table, table_index, path);
	if (expression_context.HasError()) {
		return LogicalPlanSQLExportResult::Failure(expression_context);
	}
	auto expressions = LogicalPlanSQLExportHelpers::CollectExpressions(root);
	vector<unique_ptr<ParsedExpression>> returning_list;
	for (idx_t i = 0; i < root.expressions.size(); i++) {
		auto expression = context.ExportExpression(root, expressions, i, expression_context.GetValue(), path);
		if (expression.HasError()) {
			return LogicalPlanSQLExportResult::Failure(expression);
		}
		if (output_names && output_names->size() == root.expressions.size()) {
			expression.GetValue()->SetAlias((*output_names)[i]);
		} else if (!root.expressions[i]->GetAlias().empty()) {
			expression.GetValue()->SetAlias(root.expressions[i]->GetAlias());
		}
		returning_list.push_back(std::move(expression.GetValue()));
	}
	auto &query = *exported.GetValue().query;
	switch (query.type) {
	case QueryNodeType::INSERT_QUERY_NODE:
		query.Cast<InsertQueryNode>().returning_list = std::move(returning_list);
		break;
	case QueryNodeType::UPDATE_QUERY_NODE:
		query.Cast<UpdateQueryNode>().returning_list = std::move(returning_list);
		break;
	default:
		query.Cast<DeleteQueryNode>().returning_list = std::move(returning_list);
		break;
	}
	return LogicalPlanSQLExportResult::Success({std::move(exported.GetValue().query), std::move(fields.GetValue())});
}

} // namespace duckdb
