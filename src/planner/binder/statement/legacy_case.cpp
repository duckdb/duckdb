#include "duckdb/planner/legacy_case.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_trigger_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

static void RewriteSimpleCases(ParsedExpression &expr);

static void RewriteSimpleCases(QueryNode &query) {
	ParsedExpressionIterator::EnumerateAllQueryNodeChildren(
	    query, [](unique_ptr<ParsedExpression> &child) { RewriteSimpleCases(*child); });
}

static void RewriteSimpleCases(ParsedExpression &expr) {
	ParsedExpressionIterator::EnumerateChildren(expr, [](ParsedExpression &child) { RewriteSimpleCases(child); });
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::SUBQUERY:
		RewriteSimpleCases(*expr.Cast<SubqueryExpression>().Subquery()->node);
		break;
	case ExpressionClass::CAST:
		RewriteSimpleCases(expr.Cast<CastExpression>().TargetTypeMutable());
		break;
	case ExpressionClass::CASE:
		expr.Cast<CaseExpression>().ConvertToSearched();
		break;
	default:
		break;
	}
}

static bool RequiresLegacyCase(Catalog &catalog, bool temporary) {
	auto &db = catalog.GetAttached();
	if (temporary || db.IsTemporary() || !db.HasStorageManager()) {
		return false;
	}
	auto &storage = db.GetStorageManager();
	return !storage.InMemory() && storage.GetStorageVersion() < StorageVersion::V2_1_0;
}

void NormalizeLegacyCase(CatalogEntry &entry, ParsedExpression &expression) {
	if (RequiresLegacyCase(entry.ParentCatalog(), entry.temporary)) {
		RewriteSimpleCases(expression);
	}
}

void NormalizeLegacyCase(Catalog &catalog, CreateInfo &info) {
	if (!RequiresLegacyCase(catalog, info.temporary)) {
		return;
	}

	switch (info.type) {
	case CatalogType::VIEW_ENTRY:
		RewriteSimpleCases(*info.Cast<CreateViewInfo>().query->node);
		break;
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		for (auto &macro : info.Cast<CreateMacroInfo>().macros) {
			if (info.type == CatalogType::MACRO_ENTRY) {
				RewriteSimpleCases(*macro->Cast<ScalarMacroFunction>().expression);
			} else {
				RewriteSimpleCases(*macro->Cast<TableMacroFunction>().query_node);
			}
		}
		break;
	case CatalogType::TABLE_ENTRY: {
		auto &table = info.Cast<CreateTableInfo>();
		for (auto &column : table.columns.Logical()) {
			auto &col = table.columns.GetColumnMutable(column.Logical());
			if (col.HasDefaultValue()) {
				RewriteSimpleCases(col.DefaultValueMutable());
			} else if (col.Generated() && col.HasGeneratedExpression()) {
				RewriteSimpleCases(col.GeneratedExpressionMutable());
			}
		}
		for (auto &constraint : table.constraints) {
			if (constraint->type == ConstraintType::CHECK) {
				RewriteSimpleCases(*constraint->Cast<CheckConstraint>().expression);
			}
		}
		// The SELECT in CREATE TABLE AS executes once and is not part of the stored definition.
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		auto &index = info.Cast<CreateIndexInfo>();
		for (auto &expr : index.expressions) {
			RewriteSimpleCases(*expr);
		}
		for (auto &expr : index.parsed_expressions) {
			RewriteSimpleCases(*expr);
		}
		break;
	}
	case CatalogType::TRIGGER_ENTRY:
		RewriteSimpleCases(*info.Cast<CreateTriggerInfo>().trigger_action);
		break;
	default:
		break;
	}
}

} // namespace duckdb
