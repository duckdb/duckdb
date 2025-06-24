#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformCommentOn(duckdb_libpgquery::PGCommentOnStmt &stmt) {
	QualifiedName qualified_name;
	string column_name;

	if (stmt.object_type != duckdb_libpgquery::PG_OBJECT_COLUMN) {
		qualified_name = TransformQualifiedName(*stmt.name);
	} else {
		auto transformed_expr = TransformExpression(stmt.column_expr);

		if (transformed_expr->GetExpressionType() != ExpressionType::COLUMN_REF) {
			throw ParserException("Unexpected expression found, expected column reference to comment on (e.g. "
			                      "'schema.table.column'), found '%s'",
			                      transformed_expr->ToString());
		}

		auto colref_expr = transformed_expr->Cast<ColumnRefExpression>();

		if (colref_expr.column_names.size() > 4) {
			throw ParserException("Invalid column reference: '%s', too many dots", colref_expr.ToString());
		}
		if (colref_expr.column_names.size() < 2) {
			throw ParserException("Invalid column reference: '%s', please specify a table", colref_expr.ToString());
		}

		column_name = colref_expr.GetColumnName();
		qualified_name.name = colref_expr.column_names.size() > 1 ? colref_expr.GetTableName() : "";

		if (colref_expr.column_names.size() == 4) {
			qualified_name.catalog = colref_expr.column_names[0];
			qualified_name.schema = colref_expr.column_names[1];
		} else if (colref_expr.column_names.size() == 3) {
			qualified_name.schema = colref_expr.column_names[0];
		}
	}

	auto res = make_uniq<AlterStatement>();
	unique_ptr<AlterInfo> info;

	auto expr = TransformExpression(stmt.value);
	if (expr->GetExpressionClass() != ExpressionClass::CONSTANT) {
		throw NotImplementedException("Can only use constants as comments");
	}
	auto comment_value = expr->Cast<ConstantExpression>().value;

	CatalogType type = CatalogType::INVALID;

	// Regular CatalogTypes
	switch (stmt.object_type) {
	case duckdb_libpgquery::PG_OBJECT_TABLE:
		type = CatalogType::TABLE_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_INDEX:
		type = CatalogType::INDEX_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_VIEW:
		type = CatalogType::VIEW_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_FUNCTION:
		type = CatalogType::MACRO_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_TABLE_MACRO:
		type = CatalogType::TABLE_MACRO_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_SEQUENCE:
		type = CatalogType::SEQUENCE_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_TYPE:
		type = CatalogType::TYPE_ENTRY;
		break;
	default:
		break;
	}

	if (type != CatalogType::INVALID) {
		info = make_uniq<SetCommentInfo>(type, qualified_name.catalog, qualified_name.schema, qualified_name.name,
		                                 comment_value, OnEntryNotFound::THROW_EXCEPTION);
	} else if (stmt.object_type == duckdb_libpgquery::PG_OBJECT_COLUMN) {
		info = make_uniq<SetColumnCommentInfo>(qualified_name.catalog, qualified_name.schema, qualified_name.name,
		                                       column_name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
	} else if (stmt.object_type == duckdb_libpgquery::PG_OBJECT_DATABASE) {
		throw NotImplementedException("Adding comments to databases is not implemented");
	} else if (stmt.object_type == duckdb_libpgquery::PG_OBJECT_SCHEMA) {
		throw NotImplementedException("Adding comments to schemas is not implemented");
	}

	if (info) {
		res->info = std::move(info);
		return res;
	}

	throw NotImplementedException("Can not comment on this type");
}

} // namespace duckdb
