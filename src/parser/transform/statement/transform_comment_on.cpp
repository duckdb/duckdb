#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformCommentOn(duckdb_libpgquery::PGCommentOnStmt &stmt) {
	auto qualified_name = TransformQualifiedName(*stmt.name);

	auto res = make_uniq<AlterStatement>();
	unique_ptr<AlterInfo> info;

	auto expr = TransformExpression(stmt.value);
	if (expr->expression_class != ExpressionClass::CONSTANT) {
		throw NotImplementedException("Can only use constants as comments");
	}
	auto comment_value = expr->Cast<ConstantExpression>().value;

	CatalogType type = CatalogType::INVALID;

	// Regular CatalogTypes
	switch (stmt.object_type) {
	case duckdb_libpgquery::PG_OBJECT_TABLE:
		type = CatalogType::TABLE_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_SCHEMA:
		type = CatalogType::SCHEMA_ENTRY;
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
		info = make_uniq<SetCommentInfo>(type, qualified_name.catalog, qualified_name.schema,
		                                 qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
	} else if (stmt.object_type == duckdb_libpgquery::PG_OBJECT_COLUMN) {
		// Special case: Table Column
		AlterEntryData alter_entry_data;
		alter_entry_data.catalog = INVALID_CATALOG; // TODO HACKY: support fully specified path
		alter_entry_data.schema = qualified_name.catalog;
		alter_entry_data.name = qualified_name.schema;

		info = make_uniq<AlterColumnCommentInfo>(alter_entry_data, qualified_name.name, comment_value);
	} else if (stmt.object_type == duckdb_libpgquery::PG_OBJECT_DATABASE) {
		throw NotImplementedException("Adding comments to databases in not implemented");
	}

	if (info) {
		res->info = std::move(info);
		return res;
	}

	throw NotImplementedException("Can not comment on this type");
}

} // namespace duckdb
