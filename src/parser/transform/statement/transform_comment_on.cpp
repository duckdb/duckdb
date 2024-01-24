#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

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

	switch (stmt.object_type) {
	case duckdb_libpgquery::PG_OBJECT_TABLE: {
		info = make_uniq<SetCommentInfo>(CatalogType::TABLE_ENTRY, qualified_name.catalog, qualified_name.schema, qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_SCHEMA: {
		info = make_uniq<SetCommentInfo>(CatalogType::SCHEMA_ENTRY, qualified_name.catalog, qualified_name.schema, qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_INDEX: {
		info = make_uniq<SetCommentInfo>(CatalogType::INDEX_ENTRY, qualified_name.catalog, qualified_name.schema, qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_VIEW: {
		info = make_uniq<SetCommentInfo>(CatalogType::VIEW_ENTRY, qualified_name.catalog, qualified_name.schema, qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_DATABASE: {
		info = make_uniq<SetCommentInfo>(CatalogType::DATABASE_ENTRY, qualified_name.catalog, qualified_name.schema, qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_FUNCTION: {
		info = make_uniq<SetCommentInfo>(CatalogType::MACRO_ENTRY, qualified_name.catalog, qualified_name.schema, qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_TABLE_MACRO: {
		info = make_uniq<SetCommentInfo>(CatalogType::TABLE_MACRO_ENTRY, qualified_name.catalog, qualified_name.schema, qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_SEQUENCE: {
		info = make_uniq<SetCommentInfo>(CatalogType::SEQUENCE_ENTRY, qualified_name.catalog, qualified_name.schema, qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_TYPE: {
		info = make_uniq<SetCommentInfo>(CatalogType::TYPE_ENTRY, qualified_name.catalog, qualified_name.schema, qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
		break;
	}

	default:
		throw NotImplementedException("Can not comment on this type yet");
	}

	res->info = std::move(info);

	return res;
}

} // namespace duckdb
