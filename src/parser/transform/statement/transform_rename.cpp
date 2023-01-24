#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformRename(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGRenameStmt *>(node);
	D_ASSERT(stmt);
	D_ASSERT(stmt->relation);

	unique_ptr<AlterInfo> info;

	AlterEntryData data;
	data.if_exists = stmt->missing_ok;
	data.catalog = stmt->relation->catalogname ? stmt->relation->catalogname : INVALID_CATALOG;
	data.schema = stmt->relation->schemaname ? stmt->relation->schemaname : INVALID_SCHEMA;
	if (stmt->relation->relname) {
		data.name = stmt->relation->relname;
	}
	if (stmt->relation->schemaname) {
	}
	// first we check the type of ALTER
	switch (stmt->renameType) {
	case duckdb_libpgquery::PG_OBJECT_COLUMN: {
		// change column name

		// get the old name and the new name
		string old_name = stmt->subname;
		string new_name = stmt->newname;
		info = make_unique<RenameColumnInfo>(std::move(data), old_name, new_name);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_TABLE: {
		// change table name
		string new_name = stmt->newname;
		info = make_unique<RenameTableInfo>(std::move(data), new_name);
		break;
	}

	case duckdb_libpgquery::PG_OBJECT_VIEW: {
		// change view name
		string new_name = stmt->newname;
		info = make_unique<RenameViewInfo>(std::move(data), new_name);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_DATABASE:
	default:
		throw NotImplementedException("Schema element not supported yet!");
	}
	D_ASSERT(info);

	auto result = make_unique<AlterStatement>();
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
