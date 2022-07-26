#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformRename(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGRenameStmt *>(node);
	D_ASSERT(stmt);
	D_ASSERT(stmt->relation);

	unique_ptr<AlterInfo> info;

	// first we check the type of ALTER
	switch (stmt->renameType) {
	case duckdb_libpgquery::PG_OBJECT_COLUMN: {
		// change column name

		// get the table and schema
		string schema = INVALID_SCHEMA;
		string table;
		D_ASSERT(stmt->relation->relname);
		if (stmt->relation->relname) {
			table = stmt->relation->relname;
		}
		if (stmt->relation->schemaname) {
			schema = stmt->relation->schemaname;
		}
		// get the old name and the new name
		string old_name = stmt->subname;
		string new_name = stmt->newname;
		info = make_unique<RenameColumnInfo>(schema, table, old_name, new_name);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_TABLE: {
		// change table name

		// get the table and schema
		string schema = DEFAULT_SCHEMA;
		string table;
		D_ASSERT(stmt->relation->relname);
		if (stmt->relation->relname) {
			table = stmt->relation->relname;
		}
		if (stmt->relation->schemaname) {
			schema = stmt->relation->schemaname;
		}
		string new_name = stmt->newname;
		info = make_unique<RenameTableInfo>(schema, table, new_name);
		break;
	}

	case duckdb_libpgquery::PG_OBJECT_VIEW: {
		// change view name

		// get the view and schema
		string schema = DEFAULT_SCHEMA;
		string view;
		D_ASSERT(stmt->relation->relname);
		if (stmt->relation->relname) {
			view = stmt->relation->relname;
		}
		if (stmt->relation->schemaname) {
			schema = stmt->relation->schemaname;
		}
		string new_name = stmt->newname;
		info = make_unique<RenameViewInfo>(schema, view, new_name);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_DATABASE:
	default:
		throw NotImplementedException("Schema element not supported yet!");
	}
	D_ASSERT(info);
	info->if_exists = stmt->missing_ok;

	auto result = make_unique<AlterStatement>();
	result->info = move(info);
	return result;
}

} // namespace duckdb
