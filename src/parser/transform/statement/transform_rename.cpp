#include "duckdb/parser/statement/alter_table_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<AlterTableStatement> Transformer::TransformRename(PGNode *node) {
	auto stmt = reinterpret_cast<PGRenameStmt *>(node);
	assert(stmt);
	assert(stmt->relation);

	unique_ptr<AlterTableInfo> info;

	// first we check the type of ALTER
	switch (stmt->renameType) {
	case PG_OBJECT_COLUMN: {
		// change column name

		// get the table and schema
		string schema = INVALID_SCHEMA;
		string table;
		assert(stmt->relation->relname);
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
	case PG_OBJECT_TABLE: {
		// change table name

		// get the table and schema
		string schema = DEFAULT_SCHEMA;
		string table;
		assert(stmt->relation->relname);
		if (stmt->relation->relname) {
			table = stmt->relation->relname;
		}
		if (stmt->relation->schemaname) {
			schema = stmt->relation->schemaname;
		}
		string new_name = stmt->newname;
		info = make_unique<RenameTableInfo>(schema, table, new_name, false /* is_view */);
		break;
	}

	case PG_OBJECT_VIEW: {
		// change view name

		// get the view and schema
		string schema = DEFAULT_SCHEMA;
		string view;
		assert(stmt->relation->relname);
		if (stmt->relation->relname) {
			view = stmt->relation->relname;
		}
		if (stmt->relation->schemaname) {
			schema = stmt->relation->schemaname;
		}
		string new_name = stmt->newname;
		info = make_unique<RenameTableInfo>(schema, view, new_name, true /* is_view */);
		break;
	}
	case PG_OBJECT_DATABASE:
	default:
		throw NotImplementedException("Schema element not supported yet!");
	}
	assert(info);
	return make_unique<AlterTableStatement>(move(info));
}

} // namespace duckdb
