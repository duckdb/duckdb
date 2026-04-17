#include "duckdb/parser/parsed_data/alter_scalar_function_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<AlterStatement> Transformer::TransformRename(duckdb_libpgquery::PGRenameStmt &stmt) {
	unique_ptr<AlterInfo> info;

	AlterEntryData data;
	data.if_not_found = TransformOnEntryNotFound(stmt.missing_ok);
	if (!stmt.relation) {
		throw NotImplementedException("Altering schemas is not yet supported");
	}
	data.catalog = stmt.relation->catalogname ? stmt.relation->catalogname : INVALID_CATALOG;
	data.schema = stmt.relation->schemaname ? stmt.relation->schemaname : INVALID_SCHEMA;
	if (stmt.relation->relname) {
		data.name = stmt.relation->relname;
	}
	// first we check the type of ALTER
	switch (stmt.renameType) {
	case duckdb_libpgquery::PG_OBJECT_COLUMN: {
		// change column name

		// get the old name and the new name
		auto names = TransformNameList(*stmt.name_list);
		string new_name = stmt.newname;
		if (names.size() == 1) {
			info = make_uniq<RenameColumnInfo>(std::move(data), std::move(names[0]), std::move(new_name));
		} else {
			info = make_uniq<RenameFieldInfo>(std::move(data), std::move(names), std::move(new_name));
		}
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_TABLE:
	case duckdb_libpgquery::PG_OBJECT_INDEX: {
		// ALTER INDEX ... RENAME TO ... routes through RenameTableInfo because
		// the catalog resolves relation-kind lookups (table/view/index) by name.
		string new_name = stmt.newname;
		info = make_uniq<RenameTableInfo>(std::move(data), new_name);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_VIEW: {
		// change view name
		string new_name = stmt.newname;
		info = make_uniq<RenameViewInfo>(std::move(data), new_name);
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_TABCONSTRAINT: {
		string old_name = stmt.subname;
		string new_name = stmt.newname;
		info = make_uniq<RenameConstraintInfo>(std::move(data), std::move(old_name), std::move(new_name));
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_FUNCTION: {
		string new_name = stmt.newname;
		info = make_uniq<RenameScalarFunctionInfo>(std::move(data), std::move(new_name));
		break;
	}
	case duckdb_libpgquery::PG_OBJECT_DATABASE:
	default:
		throw NotImplementedException("Schema element not supported yet!");
	}
	D_ASSERT(info);

	auto result = make_uniq<AlterStatement>();
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
