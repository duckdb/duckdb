#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateSchema(duckdb_libpgquery::PGCreateSchemaStmt &stmt) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateSchemaInfo>();

	D_ASSERT(stmt.schemaname);
	info->catalog = stmt.catalogname ? stmt.catalogname : INVALID_CATALOG;
	info->schema = stmt.schemaname;
	info->on_conflict = TransformOnConflict(stmt.onconflict);

	if (stmt.schemaElts) {
		// schema elements
		for (auto cell = stmt.schemaElts->head; cell != nullptr; cell = cell->next) {
			auto node = PGPointerCast<duckdb_libpgquery::PGNode>(cell->data.ptr_value);
			switch (node->type) {
			case duckdb_libpgquery::T_PGCreateStmt:
			case duckdb_libpgquery::T_PGViewStmt:
			default:
				throw NotImplementedException("Schema element not supported yet!");
			}
		}
	}
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
