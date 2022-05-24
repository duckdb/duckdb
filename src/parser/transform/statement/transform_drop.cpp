#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformDrop(duckdb_libpgquery::PGNode *node) {
	auto stmt = (duckdb_libpgquery::PGDropStmt *)(node);
	auto result = make_unique<DropStatement>();
	auto &info = *result->info.get();
	D_ASSERT(stmt);
	if (stmt->objects->length != 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	switch (stmt->removeType) {
	case duckdb_libpgquery::PG_OBJECT_TABLE:
		info.type = CatalogType::TABLE_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_SCHEMA:
		info.type = CatalogType::SCHEMA_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_INDEX:
		info.type = CatalogType::INDEX_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_VIEW:
		info.type = CatalogType::VIEW_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_MATVIEW:
		info.type = CatalogType::MATVIEW_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_SEQUENCE:
		info.type = CatalogType::SEQUENCE_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_FUNCTION:
		info.type = CatalogType::MACRO_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_TABLE_MACRO:
		info.type = CatalogType::TABLE_MACRO_ENTRY;
		break;
	case duckdb_libpgquery::PG_OBJECT_TYPE:
		info.type = CatalogType::TYPE_ENTRY;
		break;
	default:
		throw NotImplementedException("Cannot drop this type yet");
	}

	switch (stmt->removeType) {
	case duckdb_libpgquery::PG_OBJECT_SCHEMA:
		info.name = ((duckdb_libpgquery::PGValue *)stmt->objects->head->data.ptr_value)->val.str;
		break;
	case duckdb_libpgquery::PG_OBJECT_TYPE: {
		auto view_list = (duckdb_libpgquery::PGList *)stmt->objects;
		auto target = (duckdb_libpgquery::PGTypeName *)(view_list->head->data.ptr_value);
		info.name = (reinterpret_cast<duckdb_libpgquery::PGValue *>(target->names->tail->data.ptr_value)->val.str);
		break;
	}
	default: {
		auto view_list = (duckdb_libpgquery::PGList *)stmt->objects->head->data.ptr_value;
		if (view_list->length == 2) {
			info.schema = ((duckdb_libpgquery::PGValue *)view_list->head->data.ptr_value)->val.str;
			info.name = ((duckdb_libpgquery::PGValue *)view_list->head->next->data.ptr_value)->val.str;
		} else {
			info.name = ((duckdb_libpgquery::PGValue *)view_list->head->data.ptr_value)->val.str;
		}
		break;
	}
	}
	info.cascade = stmt->behavior == duckdb_libpgquery::PGDropBehavior::PG_DROP_CASCADE;
	info.if_exists = stmt->missing_ok;
	return move(result);
}

} // namespace duckdb
