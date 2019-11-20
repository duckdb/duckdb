#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<SQLStatement> Transformer::TransformDrop(postgres::PGNode *node) {
	auto stmt = (postgres::PGDropStmt *)(node);
	auto result = make_unique<DropStatement>();
	auto &info = *result->info.get();
	assert(stmt);
	if (stmt->objects->length != 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	switch (stmt->removeType) {
	case postgres::PG_OBJECT_TABLE:
		info.type = CatalogType::TABLE;
		break;
	case postgres::PG_OBJECT_SCHEMA:
		info.type = CatalogType::SCHEMA;
		break;
	case postgres::PG_OBJECT_INDEX:
		info.type = CatalogType::INDEX;
		break;
	case postgres::PG_OBJECT_VIEW:
		info.type = CatalogType::VIEW;
		break;
	case postgres::PG_OBJECT_SEQUENCE:
		info.type = CatalogType::SEQUENCE;
		break;
	default:
		throw NotImplementedException("Cannot drop this type yet");
	}

	switch (stmt->removeType) {
	case postgres::PG_OBJECT_SCHEMA:
		assert(stmt->objects && stmt->objects->length == 1);
		info.name = ((postgres::PGValue *)stmt->objects->head->data.ptr_value)->val.str;
		break;
	default: {
		auto view_list = (postgres::PGList *)stmt->objects->head->data.ptr_value;
		if (view_list->length == 2) {
			info.schema = ((postgres::PGValue *)view_list->head->data.ptr_value)->val.str;
			info.name = ((postgres::PGValue *)view_list->head->next->data.ptr_value)->val.str;
		} else {
			info.name = ((postgres::PGValue *)view_list->head->data.ptr_value)->val.str;
		}
		break;
	}
	}
	info.cascade = stmt->behavior == postgres::PGDropBehavior::PG_DROP_CASCADE;
	info.if_exists = stmt->missing_ok;
	return move(result);
}
