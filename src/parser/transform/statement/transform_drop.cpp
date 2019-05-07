#include "parser/statement/drop_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<SQLStatement> Transformer::TransformDrop(Node *node) {
	DropStmt *stmt = (DropStmt *)(node);
	auto result = make_unique<DropStatement>();
	auto &info = *result->info.get();
	assert(stmt);
	if (stmt->objects->length != 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	switch (stmt->removeType) {
	case OBJECT_TABLE:
		info.type = CatalogType::TABLE;
		break;
	case OBJECT_SCHEMA:
		info.type = CatalogType::SCHEMA;
		break;
	case OBJECT_INDEX:
		info.type = CatalogType::INDEX;
		break;
	case OBJECT_VIEW:
		info.type = CatalogType::VIEW;
		break;
	case OBJECT_SEQUENCE:
		info.type = CatalogType::SEQUENCE;
		break;
	default:
		throw NotImplementedException("Cannot drop this type yet");
	}

	switch (stmt->removeType) {
	case OBJECT_SCHEMA:
		assert(stmt->objects && stmt->objects->length == 1);
		info.name = ((postgres::Value *)stmt->objects->head->data.ptr_value)->val.str;
		break;
	default: {
		auto view_list = (List *)stmt->objects->head->data.ptr_value;
		if (view_list->length == 2) {
			info.schema = ((postgres::Value *)view_list->head->data.ptr_value)->val.str;
			info.name = ((postgres::Value *)view_list->head->next->data.ptr_value)->val.str;
		} else {
			info.name = ((postgres::Value *)view_list->head->data.ptr_value)->val.str;
		}
		break;
	}
	}
	info.cascade = stmt->behavior == DropBehavior::DROP_CASCADE;
	info.if_exists = stmt->missing_ok;
	return move(result);
}
