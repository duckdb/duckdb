
#include "parser/statement/drop_schema_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<DropSchemaStatement>
Transformer::TransformDropSchema(DropStmt *stmt) {
	auto result = make_unique<DropSchemaStatement>();
	auto &info = *result->info.get();

	info.cascade = stmt->behavior == DropBehavior::DROP_CASCADE;
	info.if_exists = stmt->missing_ok;
	for (auto cell = stmt->objects->head; cell != nullptr; cell = cell->next) {
		auto table_list = reinterpret_cast<List *>(cell->data.ptr_value);
		auto schema_value =
		    reinterpret_cast<value *>(table_list->head->data.ptr_value);
		info.schema = schema_value->val.str;
		break;
	}
	return result;
}