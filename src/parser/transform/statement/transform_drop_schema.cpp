#include "parser/statement/drop_schema_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<DropSchemaStatement> Transformer::TransformDropSchema(DropStmt *stmt) {
	auto result = make_unique<DropSchemaStatement>();
	auto &info = *result->info.get();

	info.cascade = stmt->behavior == DropBehavior::DROP_CASCADE;
	info.if_exists = stmt->missing_ok;
	if (!stmt->objects || stmt->objects->length != 1) {
		throw ParserException("DROP SCHEMA needs a single string parameter");
	}
	info.schema = string(((postgres::Value*) stmt->objects->head->data.ptr_value)->val.str);
	return result;
}
