#include "parser/statement/drop_sequence_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<DropSequenceStatement> Transformer::TransformDropSequence(DropStmt *stmt) {
	assert(stmt);
	auto result = make_unique<DropSequenceStatement>();
	auto &info = *result->info.get();
	auto view_list = reinterpret_cast<List *>(stmt->objects->head->data.ptr_value);

	if (view_list->length == 2) {
		info.schema = reinterpret_cast<postgres::Value *>(view_list->head->data.ptr_value)->val.str;
		info.name = reinterpret_cast<postgres::Value *>(view_list->head->next->data.ptr_value)->val.str;
	} else {
		info.name = reinterpret_cast<postgres::Value *>(view_list->head->data.ptr_value)->val.str;
	}

	info.if_exists = stmt->missing_ok;
	return result;
}
