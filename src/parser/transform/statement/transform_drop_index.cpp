#include "parser/statement/drop_index_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<DropIndexStatement> Transformer::TransformDropIndex(DropStmt *stmt) {
	auto result = make_unique<DropIndexStatement>();
	auto &info = *result->info.get();

	auto index_list = reinterpret_cast<List *>(stmt->objects->head->data.ptr_value);

	info.name = reinterpret_cast<value *>(index_list->head->data.ptr_value)->val.str;

	info.if_exists = stmt->missing_ok;
	return result;
}
