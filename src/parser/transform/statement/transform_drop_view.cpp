#include "parser/statement/drop_view_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<DropViewStatement> Transformer::TransformDropView(DropStmt *stmt) {
	auto result = make_unique<DropViewStatement>();
	auto &info = *result->info.get();
	auto view_list = reinterpret_cast<List *>(stmt->objects->head->data.ptr_value);
	info.view_name = reinterpret_cast<value *>(view_list->head->data.ptr_value)->val.str;

	// TODO IF EXISTS
	return result;
}
