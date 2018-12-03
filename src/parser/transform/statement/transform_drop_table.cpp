
#include "parser/statement/drop_table_statement.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<DropTableStatement> Transformer::TransformDropTable(DropStmt *stmt) {
	auto result = make_unique<DropTableStatement>();
	auto &info = *result->info.get();

	info.cascade = stmt->behavior == DropBehavior::DROP_CASCADE;
	auto table_list =
	    reinterpret_cast<List *>(stmt->objects->head->data.ptr_value);
	if (table_list->length == 2) {
		info.schema =
		    reinterpret_cast<value *>(table_list->head->data.ptr_value)
		        ->val.str;
		info.table =
		    reinterpret_cast<value *>(table_list->head->next->data.ptr_value)
		        ->val.str;
	} else {
		info.table = reinterpret_cast<value *>(table_list->head->data.ptr_value)
		                 ->val.str;
	}
	info.if_exists = stmt->missing_ok;
	return result;
}