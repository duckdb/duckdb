#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"

namespace duckdb {

unique_ptr<PragmaStatement> Transformer::TransformSecret(duckdb_libpgquery::PGCreateSecretStmt &stmt) {
	printf("%s\n", stmt.secret_name);
	printf("%s\n", stmt.secret_type);
	if (!stmt.options) {
		// no options
		return nullptr;
	}
	for (auto cell = stmt.options->head; cell; cell = cell->next) {
		auto option_list = PGPointerCast<duckdb_libpgquery::PGList>(cell->data.ptr_value);
		D_ASSERT(option_list->length == 2);
		auto head = option_list->head;
		auto tail = option_list->tail;
		printf("%s = %s\n", (char *) head->data.ptr_value, (char *) tail->data.ptr_value);
	}

	throw InternalException("FIXME: transform secret statement");
}

} // namespace duckdb
