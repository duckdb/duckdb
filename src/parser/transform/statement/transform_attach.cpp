#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<AttachStatement> Transformer::TransformAttach(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGAttachStmt *>(node);
	auto result = make_unique<AttachStatement>();
	auto info = make_unique<AttachInfo>();
	info->name = stmt->name ? stmt->name : string();
	info->path = stmt->path;

	if (stmt->options) {
		duckdb_libpgquery::PGListCell *cell = nullptr;
		for_each_cell(cell, stmt->options->head) {
			auto *def_elem = reinterpret_cast<duckdb_libpgquery::PGDefElem *>(cell->data.ptr_value);
			Value val;
			if (def_elem->arg) {
				val = TransformValue(*((duckdb_libpgquery::PGValue *)def_elem->arg))->value;
			} else {
				val = Value::BOOLEAN(true);
			}
			info->options[StringUtil::Lower(def_elem->defname)] = std::move(val);
		}
	}
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
