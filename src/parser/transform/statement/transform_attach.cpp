#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<AttachStatement> Transformer::TransformAttach(duckdb_libpgquery::PGAttachStmt &stmt) {
	auto result = make_uniq<AttachStatement>();
	auto info = make_uniq<AttachInfo>();
	info->name = stmt.name ? stmt.name : string();
	info->path = stmt.path;
	info->on_conflict = TransformOnConflict(stmt.onconflict);

	if (stmt.options) {
		duckdb_libpgquery::PGListCell *cell;
		for_each_cell(cell, stmt.options->head) {
			auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
			unique_ptr<ParsedExpression> expr;
			if (def_elem->arg) {
				expr = TransformExpression(def_elem->arg);
			} else {
				expr = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
			}
			info->parsed_options[StringUtil::Lower(def_elem->defname)] = std::move(expr);
		}
	}
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
