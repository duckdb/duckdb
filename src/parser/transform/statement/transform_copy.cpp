#include <string>
#include <unordered_map>
#include <utility>

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/pg_list.hpp"

namespace duckdb {

void Transformer::TransformCopyOptions(CopyInfo &info, optional_ptr<duckdb_libpgquery::PGList> options) {
	if (!options) {
		return;
	}

	duckdb_libpgquery::PGListCell *cell;
	// iterate over each option
	for_each_cell(cell, options->head) {
		auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
		string name = def_elem->defname;
		if (info.parsed_options.find(name) != info.parsed_options.end()) {
			throw ParserException("Unexpected duplicate option \"%s\"", name);
		}
		unique_ptr<ParsedExpression> expr;
		if (def_elem->arg) {
			expr = TransformExpression(*def_elem->arg);
		}
		info.parsed_options[name] = std::move(expr);
	}
}

unique_ptr<CopyStatement> Transformer::TransformCopy(duckdb_libpgquery::PGCopyStmt &stmt) {
	auto result = make_uniq<CopyStatement>();
	auto &info = *result->info;

	// get file_path and is_from
	info.is_from = stmt.is_from;

	info.file_path_expression = TransformExpression(*stmt.filename);
	if (info.file_path_expression->type == ExpressionType::VALUE_CONSTANT) {
		auto &constant_expr = info.file_path_expression->Cast<ConstantExpression>();
		if (constant_expr.value.type().id() == LogicalTypeId::VARCHAR) {
			info.file_path = StringValue::Get(constant_expr.value);
			info.file_path_expression.reset();
		}
	}

	// get select_list
	if (stmt.attlist) {
		for (auto n = stmt.attlist->head; n != nullptr; n = n->next) {
			auto target = PGPointerCast<duckdb_libpgquery::PGResTarget>(n->data.ptr_value);
			if (target->name) {
				info.select_list.emplace_back(target->name);
			}
		}
	}

	if (stmt.relation) {
		auto ref = TransformRangeVar(*stmt.relation);
		auto &table = ref->Cast<BaseTableRef>();
		info.table = table.table_name;
		info.schema = table.schema_name;
		info.catalog = table.catalog_name;
	} else {
		info.select_statement = TransformSelectNode(*stmt.query);
	}

	// handle the different options of the COPY statement
	TransformCopyOptions(info, stmt.options);

	return result;
}

} // namespace duckdb
