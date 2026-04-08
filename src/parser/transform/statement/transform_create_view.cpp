#include <string>
#include <utility>

#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "nodes/nodes.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/pg_list.hpp"
#include "nodes/value.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateView(duckdb_libpgquery::PGViewStmt &stmt) {
	D_ASSERT(stmt.type == duckdb_libpgquery::T_PGViewStmt);
	D_ASSERT(stmt.view);

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateViewInfo>();

	auto qname = TransformQualifiedName(*stmt.view);
	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->view_name = qname.name;
	info->temporary = !stmt.view->relpersistence;
	if (info->temporary && IsInvalidCatalog(info->catalog)) {
		info->catalog = TEMP_CATALOG;
	}
	info->on_conflict = TransformOnConflict(stmt.onconflict);

	info->query = TransformSelectStmt(*stmt.query, false);

	PivotEntryCheck("view");

	if (stmt.aliases && stmt.aliases->length > 0) {
		for (auto c = stmt.aliases->head; c != nullptr; c = lnext(c)) {
			auto val = PGPointerCast<duckdb_libpgquery::PGValue>(c->data.ptr_value);
			switch (val->type) {
			case duckdb_libpgquery::T_PGString: {
				info->aliases.emplace_back(val->val.str);
				break;
			}
			default:
				throw NotImplementedException("View projection type");
			}
		}
		if (info->aliases.empty()) {
			throw ParserException("Need at least one column name in CREATE VIEW projection list");
		}
	}

	if (stmt.options && stmt.options->length > 0) {
		throw NotImplementedException("VIEW options");
	}

	if (stmt.withCheckOption != duckdb_libpgquery::PGViewCheckOption::PG_NO_CHECK_OPTION) {
		throw NotImplementedException("VIEW CHECK options");
	}
	result->info = std::move(info);
	return result;
}

} // namespace duckdb
