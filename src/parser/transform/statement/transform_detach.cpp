#include <string>
#include <utility>

#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/detach_statement.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<DetachStatement> Transformer::TransformDetach(duckdb_libpgquery::PGDetachStmt &stmt) {
	auto result = make_uniq<DetachStatement>();
	auto info = make_uniq<DetachInfo>();
	info->name = stmt.db_name;
	info->if_not_found = TransformOnEntryNotFound(stmt.missing_ok);

	result->info = std::move(info);
	return result;
}

} // namespace duckdb
