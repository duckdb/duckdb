#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

unique_ptr<CreateStatement> Transformer::TransformCreateAlias(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateAliasStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTypeInfo>();
	info->name = ReadPgListToString(stmt->aliasname)[0];
	LogicalType target_type = TransformTypeName(stmt->typeName);
	LogicalType::SetAlias(target_type, info->name);
	info->type = target_type;
	result->info = move(info);
	return result;
}
} // namespace duckdb