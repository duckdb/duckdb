#include "duckdb/parser/parsed_data/create_custom_type_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/pair.hpp"
#include <iostream>

namespace duckdb {

map<CustomTypeParameterId, string> ReadPgListToParameterMap(duckdb_libpgquery::PGList *vals) {
	map<CustomTypeParameterId, string> result;
	if (!vals) {
		return result;
	}

	for (auto node = vals->head; node != nullptr; node = node->next) {
		auto target = reinterpret_cast<duckdb_libpgquery::PGCustomTypeArgExpr *>(node->data.ptr_value);
		auto name = target->name;
		auto parameter_id = TransformStringToCustomTypeParameter(name);
		auto func_name = target->func_name;
		result.insert(pair<CustomTypeParameterId, string>(parameter_id, func_name));
	}

	return result;
}

unique_ptr<CreateStatement> Transformer::TransformCreateType(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateTypeStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateCustomTypeInfo>();
	auto name = ReadPgListToString(stmt->name)[0];
	info->name = name;
	map<CustomTypeParameterId, string> parameters;
	if (stmt->vals) {
		parameters = ReadPgListToParameterMap(stmt->vals);
	}
	info->type = LogicalType::CUSTOM(info->name, parameters);
	result->info = move(info);
	return result;
}
} // namespace duckdb