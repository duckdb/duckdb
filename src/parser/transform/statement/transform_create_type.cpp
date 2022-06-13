#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

vector<string> ReadPgListToString(duckdb_libpgquery::PGList *column_list) {
	vector<string> result;
	if (!column_list) {
		return result;
	}
	for (auto c = column_list->head; c != nullptr; c = lnext(c)) {
		auto target = (duckdb_libpgquery::PGResTarget *)(c->data.ptr_value);
		result.emplace_back(target->name);
	}
	return result;
}

Vector ReadPgListToVector(duckdb_libpgquery::PGList *column_list, idx_t &size) {
	if (!column_list) {
		Vector result(LogicalType::VARCHAR);
		return result;
	}
	// First we discover the size of this list
	for (auto c = column_list->head; c != nullptr; c = lnext(c)) {
		size++;
	}

	Vector result(LogicalType::VARCHAR, size);
	auto result_ptr = FlatVector::GetData<string_t>(result);

	size = 0;
	for (auto c = column_list->head; c != nullptr; c = lnext(c)) {
		auto target = (duckdb_libpgquery::PGResTarget *)(c->data.ptr_value);
		result_ptr[size++] = StringVector::AddStringOrBlob(result, target->name);
	}
	return result;
}

unique_ptr<CreateStatement> Transformer::TransformCreateType(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateTypeStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTypeInfo>();
	info->name = ReadPgListToString(stmt->typeName)[0];
	switch (stmt->kind) {
	case duckdb_libpgquery::PG_NEWTYPE_ENUM: {
		info->internal = false;
		idx_t size = 0;
		auto ordered_array = ReadPgListToVector(stmt->vals, size);
		info->type = LogicalType::ENUM(info->name, ordered_array, size);
	} break;

	case duckdb_libpgquery::PG_NEWTYPE_ALIAS: {
		LogicalType target_type = TransformTypeName(stmt->ofType);
		target_type.SetAlias(info->name);
		info->type = target_type;
	} break;

	default:
		throw InternalException("Unknown kind of new type");
	}

	result->info = move(info);
	return result;
}
} // namespace duckdb