#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types.hpp"

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
	Vector result(LogicalType::VARCHAR);
	if (!column_list) {
		return result;
	}
	// First we discover the size of this list
	for (auto c = column_list->head; c != nullptr; c = lnext(c)) {
		size++;
	}
	if (size > STANDARD_VECTOR_SIZE) {
		result.Resize(STANDARD_VECTOR_SIZE, size);
	}
	size = 0;
	for (auto c = column_list->head; c != nullptr; c = lnext(c)) {
		auto target = (duckdb_libpgquery::PGResTarget *)(c->data.ptr_value);
		result.SetValue(size++, target->name);
	}
	return result;
}

unique_ptr<CreateStatement> Transformer::TransformCreateEnum(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateEnumStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTypeInfo>();
	info->name = ReadPgListToString(stmt->typeName)[0];
	idx_t size = 0;
	auto ordered_array = ReadPgListToVector(stmt->vals, size);
	info->type = make_unique<LogicalType>(LogicalType::ENUM(info->name, ordered_array, size));
	result->info = move(info);
	return result;
}
} // namespace duckdb