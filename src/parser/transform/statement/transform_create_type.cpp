#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

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
		auto &type_val = *((duckdb_libpgquery::PGAConst *)c->data.ptr_value);
		auto entry_value_node = (duckdb_libpgquery::PGValue)(type_val.val);
		if (entry_value_node.type != duckdb_libpgquery::T_PGString) {
			throw ParserException("Expected a string constant as value");
		}

		auto entry_value = string(entry_value_node.val.str);
		D_ASSERT(!entry_value.empty());
		result_ptr[size++] = StringVector::AddStringOrBlob(result, entry_value);
	}
	return result;
}

unique_ptr<CreateStatement> Transformer::TransformCreateType(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateTypeStmt *>(node);
	D_ASSERT(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateTypeInfo>();

	auto qualified_name = TransformQualifiedName(stmt->typeName);
	info->catalog = qualified_name.catalog;
	info->schema = qualified_name.schema;
	info->name = qualified_name.name;

	switch (stmt->kind) {
	case duckdb_libpgquery::PG_NEWTYPE_ENUM: {
		info->internal = false;
		if (stmt->query) {
			// CREATE TYPE mood AS ENUM (SELECT ...)
			D_ASSERT(stmt->vals == nullptr);
			auto query = TransformSelect(stmt->query, false);
			info->query = move(query);
			info->type = LogicalType::INVALID;
		} else {
			D_ASSERT(stmt->query == nullptr);
			idx_t size = 0;
			auto ordered_array = ReadPgListToVector(stmt->vals, size);
			info->type = LogicalType::ENUM(info->name, ordered_array, size);
		}
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
