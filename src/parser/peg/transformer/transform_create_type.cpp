#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTypeStmt(PEGTransformer &transformer,
                                                                           const optional<bool> &if_not_exists,
                                                                           const QualifiedName &qualified_name,
                                                                           unique_ptr<CreateTypeInfo> create_type) {
	auto result = make_uniq<CreateStatement>();
	create_type->CatalogMutable() = qualified_name.Catalog();
	create_type->SchemaMutable() = qualified_name.Schema();
	create_type->SetTypeName(qualified_name.Name());
	create_type->on_conflict =
	    if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	result->info = std::move(create_type);
	return result;
}

unique_ptr<CreateTypeInfo> PEGTransformerFactory::TransformCreateTypeFromType(PEGTransformer &transformer,
                                                                              const LogicalType &type) {
	auto result = make_uniq<CreateTypeInfo>();
	result->type = type;
	return result;
}

unique_ptr<CreateTypeInfo>
PEGTransformerFactory::TransformEnumSelectType(PEGTransformer &transformer,
                                               unique_ptr<SelectStatement> select_statement_internal) {
	auto result = make_uniq<CreateTypeInfo>();
	result->query = std::move(select_statement_internal);
	result->type = LogicalType::INVALID;
	return result;
}

unique_ptr<CreateTypeInfo>
PEGTransformerFactory::TransformEnumStringLiteralList(PEGTransformer &transformer,
                                                      const optional<vector<string>> &string_literal) {
	auto result = make_uniq<CreateTypeInfo>();
	idx_t enum_count = string_literal ? string_literal->size() : 0;
	Vector enum_vector(LogicalType::VARCHAR, enum_count);
	auto string_data = FlatVector::Writer<string_t>(enum_vector, enum_count);
	if (string_literal) {
		for (auto &literal : *string_literal) {
			string_data.WriteValue(string_t(literal));
		}
	}
	result->type = LogicalType::ENUM(enum_vector, enum_count);
	return result;
}

} // namespace duckdb
