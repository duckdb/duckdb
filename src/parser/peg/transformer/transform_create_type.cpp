#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTypeStmt(PEGTransformer &transformer,
                                                                           const bool &if_not_exists,
                                                                           const QualifiedName &qualified_name,
                                                                           unique_ptr<CreateTypeInfo> create_type) {
	auto result = make_uniq<CreateStatement>();
	create_type->catalog = qualified_name.catalog;
	create_type->schema = qualified_name.schema;
	create_type->name = qualified_name.name;
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

unique_ptr<CreateTypeInfo> PEGTransformerFactory::TransformEnumStringLiteralList(PEGTransformer &transformer,
                                                                                 const vector<string> &string_literal) {
	auto result = make_uniq<CreateTypeInfo>();
	Vector enum_vector(LogicalType::VARCHAR, string_literal.size());
	auto string_data = FlatVector::Writer<string_t>(enum_vector, string_literal.size());
	for (auto &literal : string_literal) {
		string_data.WriteValue(string_t(literal));
	}
	result->type = LogicalType::ENUM(enum_vector, string_literal.size());
	return result;
}

} // namespace duckdb
