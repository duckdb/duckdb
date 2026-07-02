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
	create_type->SetQualifiedName(qualified_name);
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

void PEGTransformerFactory::InitializeEnumStringLiteralListTrampoline(PEGTransformer &transformer,
                                                                      TransformStack &stack,
                                                                      TransformStackFrame &frame) {
	frame.ReserveChildSlots(0);
}

unique_ptr<TransformResultValue>
PEGTransformerFactory::FinalizeEnumStringLiteralListTrampoline(PEGTransformer &transformer, TransformStack &stack,
                                                               TransformStackFrame &frame) {
	auto &list_pr = frame.parse_result.Cast<ListParseResult>();
	optional<vector<string>> string_literal {};
	auto &string_literal_opt = ExtractResultFromParens(list_pr.GetChild(1)).Cast<OptionalParseResult>();
	if (string_literal_opt.HasResult()) {
		vector<string> string_literal_value;
		auto string_literal_items = ExtractParseResultsFromList(string_literal_opt.GetResult());
		for (auto &string_literal_item : string_literal_items) {
			string_literal_value.push_back(TransformStringLiteral(transformer, string_literal_item.get()));
		}
		string_literal = string_literal_value;
	}
	auto result = TransformEnumStringLiteralList(transformer, string_literal);
	return make_uniq<TypedTransformResult<unique_ptr<CreateTypeInfo>>>(std::move(result));
}

} // namespace duckdb
