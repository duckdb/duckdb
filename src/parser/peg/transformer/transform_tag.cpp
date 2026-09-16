#include "duckdb/parser/parsed_data/set_tags_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformTagStatement(PEGTransformer &transformer,
                                                                      const CatalogType &tag_on_type,
                                                                      const vector<string> &dotted_identifier,
                                                                      TagActionInfo tag_action_info) {
	auto result = make_uniq<AlterStatement>();
	Identifier column_name;
	QualifiedName qualified_name;
	if (tag_on_type == CatalogType::INVALID) {
		auto identifier = dotted_identifier;
		column_name = Identifier(identifier.back());
		identifier.pop_back();
		if (identifier.empty()) {
			throw ParserException("Invalid column reference: %s", SQLIdentifier(column_name));
		}
		qualified_name = StringToQualifiedName(identifier);
	} else {
		qualified_name = StringToQualifiedName(dotted_identifier);
	}
	result->info = make_uniq<SetTagsInfo>(tag_on_type, std::move(qualified_name), std::move(column_name),
	                                      std::move(tag_action_info), OnEntryNotFound::THROW_EXCEPTION);
	return std::move(result);
}

TagActionInfo PEGTransformerFactory::TransformTagSetAction(PEGTransformer &transformer,
                                                           vector<pair<string, string>> tag_assignment_list) {
	TagActionInfo result;
	result.action = TagAction::SET;
	for (auto &assignment : tag_assignment_list) {
		if (result.tags.contains(assignment.first)) {
			throw ParserException("Tag %s should be specified at most once", SQLString(assignment.first));
		}
		result.tags.insert(std::move(assignment));
	}
	return result;
}

TagActionInfo PEGTransformerFactory::TransformTagUnsetAction(PEGTransformer &transformer,
                                                             vector<string> tag_name_list) {
	TagActionInfo result;
	result.action = TagAction::UNSET;
	case_insensitive_set_t unique_names;
	for (const auto &tag_name : tag_name_list) {
		if (!unique_names.insert(tag_name).second) {
			throw ParserException("Tag %s should be specified at most once", SQLString(tag_name));
		}
	}
	result.tag_names = std::move(tag_name_list);
	return result;
}

pair<string, string> PEGTransformerFactory::TransformTagAssignment(PEGTransformer &transformer,
                                                                   const string &string_literal,
                                                                   const string &string_literal_1) {
	return make_pair(string_literal, string_literal_1);
}

vector<pair<string, string>>
PEGTransformerFactory::TransformTagAssignmentList(PEGTransformer &transformer,
                                                  vector<pair<string, string>> tag_assignment) {
	return tag_assignment;
}

vector<string> PEGTransformerFactory::TransformTagNameList(PEGTransformer &transformer,
                                                           const vector<string> &string_literal) {
	return string_literal;
}

} // namespace duckdb
