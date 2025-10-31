#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

namespace duckdb {

// IdentifierOrStringLiteral <- Identifier / StringLiteral
QualifiedName PEGTransformerFactory::TransformIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = INVALID_SCHEMA;
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		result.name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
	}
	if (choice_pr.result->type == ParseResultType::STRING) {
		result.name = choice_pr.result->Cast<StringLiteralParseResult>().result;
	}
	return result;
}

string PEGTransformerFactory::TransformColIdOrString(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<string>(choice_pr.result);
}

string PEGTransformerFactory::TransformColLabelOrString(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::STRING) {
		return choice_pr.result->Cast<StringLiteralParseResult>().result;
	}
	return transformer.Transform<string>(choice_pr.result);
}

string PEGTransformerFactory::TransformColId(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		return choice_pr.result->Cast<IdentifierParseResult>().identifier;
	}
	return transformer.Transform<string>(choice_pr.result);
}

string PEGTransformerFactory::TransformIdentifier(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

vector<string> PEGTransformerFactory::TransformDottedIdentifier(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> parts;

	parts.push_back(list_pr.Child<IdentifierParseResult>(0).identifier);

	auto &optional_elements = list_pr.Child<OptionalParseResult>(1);
	if (optional_elements.HasResult()) {
		auto repeat_elements = optional_elements.optional_result->Cast<RepeatParseResult>();
		for (auto &child_ref : repeat_elements.children) {
			auto &sub_list = child_ref->Cast<ListParseResult>();
			parts.push_back(sub_list.Child<IdentifierParseResult>(1).identifier);
		}
	}
	return parts;
}

ColumnDefinition PEGTransformerFactory::TransformColumnDefinition(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto dotted_identifier = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(0));
	auto qualified_name = StringToQualifiedName(dotted_identifier);
	auto type = transformer.Transform<LogicalType>(list_pr.Child<ListParseResult>(1));

	// TODO(Dtenwolde) Deal with ColumnConstraint
	auto result = ColumnDefinition(qualified_name.name, type);
	return result;
}

LogicalType PEGTransformerFactory::TransformTypeOrGenerated(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto type_pr = list_pr.Child<OptionalParseResult>(0);
	auto generated_column_pr = list_pr.Child<OptionalParseResult>(1);
	if (generated_column_pr.HasResult()) {
		throw NotImplementedException("Generated columns have not yet been implemented.");
	}
	LogicalType type = LogicalType::INVALID;
	if (type_pr.HasResult()) {
		type = transformer.Transform<LogicalType>(type_pr.optional_result);
	}
	// TODO(Dtenwolde) deal with generated columns
	return type;
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopLevelConstraint(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	// TODO(dtenwolde) figure out what to do with constraint name.
	auto opt_constraint_name = list_pr.Child<OptionalParseResult>(0);
	auto result = transformer.Transform<unique_ptr<Constraint>>(list_pr.Child<ListParseResult>(1));
	return result;
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopLevelConstraintList(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<Constraint>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopPrimaryKeyConstraint(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_list = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(2));
	auto result = make_uniq<UniqueConstraint>(column_list, true);
	return result;
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopUniqueConstraint(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_list = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(1));
	auto result = make_uniq<UniqueConstraint>(column_list, false);
	return result;
}

unique_ptr<Constraint> PEGTransformerFactory::TransformCheckConstraint(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto check_expr = transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens);
	auto result = make_uniq<CheckConstraint>(std::move(check_expr));
	return result;
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopForeignKeyConstraint(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto fk_list = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(2));

	auto table_name = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(4));
	auto opt_pk_list = list_pr.Child<OptionalParseResult>(5);
	vector<string> pk_list;
	if (opt_pk_list.HasResult()) {
		pk_list = transformer.Transform<vector<string>>(opt_pk_list.optional_result);
	}
	auto key_actions = list_pr.Child<OptionalParseResult>(6);
	// TODO(Dtenwolde) do something with key actions

	ForeignKeyInfo fk_info;
	fk_info.table = table_name->table_name;
	fk_info.schema = table_name->schema_name;
	// TODO(Dtenwolde) unsure about the fk type or how to figure this out.
	fk_info.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	return make_uniq<ForeignKeyConstraint>(pk_list, fk_list, fk_info);
}

vector<string> PEGTransformerFactory::TransformColumnIdList(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> result;
	auto colid_list = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto colids = ExtractParseResultsFromList(colid_list);
	for (auto colid : colids) {
		result.push_back(transformer.Transform<string>(colid));
	}
	return result;
}

} // namespace duckdb
