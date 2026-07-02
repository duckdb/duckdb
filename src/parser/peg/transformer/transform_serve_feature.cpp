#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/serve_feature_statement.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static optional_ptr<ParseResult> FindParseResultByName(ParseResult &parse_result, const string &name) {
	if (StringUtil::CIEquals(parse_result.name, name)) {
		return parse_result;
	}
	if (parse_result.type == ParseResultType::CHOICE) {
		return FindParseResultByName(parse_result.Cast<ChoiceParseResult>().GetResult(), name);
	}
	if (parse_result.type == ParseResultType::OPTIONAL) {
		auto &optional_pr = parse_result.Cast<OptionalParseResult>();
		return optional_pr.HasResult() ? FindParseResultByName(optional_pr.GetResult(), name) : nullptr;
	}
	if (parse_result.type == ParseResultType::REPEAT) {
		auto &repeat_pr = parse_result.Cast<RepeatParseResult>();
		for (auto &child : repeat_pr.GetChildren()) {
			auto result = FindParseResultByName(child.get(), name);
			if (result) {
				return result;
			}
		}
		return nullptr;
	}
	if (parse_result.type == ParseResultType::LIST) {
		auto &list_pr = parse_result.Cast<ListParseResult>();
		for (auto &child : list_pr.GetChildren()) {
			auto result = FindParseResultByName(child.get(), name);
			if (result) {
				return result;
			}
		}
	}
	return nullptr;
}

static void CollectParseResultsByName(ParseResult &parse_result, const string &name,
                                      vector<reference<ParseResult>> &results) {
	if (StringUtil::CIEquals(parse_result.name, name)) {
		results.push_back(parse_result);
		return;
	}
	if (parse_result.type == ParseResultType::CHOICE) {
		CollectParseResultsByName(parse_result.Cast<ChoiceParseResult>().GetResult(), name, results);
		return;
	}
	if (parse_result.type == ParseResultType::OPTIONAL) {
		auto &optional_pr = parse_result.Cast<OptionalParseResult>();
		if (optional_pr.HasResult()) {
			CollectParseResultsByName(optional_pr.GetResult(), name, results);
		}
		return;
	}
	if (parse_result.type == ParseResultType::REPEAT) {
		auto &repeat_pr = parse_result.Cast<RepeatParseResult>();
		for (auto &child : repeat_pr.GetChildren()) {
			CollectParseResultsByName(child.get(), name, results);
		}
		return;
	}
	if (parse_result.type == ParseResultType::LIST) {
		auto &list_pr = parse_result.Cast<ListParseResult>();
		for (auto &child : list_pr.GetChildren()) {
			CollectParseResultsByName(child.get(), name, results);
		}
	}
}

static optional_ptr<ParseResult> FindFirstIdentifierOrString(ParseResult &parse_result) {
	if (parse_result.type == ParseResultType::IDENTIFIER || parse_result.type == ParseResultType::STRING) {
		return parse_result;
	}
	if (parse_result.type == ParseResultType::CHOICE) {
		return FindFirstIdentifierOrString(parse_result.Cast<ChoiceParseResult>().GetResult());
	}
	if (parse_result.type == ParseResultType::OPTIONAL) {
		auto &optional_pr = parse_result.Cast<OptionalParseResult>();
		return optional_pr.HasResult() ? FindFirstIdentifierOrString(optional_pr.GetResult()) : nullptr;
	}
	if (parse_result.type == ParseResultType::REPEAT) {
		auto &repeat_pr = parse_result.Cast<RepeatParseResult>();
		for (auto &child : repeat_pr.GetChildren()) {
			auto result = FindFirstIdentifierOrString(child.get());
			if (result) {
				return result;
			}
		}
		return nullptr;
	}
	if (parse_result.type == ParseResultType::LIST) {
		auto &list_pr = parse_result.Cast<ListParseResult>();
		for (auto &child : list_pr.GetChildren()) {
			auto result = FindFirstIdentifierOrString(child.get());
			if (result) {
				return result;
			}
		}
	}
	return nullptr;
}

static void CollectIdentifierOrStringNames(ParseResult &parse_result, vector<string> &result) {
	if (parse_result.type == ParseResultType::IDENTIFIER) {
		result.push_back(parse_result.Cast<IdentifierParseResult>().identifier);
		return;
	}
	if (parse_result.type == ParseResultType::STRING) {
		result.push_back(parse_result.Cast<StringLiteralParseResult>().result);
		return;
	}
	if (parse_result.type == ParseResultType::CHOICE) {
		CollectIdentifierOrStringNames(parse_result.Cast<ChoiceParseResult>().GetResult(), result);
		return;
	}
	if (parse_result.type == ParseResultType::OPTIONAL) {
		auto &optional_pr = parse_result.Cast<OptionalParseResult>();
		if (optional_pr.HasResult()) {
			CollectIdentifierOrStringNames(optional_pr.GetResult(), result);
		}
		return;
	}
	if (parse_result.type == ParseResultType::REPEAT) {
		auto &repeat_pr = parse_result.Cast<RepeatParseResult>();
		for (auto &child : repeat_pr.GetChildren()) {
			CollectIdentifierOrStringNames(child.get(), result);
		}
		return;
	}
	if (parse_result.type == ParseResultType::LIST) {
		auto &list_pr = parse_result.Cast<ListParseResult>();
		for (auto &child : list_pr.GetChildren()) {
			CollectIdentifierOrStringNames(child.get(), result);
		}
	}
}

static string ExtractFirstIdentifierName(ParseResult &parse_result, const string &context) {
	auto result = FindFirstIdentifierOrString(parse_result);
	if (!result) {
		throw InternalException("Expected identifier in %s", context);
	}
	if (result->type == ParseResultType::IDENTIFIER) {
		return result->Cast<IdentifierParseResult>().identifier;
	}
	return result->Cast<StringLiteralParseResult>().result;
}

static FeatureServeEntityMapping TransformServeFeatureEntityMapping(ParseResult &parse_result) {
	auto mapping_pair = FindParseResultByName(parse_result, "ServeFeatureEntityMappingPair");
	auto &mapping_source = mapping_pair ? *mapping_pair : parse_result;
	vector<string> identifiers;
	CollectIdentifierOrStringNames(mapping_source, identifiers);
	if (identifiers.empty()) {
		throw InternalException("Expected identifier in SERVE FEATURE entity mapping");
	}
	FeatureServeEntityMapping mapping;
	mapping.feature_column = identifiers[0];
	mapping.spine_column = identifiers.size() > 1 ? identifiers[1] : mapping.feature_column;
	return mapping;
}

static vector<FeatureServeEntityMapping> TransformServeFeatureEntityMappings(ParseResult &mapping_clause) {
	vector<FeatureServeEntityMapping> mappings;
	auto mapping_list_result = FindParseResultByName(mapping_clause, "ServeFeatureEntityList");
	if (!mapping_list_result) {
		FeatureServeEntityMapping mapping;
		mapping.spine_column = ExtractFirstIdentifierName(mapping_clause, "SERVE FEATURE entity mapping");
		mappings.push_back(std::move(mapping));
		return mappings;
	}

	vector<reference<ParseResult>> mapping_items;
	CollectParseResultsByName(*mapping_list_result, "ServeFeatureEntityMapping", mapping_items);
	for (auto &mapping_item : mapping_items) {
		mappings.push_back(TransformServeFeatureEntityMapping(mapping_item.get()));
	}
	return mappings;
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformServeFeatureStatement(PEGTransformer &transformer,
                                                                               ParseResult &parse_result) {
	// ServeFeatureStatement <- 'SERVE' ServeFeatureKw List(ServeFeatureItem) 'FOR' IdentifierOrStringLiteral
	// ServeFeatureEntity? ServeFeatureAsOf?
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto feature_items = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	vector<ServeFeatureRequest> features;
	features.reserve(feature_items.size());
	for (auto &item : feature_items) {
		auto &feature_item = item.get().Cast<ListParseResult>();
		ServeFeatureRequest request;
		request.feature_name = ExtractFirstIdentifierName(feature_item.Child<ListParseResult>(0), "SERVE FEATURE item");

		auto &mapping_opt = feature_item.Child<OptionalParseResult>(1);
		if (mapping_opt.HasResult()) {
			request.entity_mappings = TransformServeFeatureEntityMappings(mapping_opt.GetResult());
		}
		features.push_back(std::move(request));
	}

	auto spine_table = ExtractFirstIdentifierName(list_pr.Child<ListParseResult>(4), "SERVE FEATURE spine table");

	string spine_entity_override;
	auto &entity_opt = list_pr.Child<OptionalParseResult>(5);
	if (entity_opt.HasResult()) {
		spine_entity_override = ExtractFirstIdentifierName(entity_opt.GetResult(), "SERVE FEATURE ENTITY clause");
	}

	string spine_asof_column;
	auto &asof_opt = list_pr.Child<OptionalParseResult>(6);
	if (asof_opt.HasResult()) {
		spine_asof_column = ExtractFirstIdentifierName(asof_opt.GetResult(), "SERVE FEATURE ASOF clause");
	}

	auto result = make_uniq<ServeFeatureStatement>();
	result->features = std::move(features);
	result->spine_table = std::move(spine_table);
	result->spine_entity_override = std::move(spine_entity_override);
	result->spine_asof_column = std::move(spine_asof_column);
	return std::move(result);
}

} // namespace duckdb
