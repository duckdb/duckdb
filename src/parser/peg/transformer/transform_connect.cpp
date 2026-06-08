#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/connect_statement.hpp"
#include "duckdb/parser/statement/disconnect_statement.hpp"

namespace duckdb {

//! Shape captured from `SessionTarget <- 'LOCAL' / StringLiteral / CatalogName`. The framework
//! wraps the named sub-rule in a List whose only child is the Choice over the alternatives.
struct SessionTargetCapture {
	string name;
	bool target_is_local = false;
	bool name_is_string_literal = false;
};

static SessionTargetCapture TransformSessionTarget(PEGTransformer &transformer, ParseResult &target_result) {
	auto &list = target_result.Cast<ListParseResult>();
	auto &inner = list.Child<ChoiceParseResult>(0).GetResult();
	SessionTargetCapture result;
	switch (inner.type) {
	case ParseResultType::KEYWORD:
		// 'LOCAL' alternative — name stays empty, just flip the flag.
		result.target_is_local = true;
		break;
	case ParseResultType::STRING:
		result.name = inner.Cast<StringLiteralParseResult>().GetRawString();
		result.name_is_string_literal = true;
		break;
	case ParseResultType::IDENTIFIER:
		result.name = inner.Cast<IdentifierParseResult>().identifier.GetIdentifierName();
		break;
	default:
		throw InternalException("Unexpected SessionTarget alternative type: %s", ParseResultToString(inner.type));
	}
	return result;
}

// ConnectStatement <- 'CONNECT' SessionTarget?
unique_ptr<SQLStatement> PEGTransformerFactory::TransformConnectStatement(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto info = make_uniq<ConnectInfo>();
	auto &target_opt = list_pr.Child<OptionalParseResult>(1);
	if (target_opt.HasResult()) {
		auto captured = TransformSessionTarget(transformer, target_opt.GetResult());
		info->name = Identifier(std::move(captured.name));
		info->target_is_local = captured.target_is_local;
		info->name_is_string_literal = captured.name_is_string_literal;
	}
	auto result = make_uniq<ConnectStatement>();
	result->info = std::move(info);
	return std::move(result);
}

// DisconnectStatement <- 'DISCONNECT'
unique_ptr<SQLStatement> PEGTransformerFactory::TransformDisconnectStatement(PEGTransformer &transformer) {
	auto result = make_uniq<DisconnectStatement>();
	result->info = make_uniq<DisconnectInfo>();
	return std::move(result);
}

} // namespace duckdb
