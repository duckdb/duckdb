#include "catch.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"

using namespace duckdb;

static string NestedFunctionExpression(idx_t depth) {
	string result;
	for (idx_t i = 0; i < depth; i++) {
		result += "abs(";
	}
	return result + "1" + string(depth, ')');
}

TEST_CASE("Parser enforces depth during heap transformation", "[parser][parser_depth]") {
	ParserOptions options;
	options.max_expression_depth = 100;
	Parser parser(options);
	REQUIRE_NOTHROW(parser.ParseQuery("SELECT abs(1)"));
	REQUIRE_THROWS_WITH(parser.ParseQuery("SELECT " + NestedFunctionExpression(100)),
	                    Catch::Contains("Max expression depth limit of 100 exceeded"));
	REQUIRE_NOTHROW(parser.ParseQuery("SELECT abs(1)"));

	options.max_expression_depth = 10000;
	Parser deeper_parser(options);
	REQUIRE_NOTHROW(deeper_parser.ParseQuery("SELECT " + NestedFunctionExpression(100)));
}

TEST_CASE("Parser rejects deep grouping expressions before hashing", "[parser][parser_depth]") {
	Parser parser;
	REQUIRE_THROWS_WITH(parser.ParseQuery("SELECT 1 GROUP BY " + NestedFunctionExpression(20000)),
	                    Catch::Contains("Max expression depth limit of 1000 exceeded"));
	REQUIRE_NOTHROW(parser.ParseQuery("SELECT 1 GROUP BY abs(1)"));
}

TEST_CASE("Transformer depth counts active frames rather than completed siblings", "[parser][parser_depth]") {
	ParserOptions options;
	options.max_expression_depth = 100;
	Parser parser(options);
	string query = "SELECT abs(1)";
	for (idx_t i = 0; i < 1000; i++) {
		query += ", abs(1)";
	}
	REQUIRE_NOTHROW(parser.ParseQuery(query));
}

TEST_CASE("Forwarding grammar layers do not consume expression depth", "[parser][parser_depth]") {
	ParserOptions options;
	options.max_expression_depth = 100;
	Parser parser(options);
	REQUIRE_NOTHROW(parser.ParseQuery("SELECT " + string(1000, '(') + "1" + string(1000, ')')));
	string addition = "1";
	for (idx_t i = 0; i < 200; i++) {
		addition = "(1 + " + addition + ")";
	}
	REQUIRE_THROWS_WITH(parser.ParseQuery("SELECT " + addition),
	                    Catch::Contains("Max expression depth limit of 100 exceeded"));
}

class DepthTestTransformProcess final : public TransformProcess {
public:
	DepthTestTransformProcess(PEGTransformer &transformer_p, ParseResult &input_p, bool reentrant_p)
	    : transformer(transformer_p), input(input_p.Cast<ListParseResult>()), reentrant(reentrant_p) {
	}

	TransformStep Resume(unique_ptr<TransformResultValue> child_result) override {
		if (child_result) {
			return TransformStep::Complete(std::move(child_result));
		}
		if (input.GetChildren().empty()) {
			return TransformStep::Complete(make_uniq<TypedTransformResult<bool>>(true));
		}
		auto &child = input.GetChild(0);
		if (reentrant) {
			return TransformStep::Complete(make_uniq<TypedTransformResult<bool>>(transformer.Transform<bool>(child)));
		}
		return TransformStep::Child({*child.GetRule(), child});
	}

private:
	PEGTransformer &transformer;
	ListParseResult &input;
	bool reentrant;
};

TEST_CASE("Transformer frame accounting unwinds and includes reentrant transforms", "[parser][parser_depth]") {
	auto grammar = CompiledGrammar::Create();
	ArenaAllocator allocator(Allocator::DefaultAllocator());
	vector<MatcherToken> tokens;
	TokenIterator iterator(tokens);
	ParserOptions options;
	options.max_expression_depth = 4;
	PEGTransformer transformer(allocator, iterator, options, *grammar);

	for (idx_t mode = 0; mode < 3; mode++) {
		INFO("reentrancy mode: " << mode);
		CompiledGrammarRule rule(
		    "Expression", [mode](PEGTransformer &transformer, ParseResult &input) -> unique_ptr<TransformProcess> {
			    if (mode == 2) {
				    auto &list = input.Cast<ListParseResult>();
				    if (!list.GetChildren().empty()) {
					    transformer.Transform<bool>(list.GetChild(0));
				    }
				    return make_uniq<FinalizeTransformProcess>(transformer, input, [](PEGTransformer &, ParseResult &) {
					    return make_uniq<TypedTransformResult<bool>>(true);
				    });
			    }
			    return make_uniq<DepthTestTransformProcess>(transformer, input, mode == 1);
		    });
		vector<unique_ptr<ListParseResult>> inputs;
		for (idx_t i = 0; i < 4; i++) {
			vector<reference<ParseResult>> children;
			if (!inputs.empty()) {
				children.push_back(*inputs.back());
			}
			auto input = make_uniq<ListParseResult>(std::move(children), "DepthTest", optional_idx());
			input->SetRule(rule);
			inputs.push_back(std::move(input));
		}
		REQUIRE(transformer.Transform<bool>(*inputs[2]));
		REQUIRE_THROWS_WITH(transformer.Transform<bool>(*inputs[3]),
		                    Catch::Contains("Max expression depth limit of 4 exceeded"));
		REQUIRE(transformer.Transform<bool>(*inputs[2]));
	}
}
