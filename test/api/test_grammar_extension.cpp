#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher/identifier_matcher.hpp"
#include "duckdb/parser/peg/matcher/keyword_matcher.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"

using namespace duckdb;

static unique_ptr<TransformResultValue> TransformGrammarExtensionTestAtom(PEGTransformer &, ParseResult &) {
	auto statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<ConstantExpression>(Value::INTEGER(42)));
	select_node->from_table = make_uniq<EmptyTableRef>();
	statement->node = std::move(select_node);
	return make_uniq<TypedTransformResult<unique_ptr<SelectStatement>>>(std::move(statement));
}

class AddGrammarExtensionTestValue final : public GrammarExtension {
public:
	AddGrammarExtensionTestValue() : GrammarExtension("extension_test_value", "GrammarExtensionTestValue") {
	}

	void Apply(ParsedGrammar &grammar) const override {
		grammar.AddRule("GrammarExtensionTestValue <- 'WRONG'");
		grammar.AddChoice("UnreservedKeyword", "'ANSWER'");
		grammar.AddTerminalRuleOverride("GrammarExtensionTestValue", [](const PEGKeywordHelper &keyword_helper) {
			if (!keyword_helper.KeywordCategoryType("ANSWER", PEGKeywordCategory::KEYWORD_UNRESERVED)) {
				throw InternalException("Parser change keyword is missing from the compiled keyword helper");
			}
			return make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE, keyword_helper);
		});
	}
};

class AddGrammarExtensionTestAtom final : public GrammarExtension {
public:
	AddGrammarExtensionTestAtom() : GrammarExtension("extension_test_atom", "GrammarExtensionTestAtom") {
	}

	void Apply(ParsedGrammar &grammar) const override {
		grammar.AddRule("GrammarExtensionTestAtom <- GrammarExtensionTestValue", TransformGrammarExtensionTestAtom);
		grammar.PrependChoice("SelectAtom", "GrammarExtensionTestAtom", [](const PEGExpression &expression) {
			return expression.type == PEGExpression::Type::REFERENCE && expression.text.GetString() == "SelectParens";
		});
	}
};

static void RegisterGrammarExtensionTestSyntax(DatabaseInstance &db) {
	GrammarExtension::Register(db, make_shared_ptr<AddGrammarExtensionTestValue>());
	GrammarExtension::Register(db, make_shared_ptr<AddGrammarExtensionTestAtom>());
}

static void CheckGrammarExtensionTestSyntax(Connection &con) {
	auto result = con.Query("ANSWER");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(42));
}

TEST_CASE("Grammar extensions apply in registration order", "[api][grammar_extension]") {
	DuckDB db(nullptr);
	RegisterGrammarExtensionTestSyntax(*db.instance);
	Connection con(db);
	CheckGrammarExtensionTestSyntax(con);
}

TEST_CASE("Grammar choices support cursor placement", "[api][grammar_extension]") {
	auto grammar = ParsedGrammar::Parse("CursorRule <- 'first' / 'last'");
	grammar.AddChoice("CursorRule", "'second'", [](const PEGExpression &expression) {
		return expression.type == PEGExpression::Type::LITERAL && expression.text.GetString() == "first";
	});
	grammar.PrependChoice("CursorRule", "'third'", [](const PEGExpression &expression) {
		return expression.type == PEGExpression::Type::LITERAL && expression.text.GetString() == "last";
	});

	vector<string> choices;
	auto rule = grammar.GetRule("CursorRule");
	REQUIRE(rule);
	for (auto &expression : rule->recipe.expression.children) {
		if (expression.type == PEGExpression::Type::LITERAL) {
			choices.push_back(expression.text.GetString());
		}
	}
	REQUIRE(choices == vector<string> {"first", "second", "third", "last"});
}

TEST_CASE("Grammar choices can be removed", "[api][grammar_extension]") {
	auto grammar = ParsedGrammar::Parse("CursorRule <- 'first' / 'second' / 'last'");
	grammar.RemoveChoice("CursorRule", [](const PEGExpression &expression) {
		return expression.type == PEGExpression::Type::LITERAL && expression.text.GetString() == "second";
	});

	auto rule = grammar.GetRule("CursorRule");
	REQUIRE(rule);
	REQUIRE(rule->recipe.expression.type == PEGExpression::Type::CHOICE);
	REQUIRE(rule->recipe.expression.children.size() == 2);
	REQUIRE(rule->recipe.expression.children[0].text.GetString() == "first");
	REQUIRE(rule->recipe.expression.children[1].text.GetString() == "last");

	REQUIRE_THROWS(grammar.RemoveChoice("CursorRule", [](const PEGExpression &expression) {
		return expression.type == PEGExpression::Type::LITERAL && expression.text.GetString() == "missing";
	}));

	grammar.RemoveChoice("CursorRule", [](const PEGExpression &expression) {
		return expression.type == PEGExpression::Type::LITERAL && expression.text.GetString() == "first";
	});
	REQUIRE(rule->recipe.expression.type == PEGExpression::Type::LITERAL);
	REQUIRE(rule->recipe.expression.text.GetString() == "last");
}

class OverrideDefaultTerminalRule final : public GrammarExtension {
public:
	OverrideDefaultTerminalRule()
	    : GrammarExtension("default_terminal_rule", "Add a terminal rule override for identifier") {
	}

	void Apply(ParsedGrammar &grammar) const override {
		grammar.AddTerminalRuleOverride("identifier", [](const PEGKeywordHelper &) {
			return make_uniq<KeywordMatcher>("replacement", KeywordInfo(0, ' '));
		});
	}
};

TEST_CASE("Default terminal rule overrides are registered before additions", "[api][grammar_extension]") {
	DuckDB db(nullptr);
	Connection con(db);
	GrammarExtension::Register(*db.instance, make_shared_ptr<OverrideDefaultTerminalRule>());
	REQUIRE_THROWS(CompiledGrammar::Get(*con.context));
}

TEST_CASE("Grammar extensions invalidate an initialized parser cache", "[api][grammar_extension]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(*con.Query("SELECT 1"));
	REQUIRE_FALSE(CompiledGrammar::Get(*con.context)->HasGrammarChanges());

	RegisterGrammarExtensionTestSyntax(*db.instance);
	CheckGrammarExtensionTestSyntax(con);
	REQUIRE(CompiledGrammar::Get(*con.context)->HasGrammarChanges());
}

class AddInvalidGrammarExtensionTestRule final : public GrammarExtension {
public:
	AddInvalidGrammarExtensionTestRule() : GrammarExtension("invalid_grammar_extension", "Invalid grammar extension") {
	}

	void Apply(ParsedGrammar &grammar) const override {
		grammar.AddRule("GrammarExtensionInvalid <- GrammarExtensionMissingRule");
	}
};

TEST_CASE("Invalid Grammar extensions fail grammar compilation", "[api][grammar_extension]") {
	DuckDB db(nullptr);
	Connection con(db);
	GrammarExtension::Register(*db.instance, make_shared_ptr<AddInvalidGrammarExtensionTestRule>());
	REQUIRE_THROWS(CompiledGrammar::Get(*con.context));
}
