#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/parser.hpp"
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

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(GrammarChange::AddRule("GrammarExtensionTestValue <- 'WRONG'"));
		changes.push_back(GrammarChange::AddChoice("UnreservedKeyword", "'ANSWER'"));
		changes.push_back(GrammarChange::AddTerminalRuleOverride(
		    "GrammarExtensionTestValue", [](const PEGKeywordHelper &keyword_helper) {
			    if (!keyword_helper.KeywordCategoryType("ANSWER", PEGKeywordCategory::KEYWORD_UNRESERVED)) {
				    throw InternalException("Parser change keyword is missing from the compiled keyword helper");
			    }
			    return make_uniq<IdentifierMatcher>(SuggestionState::SUGGEST_VARIABLE, keyword_helper);
		    }));
		return changes;
	}
};

class AddGrammarExtensionTestAtom final : public GrammarExtension {
public:
	AddGrammarExtensionTestAtom() : GrammarExtension("extension_test_atom", "GrammarExtensionTestAtom") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(GrammarChange::AddRule("GrammarExtensionTestAtom <- GrammarExtensionTestValue",
		                                         TransformGrammarExtensionTestAtom));
		changes.push_back(
		    GrammarChange::PrependChoice("SelectAtom", "GrammarExtensionTestAtom", [](const PEGExpression &expression) {
			    return expression.type == PEGExpression::Type::REFERENCE &&
			           expression.text.GetString() == "SelectParens";
		    }));
		return changes;
	}
};

static void RegisterGrammarExtensionTestSyntax(DatabaseInstance &db) {
	GrammarExtension::Register(db, make_shared_ptr<AddGrammarExtensionTestValue>());
	GrammarExtension::Register(db, make_shared_ptr<AddGrammarExtensionTestAtom>());
}

static void ActivateGrammarExtensionTestSyntax(Connection &con) {
	REQUIRE_NO_FAIL(*con.Query("SET active_grammar_extensions = ['extension_test_value', 'extension_test_atom']"));
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
	ActivateGrammarExtensionTestSyntax(con);
	CheckGrammarExtensionTestSyntax(con);
}

TEST_CASE("Grammar changes expose structured metadata", "[api][grammar_extension]") {
	auto add_rule = GrammarChange::AddRule("TrackedRule <- 'tracked'");
	REQUIRE(add_rule.Type() == GrammarChangeType::ADD_RULE);
	REQUIRE(add_rule.RuleName() == "TrackedRule");
	REQUIRE(add_rule.Definition() == "TrackedRule <- 'tracked'");

	auto add_choice = GrammarChange::AddChoice("TrackedRule", "'choice'");
	REQUIRE(add_choice.Type() == GrammarChangeType::ADD_CHOICE);
	REQUIRE(add_choice.RuleName() == "TrackedRule");
	REQUIRE(add_choice.Definition() == "'choice'");
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

TEST_CASE("Grammar choices can be replaced", "[api][grammar_extension]") {
	auto grammar = ParsedGrammar::Parse("CursorRule <- 'first' / 'second' / 'last'");
	grammar.ReplaceChoice("CursorRule", "'replacement'", [](const PEGExpression &expression) {
		return expression.type == PEGExpression::Type::LITERAL && expression.text.GetString() == "second";
	});

	auto rule = grammar.GetRule("CursorRule");
	REQUIRE(rule);
	REQUIRE(rule->recipe.expression.type == PEGExpression::Type::CHOICE);
	REQUIRE(rule->recipe.expression.children.size() == 3);
	REQUIRE(rule->recipe.expression.children[0].text.GetString() == "first");
	REQUIRE(rule->recipe.expression.children[1].text.GetString() == "replacement");
	REQUIRE(rule->recipe.expression.children[2].text.GetString() == "last");

	REQUIRE_THROWS(grammar.ReplaceChoice("CursorRule", "'replacement'", [](const PEGExpression &expression) {
		return expression.type == PEGExpression::Type::LITERAL && expression.text.GetString() == "missing";
	}));

	auto non_choice_grammar = ParsedGrammar::Parse("NonChoiceRule <- 'only'");
	REQUIRE_THROWS(
	    non_choice_grammar.ReplaceChoice("NonChoiceRule", "'replacement'", [](const PEGExpression &expression) {
		    return expression.type == PEGExpression::Type::LITERAL;
	    }));
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

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(GrammarChange::AddTerminalRuleOverride("identifier", [](const PEGKeywordHelper &) {
			return make_uniq<KeywordMatcher>("replacement", KeywordInfo(0, ' '));
		}));
		return changes;
	}
};

TEST_CASE("Default terminal rule overrides are registered before additions", "[api][grammar_extension]") {
	DuckDB db(nullptr);
	Connection con(db);
	GrammarExtension::Register(*db.instance, make_shared_ptr<OverrideDefaultTerminalRule>());
	REQUIRE_FAIL(con.Query("SET active_grammar_extensions = ['default_terminal_rule']"));
}

TEST_CASE("The parser cache only holds the base grammar", "[api][grammar_extension]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(*con.Query("SELECT 1"));
	auto base_grammar = CompiledGrammar::Get(*con.context);
	REQUIRE_FALSE(base_grammar->HasGrammarChanges());
	REQUIRE(base_grammar == CompiledGrammar::Get(*con.context));

	RegisterGrammarExtensionTestSyntax(*db.instance);
	REQUIRE(base_grammar == CompiledGrammar::Get(*con.context));
	ActivateGrammarExtensionTestSyntax(con);
	CheckGrammarExtensionTestSyntax(con);
	auto extension_grammar = CompiledGrammar::Get(*con.context);
	REQUIRE(extension_grammar->HasGrammarChanges());
	REQUIRE(extension_grammar == CompiledGrammar::Get(*con.context));
}

TEST_CASE("Active grammar extensions are cached on their connection", "[api][grammar_extension]") {
	DuckDB db(nullptr);
	RegisterGrammarExtensionTestSyntax(*db.instance);
	Connection enabled(db);
	Connection disabled(db);

	auto base_grammar = CompiledGrammar::Get(*disabled.context);
	ActivateGrammarExtensionTestSyntax(enabled);
	CheckGrammarExtensionTestSyntax(enabled);
	REQUIRE_FAIL(disabled.Query("ANSWER"));
	CheckGrammarExtensionTestSyntax(enabled);
	ActiveGrammarExtensionsSetting::SetLocal(*enabled.context, Value::LIST(LogicalType::VARCHAR, vector<Value> {}));
	REQUIRE_FAIL(enabled.Query("ANSWER"));
	REQUIRE(base_grammar == CompiledGrammar::Get(*enabled.context));
}

TEST_CASE("Parser options retain their compiled grammar", "[api][grammar_extension]") {
	DuckDB db(nullptr);
	RegisterGrammarExtensionTestSyntax(*db.instance);
	Connection con(db);
	ActivateGrammarExtensionTestSyntax(con);

	auto options = con.context->GetParserOptions();
	REQUIRE(options.compiled_grammar == CompiledGrammar::Get(*con.context));
	options.extensions = nullptr;

	Parser parser(std::move(options));
	REQUIRE_NOTHROW(parser.ParseQuery("ANSWER"));
	REQUIRE(parser.statements.size() == 1);

	Parser base_parser;
	REQUIRE_NOTHROW(base_parser.ParseQuery("SELECT 42"));
	REQUIRE(base_parser.statements.size() == 1);
}

class AddInvalidGrammarExtensionTestRule final : public GrammarExtension {
public:
	AddInvalidGrammarExtensionTestRule() : GrammarExtension("invalid_grammar_extension", "Invalid grammar extension") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(GrammarChange::AddRule("GrammarExtensionInvalid <- GrammarExtensionMissingRule"));
		return changes;
	}
};

TEST_CASE("Invalid Grammar extensions fail grammar compilation", "[api][grammar_extension]") {
	DuckDB db(nullptr);
	Connection con(db);
	RegisterGrammarExtensionTestSyntax(*db.instance);
	GrammarExtension::Register(*db.instance, make_shared_ptr<AddInvalidGrammarExtensionTestRule>());
	ActivateGrammarExtensionTestSyntax(con);
	auto result = con.Query("SET active_grammar_extensions = ['invalid_grammar_extension']");
	REQUIRE_FAIL(result);
	CheckGrammarExtensionTestSyntax(con);
	auto setting = con.Query("SELECT current_setting('active_grammar_extensions')")->GetValue(0, 0);
	REQUIRE(ListValue::GetChildren(setting).size() == 2);
}
