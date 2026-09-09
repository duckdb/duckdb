#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher/identifier_matcher.hpp"
#include "duckdb/parser/peg/matcher/keyword_matcher.hpp"
#include "duckdb/parser/peg/matcher_stack.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"

using namespace duckdb;

class GrammarExtensionTestValueTransformProcess final : public TransformProcess {
public:
	TransformStep Resume(unique_ptr<TransformResultValue> child_result) override {
		D_ASSERT(!child_result);
		return TransformStep::Complete(make_uniq<TypedTransformResult<bool>>(true));
	}
};

class GrammarExtensionTestTransformProcess final : public TransformProcess {
public:
	GrammarExtensionTestTransformProcess(PEGTransformer &transformer_p, ParseResult &parse_result_p)
	    : transformer(transformer_p), parse_result(parse_result_p) {
	}

	TransformStep Resume(unique_ptr<TransformResultValue> child_result) override {
		if (!child_result) {
			auto &list = parse_result.Cast<ListParseResult>();
			return TransformStep::Child({transformer.GetRule("GrammarExtensionTestValue"), list.GetChild(0)});
		}
		D_ASSERT(TryGetTransformResult<bool>(*child_result));
		auto statement = make_uniq<SelectStatement>();
		auto select_node = make_uniq<SelectNode>();
		select_node->select_list.push_back(make_uniq<ConstantExpression>(Value::INTEGER(42)));
		select_node->from_table = make_uniq<EmptyTableRef>();
		statement->node = std::move(select_node);
		return TransformStep::Complete(
		    make_uniq<TypedTransformResult<unique_ptr<SelectStatement>>>(std::move(statement)));
	}

private:
	PEGTransformer &transformer;
	ParseResult &parse_result;
};

static unique_ptr<TransformProcess> StartGrammarExtensionTestValueTransform(PEGTransformer &, ParseResult &) {
	return make_uniq<GrammarExtensionTestValueTransformProcess>();
}

static unique_ptr<TransformProcess> StartGrammarExtensionTestTransform(PEGTransformer &transformer,
                                                                       ParseResult &parse_result) {
	return make_uniq<GrammarExtensionTestTransformProcess>(transformer, parse_result);
}

class GrammarExtensionTestMatchProcess final : public MatchProcess {
public:
	GrammarExtensionTestMatchProcess(const Matcher &child_p, MatchState &state_p)
	    : child(child_p), state(state_p), child_state(state_p) {
	}

	MatchStep Resume(optional<MatcherResult> child_result) override {
		D_ASSERT(awaiting_child == child_result.has_value());
		if (!child_result) {
			awaiting_child = true;
			return MatchStep::Child({child, child_state});
		}
		awaiting_child = false;
		if (child_result->IsSuccess()) {
			state.token_iterator.SetPosition(child_state.token_iterator);
		}
		return MatchStep::Complete(*child_result);
	}

private:
	const Matcher &child;
	MatchState &state;
	MatchState child_state;
	bool awaiting_child = false;
};

class GrammarExtensionTestMatcher final : public Matcher {
public:
	GrammarExtensionTestMatcher() : child("ANSWER", KeywordInfo()) {
	}

	unique_ptr<MatchProcess> StartMatch(MatchState &state) const override {
		return make_uniq<GrammarExtensionTestMatchProcess>(child, state);
	}

	SuggestionType AddSuggestionInternal(MatchState &state) const override {
		return child.AddSuggestion(state);
	}

	string ToString() const override {
		return "GrammarExtensionTestMatcher";
	}

private:
	KeywordMatcher child;
};

class AddGrammarExtensionTestValue final : public GrammarExtension {
public:
	AddGrammarExtensionTestValue() : GrammarExtension("extension_test_value", "GrammarExtensionTestValue") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(
		    GrammarChange::AddRule("GrammarExtensionTestValue <- 'WRONG'", StartGrammarExtensionTestValueTransform));
		changes.push_back(GrammarChange::AddChoice("UnreservedKeyword", "'ANSWER'"));
		changes.push_back(GrammarChange::AddTerminalRuleOverride(
		    "GrammarExtensionTestValue", [](const PEGKeywordHelper &keyword_helper) {
			    if (!keyword_helper.KeywordCategoryType("ANSWER", PEGKeywordCategory::KEYWORD_UNRESERVED)) {
				    throw InternalException("Parser change keyword is missing from the compiled keyword helper");
			    }
			    return make_uniq<GrammarExtensionTestMatcher>();
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
		                                         StartGrammarExtensionTestTransform));
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
	REQUIRE_NO_FAIL(*con.Query("SET heap_based_parser = true"));
	CheckGrammarExtensionTestSyntax(con);
}

struct MatchProcessLifetimeState {
	idx_t active = 0;
	idx_t started = 0;
	idx_t depth = 0;
	bool throw_at_leaf = false;
	bool fail_at_leaf = false;
	bool create_result = false;
	bool state_valid = true;
	idx_t root_children = 1;
	vector<idx_t> destroyed;
};

class NestedTestMatchProcess final : public MatchProcess {
public:
	NestedTestMatchProcess(const Matcher &matcher_p, MatchState &state, MatchProcessLifetimeState &lifetime_p)
	    : matcher(matcher_p), input_state(state), child_state(state), lifetime(lifetime_p), depth(++lifetime.active) {
		lifetime.started++;
	}

	~NestedTestMatchProcess() override {
		lifetime.state_valid &= &input_state.context == &child_state.context;
		lifetime.destroyed.push_back(depth);
		lifetime.active--;
	}

	MatchStep Resume(optional<MatcherResult> child_result) override {
		if (child_result) {
			if (depth == 1 && ++completed_children < lifetime.root_children) {
				return MatchStep::Child({matcher, child_state});
			}
			return MatchStep::Complete(*child_result);
		}
		if (depth < lifetime.depth) {
			return MatchStep::Child({matcher, child_state});
		}
		if (lifetime.throw_at_leaf) {
			throw InvalidInputException("Nested matcher test failure");
		}
		if (lifetime.fail_at_leaf) {
			return MatchStep::Complete(MatcherResult::Failure());
		}
		if (lifetime.create_result) {
			return MatchStep::Complete(child_state.AllocateParseResult<ListParseResult>(
			    vector<reference<ParseResult>>(), string("nested result"), optional_idx()));
		}
		return MatchStep::Complete(MatcherResult::Success());
	}

private:
	const Matcher &matcher;
	MatchState &input_state;
	MatchState child_state;
	MatchProcessLifetimeState &lifetime;
	idx_t depth;
	idx_t completed_children = 0;
};

class NestedTestMatcher final : public Matcher {
public:
	explicit NestedTestMatcher(MatchProcessLifetimeState &lifetime_p)
	    : Matcher(MatcherType::LIST), lifetime(lifetime_p) {
	}

	unique_ptr<MatchProcess> StartMatch(MatchState &state) const override {
		return make_uniq<NestedTestMatchProcess>(*this, state, lifetime);
	}

	SuggestionType AddSuggestionInternal(MatchState &) const override {
		return SuggestionType::MANDATORY;
	}

	string ToString() const override {
		return "NestedTestMatcher";
	}

private:
	MatchProcessLifetimeState &lifetime;
};

TEST_CASE("Heap matcher vector growth preserves custom process lifetimes", "[api][grammar_extension]") {
	vector<MatcherToken> tokens;
	TokenIterator iterator(tokens);
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator allocator;
	idx_t max_token_index = 0;
	MatchContext context(suggestions, allocator, max_token_index);
	MatchState state(iterator, context);
	MatchProcessLifetimeState lifetime;
	lifetime.destroyed.reserve(2049);
	NestedTestMatcher matcher(lifetime);

	SECTION("Completed frames are reused across vector growth boundaries") {
		MatchStack stack;
		lifetime.create_result = true;
		for (idx_t depth : {idx_t(1), idx_t(64), idx_t(65), idx_t(128), idx_t(129), idx_t(256), idx_t(257), idx_t(1025),
		                    idx_t(33), idx_t(130)}) {
			lifetime.depth = depth;
			lifetime.started = 0;
			lifetime.destroyed.clear();
			auto result = stack.Execute({matcher, state});
			REQUIRE(result.IsSuccess());
			REQUIRE(result.HasParseResult());
			REQUIRE(result.GetParseResult()->name == "nested result");
			REQUIRE(lifetime.active == 0);
			REQUIRE(lifetime.state_valid);
			REQUIRE(lifetime.started == depth);
			REQUIRE(lifetime.destroyed.size() == depth);
			for (idx_t i = 0; i < depth; i++) {
				REQUIRE(lifetime.destroyed[i] == depth - i);
			}
		}
	}

	SECTION("Exceptions destroy child processes before their parents") {
		lifetime.depth = 1025;
		lifetime.throw_at_leaf = true;
		{
			MatchStack stack;
			REQUIRE_THROWS_AS(stack.Execute({matcher, state}), InvalidInputException);
		}
		REQUIRE(lifetime.active == 0);
		REQUIRE(lifetime.state_valid);
		REQUIRE(lifetime.started == lifetime.depth);
		REQUIRE(lifetime.destroyed.size() == lifetime.depth);
		for (idx_t i = 0; i < lifetime.depth; i++) {
			REQUIRE(lifetime.destroyed[i] == lifetime.depth - i);
		}
	}

	SECTION("A waiting parent resumes after multiple deep children") {
		lifetime.depth = 1025;
		lifetime.root_children = 2;
		MatchStack stack;
		REQUIRE(stack.Execute({matcher, state}).IsSuccess());
		REQUIRE(lifetime.active == 0);
		REQUIRE(lifetime.state_valid);
		REQUIRE(lifetime.started == 1 + 2 * (lifetime.depth - 1));
		REQUIRE(lifetime.destroyed.size() == lifetime.started);
		for (idx_t sibling = 0; sibling < 2; sibling++) {
			for (idx_t i = 0; i < lifetime.depth - 1; i++) {
				REQUIRE(lifetime.destroyed[sibling * (lifetime.depth - 1) + i] == lifetime.depth - i);
			}
		}
		REQUIRE(lifetime.destroyed.back() == 1);
	}

	SECTION("Failed matches leave the vector ready for another execution") {
		lifetime.depth = 1025;
		lifetime.fail_at_leaf = true;
		MatchStack stack;
		REQUIRE_FALSE(stack.Execute({matcher, state}).IsSuccess());
		REQUIRE(lifetime.active == 0);
		REQUIRE(lifetime.state_valid);
		REQUIRE(lifetime.started == lifetime.depth);
		REQUIRE(lifetime.destroyed.size() == lifetime.depth);
		lifetime.fail_at_leaf = false;
		lifetime.started = 0;
		lifetime.destroyed.clear();
		REQUIRE(stack.Execute({matcher, state}).IsSuccess());
		REQUIRE(lifetime.active == 0);
		REQUIRE(lifetime.state_valid);
		REQUIRE(lifetime.started == lifetime.depth);
	}
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
