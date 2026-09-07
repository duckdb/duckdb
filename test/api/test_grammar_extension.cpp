#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/matcher/identifier_matcher.hpp"
#include "duckdb/parser/peg/matcher/keyword_matcher.hpp"
#include "duckdb/parser/peg/matcher/list_matcher.hpp"
#include "duckdb/parser/peg/matcher_stack.hpp"
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
		changes.push_back(GrammarChange::AddRule("GrammarExtensionTestValue <- 'WRONG'"));
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
	REQUIRE_NO_FAIL(*con.Query("SET heap_based_parser = false"));
	CheckGrammarExtensionTestSyntax(con);
	REQUIRE_NO_FAIL(*con.Query("SET heap_based_parser = true"));
	CheckGrammarExtensionTestSyntax(con);
}

struct MatchProcessLifetimeState {
	idx_t active = 0;
	idx_t started = 0;
	idx_t depth = 0;
	bool throw_at_leaf = false;
	bool throw_in_constructor = false;
	bool storage_valid = true;
	bool create_result = false;
	bool fail_at_leaf = false;
	idx_t root_children = 1;
	vector<idx_t> destroyed;
	vector<uintptr_t> addresses;
};

class NestedTestMatchProcess : public MatchProcess {
public:
	NestedTestMatchProcess(const Matcher &matcher_p, MatchState &state, MatchProcessLifetimeState &lifetime_p)
	    : matcher(matcher_p), child_state(state), lifetime(lifetime_p), depth(++lifetime.active) {
		lifetime.started++;
		lifetime.addresses.push_back(reinterpret_cast<uintptr_t>(this));
	}

	~NestedTestMatchProcess() override {
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
			    vector<reference<ParseResult>>(), string("pooled result"), optional_idx()));
		}
		return MatchStep::Complete(MatcherResult::Success());
	}

private:
	const Matcher &matcher;
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

TEST_CASE("Heap matcher segments preserve custom process lifetimes", "[api][grammar_extension]") {
	vector<MatcherToken> tokens;
	TokenIterator iterator(tokens);
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator allocator;
	idx_t max_token_index = 0;
	MatchContext context(suggestions, allocator, max_token_index);
	MatchState state(iterator, context);
	MatchProcessLifetimeState lifetime;
	lifetime.destroyed.reserve(130);
	NestedTestMatcher matcher(lifetime);

	SECTION("Completed frames are reused across segment boundaries") {
		MatchStack stack;
		for (idx_t depth : {idx_t(1), idx_t(32), idx_t(33), idx_t(64), idx_t(65), idx_t(130), idx_t(33)}) {
			lifetime.depth = depth;
			lifetime.started = 0;
			lifetime.destroyed.clear();
			REQUIRE(stack.Execute({matcher, state}).IsSuccess());
			REQUIRE(lifetime.active == 0);
			REQUIRE(lifetime.started == depth);
			REQUIRE(lifetime.destroyed.size() == depth);
			for (idx_t i = 0; i < depth; i++) {
				REQUIRE(lifetime.destroyed[i] == depth - i);
			}
		}
	}

	SECTION("Exceptions destroy child processes before their parents") {
		lifetime.depth = 130;
		lifetime.throw_at_leaf = true;
		{
			MatchStack stack;
			REQUIRE_THROWS_AS(stack.Execute({matcher, state}), InvalidInputException);
		}
		REQUIRE(lifetime.active == 0);
		REQUIRE(lifetime.started == lifetime.depth);
		REQUIRE(lifetime.destroyed.size() == lifetime.depth);
		for (idx_t i = 0; i < lifetime.depth; i++) {
			REQUIRE(lifetime.destroyed[i] == lifetime.depth - i);
		}
	}
}

class LegacyListTestMatcher final : public ListMatcher {
public:
	explicit LegacyListTestMatcher(MatchProcessLifetimeState &lifetime_p) : lifetime(lifetime_p) {
	}

	unique_ptr<MatchProcess> StartMatch(MatchState &state) const override {
		return make_uniq<NestedTestMatchProcess>(*this, state, lifetime);
	}

private:
	MatchProcessLifetimeState &lifetime;
};

template <idx_t SIZE, idx_t ALIGNMENT>
class alignas(ALIGNMENT) ArenaNestedTestMatchProcess final : public NestedTestMatchProcess {
public:
	ArenaNestedTestMatchProcess(const Matcher &matcher, MatchState &state, MatchProcessLifetimeState &lifetime_p)
	    : NestedTestMatchProcess(matcher, state, lifetime_p), lifetime(lifetime_p) {
		lifetime.storage_valid &= reinterpret_cast<uintptr_t>(this) % ALIGNMENT == 0;
		if (lifetime.throw_in_constructor && lifetime.active == lifetime.depth) {
			throw InvalidInputException("Nested process constructor failure");
		}
		payload.fill(0xa5);
	}

	~ArenaNestedTestMatchProcess() override {
		for (auto byte : payload) {
			lifetime.storage_valid &= byte == 0xa5;
		}
	}

private:
	MatchProcessLifetimeState &lifetime;
	array<uint8_t, SIZE> payload;
};

class ArenaNestedTestMatcher final : public ListMatcher {
public:
	explicit ArenaNestedTestMatcher(MatchProcessLifetimeState &lifetime_p, bool mixed_ownership_p = false)
	    : lifetime(lifetime_p), mixed_ownership(mixed_ownership_p) {
	}

	unique_ptr<MatchProcess> StartMatch(MatchState &state) const override {
		return make_uniq<NestedTestMatchProcess>(*this, state, lifetime);
	}

	match_process_ptr_t StartMatch(MatchState &state, MatchProcessAllocator &allocator) const override {
		if (lifetime.active % 2) {
			if (mixed_ownership) {
				return Matcher::StartMatch(state, allocator);
			}
			return allocator.Make<ArenaNestedTestMatchProcess<9000, 64>>(*this, state, lifetime);
		}
		return allocator.Make<ArenaNestedTestMatchProcess<32, 8>>(*this, state, lifetime);
	}

private:
	MatchProcessLifetimeState &lifetime;
	bool mixed_ownership;
};

TEST_CASE("Heap matcher preserves overrides on derived built-in matchers", "[api][grammar_extension]") {
	vector<MatcherToken> tokens;
	TokenIterator iterator(tokens);
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator allocator;
	idx_t max_token_index = 0;
	MatchContext context(suggestions, allocator, max_token_index);
	MatchState state(iterator, context);
	MatchProcessLifetimeState lifetime;
	lifetime.depth = 130;
	LegacyListTestMatcher matcher(lifetime);
	MatchStack stack;
	REQUIRE(stack.Execute({matcher, state}).IsSuccess());
	REQUIRE(lifetime.started == lifetime.depth);
	REQUIRE(lifetime.active == 0);
	REQUIRE(lifetime.destroyed.size() == lifetime.depth);
}

TEST_CASE("Heap matcher reuses variable-sized aligned process storage", "[api][grammar_extension]") {
	vector<MatcherToken> tokens;
	TokenIterator iterator(tokens);
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator allocator;
	idx_t max_token_index = 0;
	MatchContext context(suggestions, allocator, max_token_index);
	MatchState state(iterator, context);
	MatchProcessLifetimeState lifetime;
	ArenaNestedTestMatcher matcher(lifetime);

	SECTION("Sibling executions reuse storage without moving parent processes") {
		MatchStack stack;
		vector<uintptr_t> addresses;
		for (idx_t depth :
		     {idx_t(1), idx_t(64), idx_t(65), idx_t(128), idx_t(129), idx_t(130), idx_t(33), idx_t(130)}) {
			lifetime.depth = depth;
			lifetime.started = 0;
			lifetime.destroyed.clear();
			lifetime.addresses.clear();
			REQUIRE(stack.Execute({matcher, state}).IsSuccess());
			REQUIRE(lifetime.started == depth);
			REQUIRE(lifetime.active == 0);
			REQUIRE(lifetime.storage_valid);
			REQUIRE(lifetime.destroyed.size() == depth);
			for (idx_t i = 0; i < depth; i++) {
				REQUIRE(lifetime.destroyed[i] == depth - i);
			}
			for (idx_t i = 0; i < MinValue<idx_t>(addresses.size(), lifetime.addresses.size()); i++) {
				REQUIRE(addresses[i] == lifetime.addresses[i]);
			}
			if (addresses.size() < lifetime.addresses.size()) {
				addresses = lifetime.addresses;
			}
		}
	}

	SECTION("Resume exceptions destroy pooled children before their parents") {
		lifetime.throw_at_leaf = true;
	}
	SECTION("Constructor exceptions destroy constructed members and parent processes") {
		lifetime.throw_in_constructor = true;
	}
	SECTION("Pooled and heap-owned processes can alternate on the same stack") {
		lifetime.depth = 130;
		ArenaNestedTestMatcher mixed_matcher(lifetime, true);
		MatchStack stack;
		REQUIRE(stack.Execute({mixed_matcher, state}).IsSuccess());
		REQUIRE(lifetime.active == 0);
		REQUIRE(lifetime.storage_valid);
		REQUIRE(lifetime.started == lifetime.depth);
		REQUIRE(lifetime.destroyed.size() == lifetime.depth);
		for (idx_t i = 0; i < lifetime.depth; i++) {
			REQUIRE(lifetime.destroyed[i] == lifetime.depth - i);
		}
	}
	SECTION("Siblings reuse process slots while their parent stays alive") {
		lifetime.depth = 65;
		lifetime.root_children = 2;
		MatchStack stack;
		REQUIRE(stack.Execute({matcher, state}).IsSuccess());
		REQUIRE(lifetime.active == 0);
		REQUIRE(lifetime.storage_valid);
		REQUIRE(lifetime.started == 1 + 2 * (lifetime.depth - 1));
		for (idx_t i = 1; i < lifetime.depth; i++) {
			REQUIRE(lifetime.addresses[i] == lifetime.addresses[lifetime.depth - 1 + i]);
		}
	}
	if (lifetime.throw_at_leaf || lifetime.throw_in_constructor) {
		lifetime.depth = 130;
		{
			MatchStack stack;
			REQUIRE_THROWS_AS(stack.Execute({matcher, state}), InvalidInputException);
		}
		REQUIRE(lifetime.active == 0);
		REQUIRE(lifetime.started == lifetime.depth);
		REQUIRE(lifetime.storage_valid);
		REQUIRE(lifetime.destroyed.size() == lifetime.depth);
		for (idx_t i = 0; i < lifetime.depth; i++) {
			REQUIRE(lifetime.destroyed[i] == lifetime.depth - i);
		}
	}
}

TEST_CASE("Process constructor failures restore the pool checkpoint", "[api][grammar_extension]") {
	vector<MatcherToken> tokens;
	TokenIterator iterator(tokens);
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_results;
	idx_t max_token_index = 0;
	MatchContext context(suggestions, parse_results, max_token_index);
	MatchState state(iterator, context);
	MatchProcessLifetimeState lifetime;
	lifetime.depth = 1;
	lifetime.throw_in_constructor = true;
	ArenaNestedTestMatcher matcher(lifetime);
	ArenaAllocator arena(Allocator::DefaultAllocator());
	MatchProcessAllocator pool(arena);
	auto position = pool.GetPosition();
	idx_t reserved = 0;
	for (idx_t i = 0; i < 10; i++) {
		REQUIRE_THROWS_AS((pool.Make<ArenaNestedTestMatchProcess<9000, 64>>(matcher, state, lifetime)),
		                  InvalidInputException);
		REQUIRE(lifetime.active == 0);
		REQUIRE(pool.GetPosition().segment.get() == position.segment.get());
		REQUIRE(pool.GetPosition().offset == position.offset);
		if (i == 0) {
			reserved = arena.AllocationSize();
		}
		REQUIRE(arena.AllocationSize() == reserved);
	}
	lifetime.throw_in_constructor = false;
	auto process = pool.Make<ArenaNestedTestMatchProcess<9000, 64>>(matcher, state, lifetime);
	REQUIRE(lifetime.active == 1);
	process.reset();
	pool.Rewind(position);
	REQUIRE(lifetime.active == 0);
	REQUIRE(lifetime.storage_valid);
}

TEST_CASE("Packrat results outlive reused process storage", "[api][grammar_extension]") {
	vector<MatcherToken> tokens;
	TokenIterator iterator(tokens);
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_results;
	ParserPackratCache cache;
	idx_t max_token_index = 0;
	MatchContext context(suggestions, parse_results, max_token_index);
	context.packrat_cache = &cache;
	MatchState state(iterator, context);
	MatchProcessLifetimeState lifetime;
	lifetime.depth = 3;
	lifetime.create_result = true;
	MatcherAllocator matchers;
	auto &matcher = matchers.Allocate(make_uniq<ArenaNestedTestMatcher>(lifetime));
	matcher.SetPackratMemoized();
	MatchStack stack;

	SECTION("Cached successes retain separately allocated parse results") {
	}
	SECTION("Cached failures need no new process") {
		lifetime.fail_at_leaf = true;
	}
	auto first = stack.Execute({matcher, state});
	REQUIRE(first.IsSuccess() == !lifetime.fail_at_leaf);
	REQUIRE(lifetime.started == lifetime.depth);
	REQUIRE(lifetime.active == 0);

	ArenaNestedTestMatcher overwriter(lifetime);
	lifetime.depth = 130;
	lifetime.create_result = false;
	stack.Execute({overwriter, state});
	lifetime.started = 0;
	auto cached = stack.Execute({matcher, state});
	REQUIRE(cached.IsSuccess() == first.IsSuccess());
	REQUIRE(cached.GetParseResult().get() == first.GetParseResult().get());
	REQUIRE(lifetime.started == 0);
	REQUIRE(lifetime.active == 0);
	REQUIRE(lifetime.storage_valid);
	if (cached.IsSuccess()) {
		REQUIRE(cached.HasParseResult());
		REQUIRE(cached.GetParseResult()->name == "pooled result");
	}
}

TEST_CASE("Compiled grammar processes use arena ownership", "[api][grammar_extension]") {
	vector<MatcherToken> tokens;
	TokenIterator iterator(tokens);
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_results;
	idx_t max_token_index = 0;
	MatchContext context(suggestions, parse_results, max_token_index);
	MatchState state(iterator, context);
	auto grammar = CompiledGrammar::Create();
	ArenaAllocator arena(Allocator::DefaultAllocator());
	MatchProcessAllocator pool(arena);
	auto position = pool.GetPosition();
	auto process = grammar->TopLevelStatementMatcher().StartMatch(state, pool);
	REQUIRE(process);
	REQUIRE(process.get_deleter().arena_allocated);
	process.reset();
	pool.Rewind(position);
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
