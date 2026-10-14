#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "duckdb/main/statement_iterator.hpp"
#include "duckdb/parser/parser.hpp"

#include <functional>

using namespace duckdb;

namespace {

struct PreprocessHookState : ClientContextState {
	idx_t starts = 0;
	idx_t ends = 0;
	idx_t depth = 0;
	idx_t query_begins = 0;
	vector<idx_t> transaction_begins;
	vector<idx_t> transaction_commits;
	vector<idx_t> transaction_rollbacks;
	std::function<void()> on_begin;
	std::function<void()> on_end;
	std::function<void()> on_destroy;
	std::function<void()> on_transaction_begin;
	std::function<void()> on_transaction_commit;
	std::function<void()> on_transaction_rollback;

	~PreprocessHookState() override {
		if (on_destroy) {
			on_destroy();
		}
	}
	void OnStatementPreprocessBegin(ClientContext &) override {
		starts++;
		depth++;
		if (on_begin) {
			on_begin();
		}
	}
	void OnStatementPreprocessEnd(ClientContext &) noexcept override {
		depth--;
		ends++;
		if (on_end) {
			on_end();
		}
	}
	void QueryBegin(ClientContext &) override {
		query_begins++;
		REQUIRE(depth == 0);
	}
	void TransactionBegin(MetaTransaction &, ClientContext &) override {
		transaction_begins.push_back(depth);
		if (on_transaction_begin) {
			on_transaction_begin();
		}
	}
	void TransactionCommit(MetaTransaction &, ClientContext &) override {
		transaction_commits.push_back(depth);
		if (on_transaction_commit) {
			on_transaction_commit();
		}
	}
	void TransactionRollback(MetaTransaction &, ClientContext &) override {
		transaction_rollbacks.push_back(depth);
		if (on_transaction_rollback) {
			on_transaction_rollback();
		}
	}
};

shared_ptr<PreprocessHookState> RegisterPreprocessState(Connection &con) {
	auto state = make_shared_ptr<PreprocessHookState>();
	con.context->registered_state->Insert("preprocess_test", state);
	return state;
}

struct PreprocessParserInfo : ParserExtensionInfo {
	explicit PreprocessParserInfo(PreprocessHookState &state) : state(state) {
	}
	PreprocessHookState &state;
	vector<idx_t> select_depths;
};

ParserOverrideResult ObservePreprocessParse(ParserExtensionInfo *info_p, const string &query, ParserOptions &) {
	auto &info = *static_cast<PreprocessParserInfo *>(info_p);
	if (query == "SELECT 42") {
		info.select_depths.push_back(info.state.depth);
	}
	return ParserOverrideResult();
}

StatementSignature BindPreprocessStatement(Connection &con, const string &query) {
	auto statements = con.ExtractStatements(query);
	REQUIRE(statements.size() == 1);
	return con.context->BindStatement(std::move(statements[0]));
}

} // namespace

TEST_CASE("Statement preprocessing encloses transaction callbacks and PRAGMA reparsing", "[api][preprocess_hooks]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET allow_parser_override_extension='fallback'"));
	auto state = RegisterPreprocessState(con);
	auto info = make_shared_ptr<PreprocessParserInfo>(*state);
	ParserExtension extension;
	extension.parser_info = info;
	extension.parser_override = ObservePreprocessParse;
	ExtensionCallbackManager::Get(*con.context).Register(std::move(extension));
	state->on_transaction_begin = [&] {
		Parser parser(*con.context);
		parser.ParseQuery("SELECT 42");
	};

	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, "test_extension", ""};
	ExtensionLoader loader {load_info};
	loader.RegisterFunction(PragmaFunction::PragmaStatement(
	    "preprocess_answer", +[](ClientContext &context, const FunctionParameters &) -> string {
		    auto state = context.registered_state->Get<PreprocessHookState>("preprocess_test");
		    REQUIRE(state->depth == 1);
		    return "SELECT 42";
	    }));

	auto signature = BindPreprocessStatement(con, "PRAGMA preprocess_answer");
	REQUIRE(signature.names.size() == 1);
	REQUIRE(state->starts == 1);
	REQUIRE(state->ends == 1);
	REQUIRE(state->depth == 0);
	REQUIRE(state->query_begins == 0);
	REQUIRE(state->transaction_begins == vector<idx_t> {1});
	REQUIRE(state->transaction_commits == vector<idx_t> {1});
	REQUIRE(info->select_depths == vector<idx_t> {1, 1});

	state->on_transaction_begin = nullptr;
	REQUIRE_NO_FAIL(con.Query("SELECT 42"));
	REQUIRE(state->starts == 1);
	REQUIRE(state->query_begins == 1);
	REQUIRE(info->select_depths == vector<idx_t> {1, 1, 0});
}

TEST_CASE("Statement preprocessing unwinds after catalog and reparse errors", "[api][preprocess_hooks]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto state = RegisterPreprocessState(con);
	string query;
	SECTION("Catalog lookup failure") {
		query = "PRAGMA missing_preprocess_pragma";
	}
	SECTION("Replacement query parser failure") {
		ExtensionInfo extension_info {};
		ExtensionActiveLoad load_info {*db.instance, extension_info, "test_extension", ""};
		ExtensionLoader loader {load_info};
		loader.RegisterFunction(PragmaFunction::PragmaStatement(
		    "preprocess_invalid", +[](ClientContext &, const FunctionParameters &) -> string { return "SELECT ("; }));
		query = "PRAGMA preprocess_invalid";
	}
	REQUIRE_THROWS(con.Query(query)->ThrowIfError());
	REQUIRE(state->starts == 1);
	REQUIRE(state->ends == 1);
	REQUIRE(state->depth == 0);
	REQUIRE(state->transaction_rollbacks == vector<idx_t> {1});
	REQUIRE_NO_FAIL(con.Query("SELECT 42"));
}

TEST_CASE("Statement preprocessing remains scoped in explicit transactions and lazy APIs", "[api][preprocess_hooks]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto state = RegisterPreprocessState(con);
	con.BeginTransaction();
	const auto starts = state->starts;
	auto iterator = con.context->IterateStatements("PRAGMA version");
	REQUIRE(iterator.Peek());
	REQUIRE(state->starts == starts);
	auto statement = iterator.GetStatement();
	REQUIRE(statement);
	REQUIRE(state->starts == starts);
	con.context->BindStatement(std::move(statement));
	REQUIRE(state->starts == starts + 1);
	REQUIRE(state->ends == state->starts);
	REQUIRE(state->depth == 0);
	REQUIRE(con.context->transaction.HasActiveTransaction());

	auto prepared = con.Prepare("SELECT 42");
	REQUIRE_FALSE(prepared->HasError());
	REQUIRE_NO_FAIL(prepared->Execute());
	REQUIRE(state->starts == starts + 1);
	con.Rollback();
	REQUIRE(state->depth == 0);
}

TEST_CASE("Statement preprocessing unwinds when transaction callbacks throw", "[api][preprocess_hooks]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Injected failures interrupt transaction callback dispatch before all transaction observers are notified.
	con.context->registered_state->Remove("debug_client_context_state");
	auto state = RegisterPreprocessState(con);
	string query = "PRAGMA version";
	const auto fail = [] {
		throw InvalidInputException("transaction callback failed");
	};
	SECTION("Begin") {
		state->on_transaction_begin = fail;
	}
	SECTION("Commit") {
		state->on_transaction_commit = fail;
	}
	SECTION("Rollback") {
		state->on_transaction_rollback = fail;
		query = "PRAGMA missing_preprocess_pragma";
	}
	REQUIRE_THROWS(con.Query(query)->ThrowIfError());
	REQUIRE(state->starts == 1);
	REQUIRE(state->ends == 1);
	REQUIRE(state->depth == 0);
	state->on_transaction_begin = nullptr;
	state->on_transaction_commit = nullptr;
	state->on_transaction_rollback = nullptr;
	if (con.context->transaction.HasActiveTransaction()) {
		// A failing begin callback can leave the preprocessing transaction open in autocommit mode.
		con.context->transaction.Rollback(nullptr);
	}
	REQUIRE_NO_FAIL(con.Query("SELECT 42"));
}

TEST_CASE("Statement preprocessing callbacks support reentrancy", "[api][preprocess_hooks]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection nested(db);
	auto state = RegisterPreprocessState(con);
	nested.context->registered_state->Insert("preprocess_test", state);
	state->on_begin = [&] {
		if (state->depth != 1) {
			return;
		}
		BindPreprocessStatement(nested, "PRAGMA version");
		REQUIRE(state->depth == 1);
	};
	BindPreprocessStatement(con, "PRAGMA version");
	REQUIRE(state->starts == 2);
	REQUIRE(state->ends == 2);
	REQUIRE(state->depth == 0);
	REQUIRE(state->transaction_begins == vector<idx_t> {2, 1});
	REQUIRE(state->transaction_commits == vector<idx_t> {2, 1});
}

TEST_CASE("Statement preprocessing retains the entry snapshot until paired callbacks finish",
          "[api][preprocess_hooks]") {
	DuckDB db(nullptr);
	Connection con(db);
	idx_t destroyed = 0;
	idx_t ended = 0;
	auto later = make_shared_ptr<PreprocessHookState>();
	{
		auto state = RegisterPreprocessState(con);
		state->on_begin = [&] {
			con.context->registered_state->Remove("preprocess_test");
			con.context->registered_state->Insert("later", later);
		};
		state->on_end = [&] {
			ended++;
		};
		state->on_destroy = [&] {
			destroyed++;
		};
	}
	BindPreprocessStatement(con, "PRAGMA version");
	REQUIRE(ended == 1);
	REQUIRE(destroyed == 1);
	REQUIRE(later->starts == 0);
	REQUIRE(later->ends == 0);
	BindPreprocessStatement(con, "PRAGMA version");
	REQUIRE(later->starts == 1);
	REQUIRE(later->ends == 1);
}

TEST_CASE("Statement preprocessing unwinds successful callbacks when a later begin throws", "[api][preprocess_hooks]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &registry = *con.context->registered_state;
	registry.Remove("debug_client_context_state");
	for (idx_t i = 0; i < 4; i++) {
		registry.Insert(std::to_string(i), make_shared_ptr<PreprocessHookState>());
	}
	auto states = registry.States();
	REQUIRE(states.size() == 4);
	auto &first = states[0]->Cast<PreprocessHookState>();
	auto &second = states[1]->Cast<PreprocessHookState>();
	auto &third = states[2]->Cast<PreprocessHookState>();
	auto &fourth = states[3]->Cast<PreprocessHookState>();
	vector<idx_t> end_order;
	first.on_end = [&] {
		end_order.push_back(0);
	};
	second.on_end = [&] {
		end_order.push_back(1);
	};
	third.on_begin = [&] {
		// A begin callback that throws must unwind its own incomplete entry.
		third.depth--;
		throw InvalidInputException("preprocessing begin failed");
	};
	REQUIRE_THROWS(BindPreprocessStatement(con, "PRAGMA version"));
	REQUIRE(first.starts == 1);
	REQUIRE(first.ends == 1);
	REQUIRE(first.depth == 0);
	REQUIRE(second.starts == 1);
	REQUIRE(second.ends == 1);
	REQUIRE(second.depth == 0);
	REQUIRE(end_order == vector<idx_t> {1, 0});
	REQUIRE(third.starts == 1);
	REQUIRE(third.ends == 0);
	REQUIRE(third.depth == 0);
	REQUIRE(fourth.starts == 0);
	REQUIRE_FALSE(con.context->transaction.HasActiveTransaction());
	third.on_begin = nullptr;
	REQUIRE_NO_FAIL(con.Query("SELECT 42"));
}

TEST_CASE("Statement preprocessing covers query execution and repeated prepared expansion", "[api][preprocess_hooks]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto state = RegisterPreprocessState(con);
	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, "test_extension", ""};
	ExtensionLoader loader {load_info};
	loader.RegisterFunction(PragmaFunction::PragmaStatement(
	    "preprocess_group", +[](ClientContext &context, const FunctionParameters &) -> string {
		    auto state = context.registered_state->Get<PreprocessHookState>("preprocess_test");
		    REQUIRE(state->depth == 1);
		    return "SELECT 41; SELECT 42";
	    }));

	REQUIRE_NO_FAIL(con.Query("PRAGMA preprocess_group"));
	REQUIRE(state->starts == 1);
	REQUIRE(state->ends == 1);
	REQUIRE(state->depth == 0);
	REQUIRE(state->query_begins == 1);
	auto prepared = con.Prepare("PRAGMA preprocess_group");
	REQUIRE_FALSE(prepared->HasError());
	REQUIRE(state->starts == 2);
	REQUIRE(state->ends == 2);
	for (idx_t i = 0; i < 2; i++) {
		REQUIRE_NO_FAIL(prepared->Execute());
		REQUIRE(state->starts == 3 + i);
		REQUIRE(state->ends == state->starts);
		REQUIRE(state->depth == 0);
	}
	const auto starts = state->starts;
	REQUIRE_NO_FAIL(con.Query("BEGIN; SELECT 42; ROLLBACK"));
	REQUIRE(state->starts == starts);
}
