#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/peg/dialect_extension.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

namespace duckdb {

Parser::Parser(ClientContext &context) : Parser(context.GetParserOptions()) {
}

Parser::Parser(ClientContext &context, IdentifierCaseMode identifier_case_mode) : Parser(context) {
	options.identifier_case_mode = identifier_case_mode;
}

shared_ptr<CompiledGrammar> CompiledGrammar::Get(ClientContext &context) {
	auto &client_config = ClientConfig::GetConfig(context);
	auto &callback_manager = ExtensionCallbackManager::Get(context);
	if (client_config.current_dialect) {
		auto dialect_extension = callback_manager.GetDialectExtension(*client_config.current_dialect);
		if (!dialect_extension) {
			throw InternalException("Dialect extension set to '%s' but couldn't be located in the registry",
			                        *client_config.current_dialect);
		}
		return dialect_extension->GetCompiledGrammar(context);
	}
	if (client_config.cached_grammar) {
		return client_config.cached_grammar;
	}
	return DatabaseInstance::GetDatabase(context).GetParserCache().GetMatcher();
}

shared_ptr<CompiledGrammar> CompiledGrammar::Create(const ClientContext &context,
                                                    const vector<string> &active_extensions) {
	vector<reference<GrammarExtension>> selected_extensions;
	auto &callback_manager = ExtensionCallbackManager::Get(context);
	for (auto &name : active_extensions) {
		auto grammar_extension = callback_manager.FindGrammarExtension(name);
		if (grammar_extension) {
			selected_extensions.emplace_back(*grammar_extension);
		}
	}
	return Create(selected_extensions);
}

shared_ptr<CompiledGrammar> ParserCache::GetMatcher() {
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (matcher) {
			return matcher;
		}
	}
	auto new_matcher = CompiledGrammar::Create();

	std::unique_lock<std::mutex> lock(mutex);
	if (!matcher) {
		matcher = std::move(new_matcher);
	}
	return matcher;
}

} // namespace duckdb
