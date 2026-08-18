#pragma once

#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

struct CompiledGrammar {
public:
	Matcher &ProgramMatcher() {
		return *program_matcher;
	}
	Matcher &TopLevelStatementMatcher() {
		return *top_level_statement_matcher;
	}
	const PEGKeywordHelper &GetKeywordHelper() const {
		return keyword_helper;
	}

public:
	static shared_ptr<CompiledGrammar> Get(ClientContext &context);
	static shared_ptr<CompiledGrammar> Get(DatabaseInstance &db);

public:
	//! FIXME: this should be a private detail of the parsed grammar
	const PEGTransformerFactory &GetTransformerFactory();

private:
	friend struct ParserCache;
	MatcherAllocator allocator;
	optional_ptr<Matcher> program_matcher;
	optional_ptr<Matcher> top_level_statement_matcher;

	PEGKeywordHelper keyword_helper;
	PEGTransformerFactory transformer_factory;
};

//! Per-database cache holder for the compiled PEG root matcher and transformer factory.
//! Both are always invalidated together, so they share one mutex and one Invalidate() call.
struct ParserCache {
	shared_ptr<CompiledGrammar> GetMatcher();
	void Invalidate();

private:
	std::mutex mutex;
	shared_ptr<CompiledGrammar> matcher;
};

} // namespace duckdb
