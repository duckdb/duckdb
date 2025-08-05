#pragma once

#include "duckdb/parser/parser_override.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

class PEGParserOverride : public ParserOverride {
public:
	explicit PEGParserOverride(unique_ptr<ParserOverrideOptions> options, ClientContext &context);

	vector<unique_ptr<SQLStatement>> Parse(const string &query) override;

private:
	unique_ptr<PEGTransformerFactory> factory;
};

} // namespace duckdb
