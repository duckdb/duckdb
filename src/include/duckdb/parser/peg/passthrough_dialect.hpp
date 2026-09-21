//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/peg/passthrough_dialect.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/peg/dialect_extension.hpp"

namespace duckdb {

//! The grammar used while a client is CONNECT-ed to a database that parses nothing locally:
//!
//!   Statement            <- DisconnectStatement / PassthroughStatement
//!   PassthroughStatement <- StatementToken+
//!
//! DISCONNECT stays interpreted so the client can always end the connection; every other statement is handed to
//! the remote verbatim. The tokenizer is unchanged, so statement boundaries are DuckDB's.
class PassthroughDialect : public DialectExtension {
public:
	static constexpr const char *NAME = "passthrough";

public:
	PassthroughDialect() : DialectExtension(NAME) {
	}

	void ApplyGrammarChanges(GrammarChangesInput &input) override;
};

} // namespace duckdb
