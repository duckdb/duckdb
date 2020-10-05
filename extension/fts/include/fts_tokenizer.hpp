//===----------------------------------------------------------------------===//
//                         DuckDB
//
// fts_tokenizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class FTSTokenizer {
public:
	FTSTokenizer();

private:
    void Tokenize();
    void Tokenize(const char *text, size_t text_size);
    bool IsToken(const char &c);

    // TODO: define whitespace/token chars
    // TODO: define stemmer
    // TODO: define stopper
};

} // namespace duckdb
