//===----------------------------------------------------------------------===//
//                         DuckDB
//
// fts-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

#ifndef CHAR_ENC
#define CHAR_ENC "UTF_8"
#endif

typedef unsigned char sb_symbol;

class FTSExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};

} // namespace duckdb
