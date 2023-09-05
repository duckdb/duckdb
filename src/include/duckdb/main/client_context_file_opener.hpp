//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context_file_opener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_opener.hpp"

namespace duckdb {

class ClientContext;

//! ClientContext-specific FileOpener implementation.
//! This object is owned by ClientContext and never outlives it.
class ClientContextFileOpener : public FileOpener {
public:
	explicit ClientContextFileOpener(ClientContext &context_p) : context(context_p) {
	}

	bool TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &info) override;
	bool TryGetCurrentSetting(const string &key, Value &result) override;

	ClientContext *TryGetClientContext() override {
		return &context;
	};

private:
	ClientContext &context;
};

} // namespace duckdb
