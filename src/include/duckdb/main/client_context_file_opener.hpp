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

	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &info) override;
	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) override;

	optional_ptr<ClientContext> TryGetClientContext() override {
		return &context;
	}
	optional_ptr<DatabaseInstance> TryGetDatabase() override;

private:
	ClientContext &context;
};

} // namespace duckdb
