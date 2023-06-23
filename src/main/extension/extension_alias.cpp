#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

static ExtensionAlias internal_aliases[] = {{"http", "httpfs"}, // httpfs
                                            {"https", "httpfs"},
                                            {"md", "motherduck"}, // motherduck
                                            {"s3", "httpfs"},
                                            {"postgres", "postgres_scanner"}, // postgres
                                            {"sqlite", "sqlite_scanner"},     // sqlite
                                            {"sqlite3", "sqlite_scanner"},
                                            {nullptr, nullptr}};

idx_t ExtensionHelper::ExtensionAliasCount() {
	idx_t index;
	for (index = 0; internal_aliases[index].alias != nullptr; index++) {
	}
	return index;
}

ExtensionAlias ExtensionHelper::GetExtensionAlias(idx_t index) {
	D_ASSERT(index < ExtensionAliasCount());
	return internal_aliases[index];
}

string ExtensionHelper::ApplyExtensionAlias(string extension_name) {
	auto lname = StringUtil::Lower(extension_name);
	for (idx_t index = 0; internal_aliases[index].alias; index++) {
		if (lname == internal_aliases[index].alias) {
			return internal_aliases[index].extension;
		}
	}
	return extension_name;
}

} // namespace duckdb
