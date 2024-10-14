#pragma once

#include "duckdb.hpp"

namespace ssb {
struct SSBGenWrapper {
	static void CreateSSBSchema(duckdb::ClientContext &context, std::string catalog, std::string schema);
	static void LoadSSBData(duckdb::ClientContext &context, double sf, std::string catalog, std::string schema);
};
} // namespace ssb