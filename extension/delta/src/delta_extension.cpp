#include "delta_extension.hpp"
#include "delta_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Load functions
	auto &db = loader.GetDatabaseInstance();
	for (const auto &function : DeltaFunctions::GetTableFunctions(db)) {
		loader.RegisterFunction(function);
	}
}

void DeltaExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DeltaExtension::Name() {
	return "delta";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(delta, loader) {
	duckdb::LoadInternal(loader);
}
}
