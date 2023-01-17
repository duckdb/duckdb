#include "duckdb/catalog/dcatalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {

DCatalog::DCatalog(AttachedDatabase &db) : Catalog(db), dependency_manager(make_unique<DependencyManager>(*this)) {
}

DCatalog::~DCatalog() {
}

void DCatalog::Initialize(bool load_builtin) {
	// first initialize the base system catalogs
	// these are never written to the WAL
	// we start these at 1 because deleted entries default to 0
	CatalogTransaction data(GetDatabase(), 1, 1);

	// create the default schema
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;
	CreateSchema(data, &info);

	if (load_builtin) {
		// initialize default functions
		BuiltinFunctions builtin(data, *this);
		builtin.Initialize();
	}

	Verify();
}

bool DCatalog::IsDCatalog() {
	return true;
}

}
