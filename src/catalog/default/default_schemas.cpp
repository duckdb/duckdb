#include "duckdb/catalog/default/default_schemas.hpp"

namespace duckdb {

struct DefaultSchema {
	const char *name;
};

static DefaultSchema internal_schemas[] = {
	{ "information_schema" },
	{ nullptr }
};

bool DefaultSchemas::GetDefaultSchema(string schema) {
	for(idx_t index = 0; internal_schemas[index].name != nullptr; index++) {
		if (internal_schemas[index].name == schema) {
			return true;
		}
	}
	return false;
}

}
