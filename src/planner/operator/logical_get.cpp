#include "planner/operator/logical_get.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

vector<string> LogicalGet::GetNames() {
	assert(table);
	vector<string> names;
	for (auto &column : table->columns) {
		names.push_back(column.name);
	}
	return names;
}

void LogicalGet::ResolveTypes() {
	types = table->GetTypes(column_ids);
}

size_t LogicalGet::EstimateCardinality() {
	if (table) {
		return table->storage->cardinality;
	} else {
		return 1;
	}
}
