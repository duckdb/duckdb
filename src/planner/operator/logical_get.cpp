#include "planner/operator/logical_get.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void LogicalGet::ResolveTypes() {
	if (column_ids.size() == 0) {
		types = {TypeId::INTEGER};
	} else {
		types = table->GetTypes(column_ids);
	}
}

index_t LogicalGet::EstimateCardinality() {
	if (table) {
		return table->storage->cardinality;
	} else {
		return 1;
	}
}
