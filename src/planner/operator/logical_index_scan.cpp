#include "planner/operator/logical_index_scan.hpp"

#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

// void LogicalIndexScan::ResolveTypes() {
//	if (column_ids.size() == 0) {
//		types = {TypeId::INTEGER};
//	} else {
//		types = tableref.GetTypes(column_ids);
//	}
//}
