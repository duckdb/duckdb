#include "planner/operator/logical_index_scan.hpp"

using namespace duckdb;
using namespace std;


void LogicalIndexScan::ResolveTypes() {
    types = children[0]->types;
}

