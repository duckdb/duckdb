
#include "common/helper.hpp"

#include "storage/data_table.hpp"

using namespace duckdb;
using namespace std;

void DataTable::AddColumn(TypeId type) {
	columns.push_back(make_unique<DataColumn>(type));
}
