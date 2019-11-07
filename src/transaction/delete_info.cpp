#include "transaction/delete_info.hpp"
#include "storage/table/chunk_info.hpp"
#include "storage/table/version_manager.hpp"

using namespace duckdb;
using namespace std;

DataTable &DeleteInfo::GetTable() {
	return vinfo->manager.table;
}
