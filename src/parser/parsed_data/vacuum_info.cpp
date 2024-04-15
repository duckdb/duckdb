#include "duckdb/parser/parsed_data/vacuum_info.hpp"

namespace duckdb {

VacuumInfo::VacuumInfo(VacuumOptions options) : ParseInfo(TYPE), options(options), has_table(false) {
}

unique_ptr<VacuumInfo> VacuumInfo::Copy() {
	auto result = make_uniq<VacuumInfo>(options);
	result->has_table = has_table;
	if (has_table) {
		result->ref = ref->Copy();
	}
	result->table = table;
	result->column_id_map = column_id_map;
	result->columns = columns;
	return result;
}

TableCatalogEntry &VacuumInfo::GetTable() {
	D_ASSERT(HasTable());
	return *table;
}

bool VacuumInfo::HasTable() const {
	return table != nullptr;
}

void VacuumInfo::SetTable(TableCatalogEntry &table_p) {
	table = &table_p;
}

} // namespace duckdb
