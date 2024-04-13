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
	return result;
}

string VacuumInfo::ToString() const {
	string result = "";
	result += "VACUUM";
	if (options.analyze) {
		result += " ANALYZE";
	}
	result += " " + ref->ToString();
	if (!columns.empty()) {
		result += "(" + StringUtil::Join(columns, ", ") + ")";
	}
	result += ";";
	return result;
}

} // namespace duckdb
