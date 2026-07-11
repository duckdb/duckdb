#include "duckdb/parser/tableref/feature_at_version_ref.hpp"

#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

string FeatureAtVersionRef::ToString() const {
	string result = "FEATURE " + SQLIdentifier(feature_name) + " AT VERSION " + duckdb::to_string(version);
	return BaseToString(result, column_name_alias);
}

bool FeatureAtVersionRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<FeatureAtVersionRef>();
	return feature_name == other.feature_name && version == other.version;
}

unique_ptr<TableRef> FeatureAtVersionRef::Copy() {
	auto copy = make_uniq<FeatureAtVersionRef>();
	copy->feature_name = feature_name;
	copy->version = version;
	CopyProperties(*copy);
	return std::move(copy);
}

} // namespace duckdb
