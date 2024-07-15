#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/common/string_util.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

string ColumnDataRef::ToString() const {
	auto dependency_item = external_dependency->GetDependency("materialized");
	auto &materialized_dependency = dependency_item->Cast<MaterializedDependency>();
	auto result = materialized_dependency.collection->ToString();
	return BaseToString(result, expected_names);
}

bool ColumnDataRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ColumnDataRef>();

	auto dependency_item_a = external_dependency->GetDependency("materialized");
	auto &materialized_a = dependency_item_a->Cast<MaterializedDependency>();
	auto &collection_a = materialized_a.collection;

	auto dependency_item_b = other.external_dependency->GetDependency("materialized");
	auto &materialized_b = dependency_item_b->Cast<MaterializedDependency>();
	auto &collection_b = materialized_b.collection;

	auto expected_types = collection_a->Types();
	auto other_expected_types = collection_b->Types();

	if (expected_types.size() != other_expected_types.size()) {
		return false;
	}
	if (expected_names.size() != other.expected_names.size()) {
		return false;
	}
	D_ASSERT(expected_types.size() == expected_names.size());
	for (idx_t i = 0; i < expected_types.size(); i++) {
		auto &this_type = expected_types[i];
		auto &other_type = other_expected_types[i];

		auto &this_name = expected_names[i];
		auto &other_name = other.expected_names[i];

		if (this_type != other_type) {
			return false;
		}
		if (!StringUtil::CIEquals(this_name, other_name)) {
			return false;
		}
	}
	string unused;

	if (!ColumnDataCollection::ResultEquals(*collection_a, *collection_b, unused, true)) {
		return false;
	}
	return true;
}

void ColumnDataRef::Serialize(Serializer &serializer) const {
	throw NotImplementedException(
	    "ColumnDataRef is made as part of a MaterializedRelation and should never be serialized");
}

unique_ptr<TableRef> ColumnDataRef::Deserialize(Deserializer &source) {
	throw InternalException("Can not be serialized");
}

unique_ptr<TableRef> ColumnDataRef::Copy() {
	auto dependency_item = external_dependency->GetDependency("materialized");
	auto materialized_dependency = shared_ptr_cast<DependencyItem, MaterializedDependency>(std::move(dependency_item));
	auto result = make_uniq<ColumnDataRef>(materialized_dependency, expected_names);
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
