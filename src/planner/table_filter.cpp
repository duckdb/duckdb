#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"

namespace duckdb {

void TableFilterSet::PushFilter(idx_t column_index, unique_ptr<TableFilter> filter) {
	auto entry = filters.find(column_index);
	if (entry == filters.end()) {
		// no filter yet: push the filter directly
		filters[column_index] = std::move(filter);
	} else {
		// there is already a filter: AND it together
		if (entry->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &and_filter = (ConjunctionAndFilter &)*entry->second;
			and_filter.child_filters.push_back(std::move(filter));
		} else {
			auto and_filter = make_unique<ConjunctionAndFilter>();
			and_filter->child_filters.push_back(std::move(entry->second));
			and_filter->child_filters.push_back(std::move(filter));
			filters[column_index] = std::move(and_filter);
		}
	}
}

//! Serializes a LogicalType to a stand-alone binary blob
void TableFilterSet::Serialize(Serializer &serializer) const {
	serializer.Write<idx_t>(filters.size());
	for (auto &entry : filters) {
		serializer.Write<idx_t>(entry.first);
		entry.second->Serialize(serializer);
	}
}

//! Deserializes a blob back into an LogicalType
unique_ptr<TableFilterSet> TableFilterSet::Deserialize(Deserializer &source) {
	auto len = source.Read<idx_t>();
	auto res = make_unique<TableFilterSet>();
	for (idx_t i = 0; i < len; i++) {
		auto key = source.Read<idx_t>();
		auto value = TableFilter::Deserialize(source);
		res->filters[key] = std::move(value);
	}
	return res;
}

//! Serializes a LogicalType to a stand-alone binary blob
void TableFilter::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<TableFilterType>(filter_type);
	Serialize(writer);
	writer.Finalize();
}

//! Deserializes a blob back into an LogicalType
unique_ptr<TableFilter> TableFilter::Deserialize(Deserializer &source) {
	unique_ptr<TableFilter> result;

	FieldReader reader(source);
	auto filter_type = reader.ReadRequired<TableFilterType>();
	switch (filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
		result = ConstantFilter::Deserialize(reader);
		break;
	case TableFilterType::CONJUNCTION_AND:
		result = ConjunctionAndFilter::Deserialize(reader);
		break;
	case TableFilterType::CONJUNCTION_OR:
		result = ConjunctionOrFilter::Deserialize(reader);
		break;
	case TableFilterType::IS_NOT_NULL:
		result = IsNotNullFilter::Deserialize(reader);
		break;
	case TableFilterType::IS_NULL:
		result = IsNullFilter::Deserialize(reader);
		break;
	default:
		throw NotImplementedException("Unsupported table filter type for deserialization");
	}
	reader.Finalize();
	return result;
}

} // namespace duckdb
