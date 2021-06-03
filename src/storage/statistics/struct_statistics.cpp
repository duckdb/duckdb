#include "duckdb/storage/statistics/struct_statistics.hpp"

namespace duckdb {


StructStatistics::StructStatistics(LogicalType type_p) :
	BaseStatistics(move(type_p)) {
	D_ASSERT(type.id() == LogicalTypeId::STRUCT);

	auto &child_types = type.child_types();
	child_stats.resize(child_types.size());
	for(idx_t i = 0; i < child_types.size(); i++) {
		child_stats[i] = BaseStatistics::CreateEmpty(child_types[i].second);
	}
	validity_stats = make_unique<ValidityStatistics>(false);
}

void StructStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);

	auto &other = (const StructStatistics &)other_p;
	D_ASSERT(other.child_stats.size() == child_stats.size());
	for(idx_t i = 0; i < child_stats.size(); i++) {
		if (child_stats[i] && other.child_stats[i]) {
			child_stats[i]->Merge(*other.child_stats[i]);
		}
	}
}

FilterPropagateResult StructStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) {
	// for now...
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

unique_ptr<BaseStatistics> StructStatistics::Copy() {
	auto copy = make_unique<StructStatistics>(type);
	if (validity_stats) {
		copy->validity_stats = validity_stats->Copy();
	}
	for(idx_t i = 0; i < child_stats.size(); i++) {
		if (child_stats[i]) {
			copy->child_stats[i] = child_stats[i]->Copy();
		}
	}
	return move(copy);
}

void StructStatistics::Serialize(Serializer &serializer) {
	BaseStatistics::Serialize(serializer);
	for(idx_t i = 0; i < child_stats.size(); i++) {
		D_ASSERT(child_stats[i]);
		child_stats[i]->Serialize(serializer);
	}
}

unique_ptr<BaseStatistics> StructStatistics::Deserialize(Deserializer &source, LogicalType type) {
	D_ASSERT(type.id() == LogicalTypeId::STRUCT);
	auto result = make_unique<StructStatistics>(move(type));
	auto &child_types = result->type.child_types();
	for(idx_t i = 0; i < child_types.size(); i++) {
		result->child_stats[i] = BaseStatistics::Deserialize(source, child_types[i].second);
	}
	return move(result);
}

string StructStatistics::ToString() {
	string result;
	result += " {";
	auto &child_types = type.child_types();
	for(idx_t i = 0; i < child_types.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += child_types[i].first + ": " + (child_stats[i] ? child_stats[i]->ToString() : "No Stats");
	}
	result += "}";
	result += validity_stats ? validity_stats->ToString() : "";
	return result;
}

void StructStatistics::Verify(Vector &vector, idx_t count) {
	BaseStatistics::Verify(vector, count);

	auto &child_entries = StructVector::GetEntries(vector);
	for(idx_t i = 0; i < child_entries.size(); i++) {
		child_stats[i]->Verify(*child_entries[i], count);
	}
}


}
