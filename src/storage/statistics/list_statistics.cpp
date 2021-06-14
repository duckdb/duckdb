#include "duckdb/storage/statistics/list_statistics.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

ListStatistics::ListStatistics(LogicalType type_p) : BaseStatistics(move(type_p)) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);

	auto &child_type = ListType::GetChildType(type);
	child_stats = BaseStatistics::CreateEmpty(child_type);
	validity_stats = make_unique<ValidityStatistics>(false);
}

void ListStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);

	auto &other = (const ListStatistics &)other_p;
	if (child_stats && other.child_stats) {
		child_stats->Merge(*other.child_stats);
	}
}

FilterPropagateResult ListStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) {
	// for now...
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

unique_ptr<BaseStatistics> ListStatistics::Copy() {
	auto copy = make_unique<ListStatistics>(type);
	if (validity_stats) {
		copy->validity_stats = validity_stats->Copy();
	}
	if (child_stats) {
		copy->child_stats = child_stats->Copy();
	}
	return move(copy);
}

void ListStatistics::Serialize(Serializer &serializer) {
	BaseStatistics::Serialize(serializer);
	child_stats->Serialize(serializer);
}

unique_ptr<BaseStatistics> ListStatistics::Deserialize(Deserializer &source, LogicalType type) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto result = make_unique<ListStatistics>(move(type));
	auto &child_type = ListType::GetChildType(result->type);
	result->child_stats = BaseStatistics::Deserialize(source, child_type);
	return move(result);
}

string ListStatistics::ToString() {
	string result;
	result += " [";
	result += child_stats ? child_stats->ToString() : "No Stats";
	result += "]";
	result += validity_stats ? validity_stats->ToString() : "";
	return result;
}

void ListStatistics::Verify(Vector &vector, idx_t count) {
	BaseStatistics::Verify(vector, count);


	auto &child_entry = ListVector::GetEntry(vector);
	child_stats->Verify(child_entry, count);
}

} // namespace duckdb
