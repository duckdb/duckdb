#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

unique_ptr<BaseStatistics> ListStats::CreateUnknown(LogicalType type) {
	auto &child_type = ListType::GetChildType(type);
	auto result = make_unique<BaseStatistics>(std::move(type));
	result->InitializeUnknown();
	result->child_stats.push_back(BaseStatistics::CreateUnknown(child_type));
	return result;
}

unique_ptr<BaseStatistics> ListStats::CreateEmpty(LogicalType type) {
	auto &child_type = ListType::GetChildType(type);
	auto result = make_unique<BaseStatistics>(std::move(type));
	result->InitializeEmpty();
	result->child_stats.push_back(BaseStatistics::CreateEmpty(child_type));
	return result;
}

const unique_ptr<BaseStatistics> &ListStats::GetChildStats(const BaseStatistics &stats) {
	D_ASSERT(ListStats::IsList(stats));
	D_ASSERT(stats.child_stats.size() == 1);
	return stats.child_stats[0];
}
unique_ptr<BaseStatistics> &ListStats::GetChildStats(BaseStatistics &stats) {
	D_ASSERT(ListStats::IsList(stats));
	D_ASSERT(stats.child_stats.size() == 1);
	return stats.child_stats[0];
}

bool ListStats::IsList(const BaseStatistics &stats) {
	return stats.GetType().InternalType() == PhysicalType::LIST;
}

void ListStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}

	auto &child_stats = ListStats::GetChildStats(stats);
	auto &other_child_stats = ListStats::GetChildStats(other);
	if (child_stats && other_child_stats) {
		child_stats->Merge(*other_child_stats);
	} else {
		child_stats.reset();
	}
}

void ListStats::Serialize(const BaseStatistics &stats, FieldWriter &writer) {
	auto &child_stats = ListStats::GetChildStats(stats);
	writer.WriteOptional(child_stats);
}

unique_ptr<BaseStatistics> ListStats::Deserialize(FieldReader &reader, LogicalType type) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto &child_type = ListType::GetChildType(type);
	auto result = make_unique<BaseStatistics>(std::move(type));
	result->child_stats.push_back(reader.ReadOptional<BaseStatistics>(nullptr, child_type));
	return result;
}

string ListStats::ToString(const BaseStatistics &stats) {
	auto &child_stats = ListStats::GetChildStats(stats);
	return StringUtil::Format("[%s]", child_stats ? child_stats->ToString() : "No Stats");
}

void ListStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &child_stats = ListStats::GetChildStats(stats);
	if (!child_stats) {
		return;
	}
	auto &child_entry = ListVector::GetEntry(vector);
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);

	auto list_data = (list_entry_t *)vdata.data;
	idx_t total_list_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		auto list = list_data[index];
		if (vdata.validity.RowIsValid(index)) {
			for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
				total_list_count++;
			}
		}
	}
	SelectionVector list_sel(total_list_count);
	idx_t list_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		auto list = list_data[index];
		if (vdata.validity.RowIsValid(index)) {
			for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
				list_sel.set_index(list_count++, list.offset + list_idx);
			}
		}
	}

	child_stats->Verify(child_entry, list_sel, list_count);
}

} // namespace duckdb
