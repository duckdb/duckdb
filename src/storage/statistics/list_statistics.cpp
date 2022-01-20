#include "duckdb/storage/statistics/list_statistics.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/field_writer.hpp"

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
	} else {
		child_stats.reset();
	}
}

// LCOV_EXCL_START
FilterPropagateResult ListStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) const {
	throw InternalException("List zonemaps are not supported yet");
}
// LCOV_EXCL_STOP

unique_ptr<BaseStatistics> ListStatistics::Copy() const {
	auto copy = make_unique<ListStatistics>(type);
	copy->validity_stats = validity_stats ? validity_stats->Copy() : nullptr;
	copy->child_stats = child_stats ? child_stats->Copy() : nullptr;
	return move(copy);
}

void ListStatistics::Serialize(FieldWriter &writer) const {
	writer.WriteSerializable(*child_stats);
}

unique_ptr<BaseStatistics> ListStatistics::Deserialize(FieldReader &reader, LogicalType type) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto result = make_unique<ListStatistics>(move(type));
	auto &child_type = ListType::GetChildType(result->type);
	auto &source = reader.GetSource();
	result->child_stats = BaseStatistics::Deserialize(source, child_type);
	return move(result);
}

string ListStatistics::ToString() const {
	string result;
	result += " [";
	result += child_stats ? child_stats->ToString() : "No Stats";
	result += "]";
	result += validity_stats ? validity_stats->ToString() : "";
	return result;
}

void ListStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	if (child_stats) {
		auto &child_entry = ListVector::GetEntry(vector);
		VectorData vdata;
		vector.Orrify(count, vdata);

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
}

} // namespace duckdb
