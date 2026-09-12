#include "duckdb/main/result_unit.hpp"

namespace duckdb {

ResultUnit::ResultUnit(idx_t row_count_p, idx_t byte_size_p) : row_count(row_count_p), byte_size(byte_size_p) {
}

ResultUnit::~ResultUnit() {
}

ChunkUnit::ChunkUnit(unique_ptr<DataChunk> chunk_p)
    : ResultUnit(chunk_p->size(), chunk_p->GetDataSize()), chunk(std::move(chunk_p)) {
}

static idx_t TotalRows(const vector<unique_ptr<ResultUnit>> &units) {
	idx_t count = 0;
	for (auto &unit : units) {
		count += unit->row_count;
	}
	return count;
}

ResultUnitCollection::ResultUnitCollection() {
}

ResultUnitCollection::ResultUnitCollection(vector<unique_ptr<ResultUnit>> units_p)
    : total_rows(TotalRows(units_p)), total_units(units_p.size()) {
	// Filled here, not in the initializer list: the totals above read units_p before it is moved from
	for (auto &unit : units_p) {
		units.push_back(std::move(unit));
	}
}

ResultUnitCollection::~ResultUnitCollection() {
}

unique_ptr<ResultUnit> ResultUnitCollection::Fetch() {
	if (units.empty()) {
		return nullptr;
	}
	auto unit = std::move(units.front());
	units.pop_front();
	return unit;
}

} // namespace duckdb
