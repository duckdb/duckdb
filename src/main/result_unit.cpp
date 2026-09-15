#include "duckdb/main/result_unit.hpp"

namespace duckdb {

ResultUnit::ResultUnit(ResultUnitType type_p) : type(type_p) {
}

ResultUnit::~ResultUnit() {
}

ChunkUnit::ChunkUnit(unique_ptr<DataChunk> chunk_p) : ResultUnit(ResultUnitType::CHUNK), chunk(std::move(chunk_p)) {
	D_ASSERT(chunk);
	row_count = chunk->size();
	byte_size = chunk->GetDataSize();
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
