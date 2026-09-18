#include "duckdb/main/result_unit.hpp"

#include "duckdb/main/buffered_data/buffered_data.hpp"

namespace duckdb {

ResultUnit::ResultUnit(idx_t row_count_p, idx_t byte_size_p) : row_count(row_count_p), byte_size(byte_size_p) {
}

ResultUnit::~ResultUnit() {
}

ChunkUnit::ChunkUnit(unique_ptr<DataChunk> chunk_p)
    : ResultUnit(chunk_p->size(), chunk_p->GetDataSize()), chunk(std::move(chunk_p)) {
}

unique_ptr<ResultUnit> ChunkUnit::Copy() const {
	return make_uniq<ChunkUnit>(BufferedData::CopyForBuffering(*chunk));
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

// Relies on units being declared before total_rows: members initialize in declaration order
ResultUnitCollection::ResultUnitCollection(vector<unique_ptr<ResultUnit>> units_p)
    : units(std::move(units_p)), total_rows(TotalRows(units)) {
}

ResultUnitCollection::~ResultUnitCollection() {
}

} // namespace duckdb
