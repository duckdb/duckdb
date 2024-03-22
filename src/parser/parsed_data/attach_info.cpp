#include "duckdb/parser/parsed_data/attach_info.hpp"

#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

optional_idx AttachInfo::GetBlockAllocSize() const {

	for (auto &entry : options) {
		if (entry.first == "block_size") {
			// Extract the block allocation size. This is NOT the actual memory available on a block (block_size),
			// even though the corresponding option we expose to the user is called "block_size".
			idx_t block_alloc_size = UBigIntValue::Get(entry.second.DefaultCastAs(LogicalType::UBIGINT));
			Storage::VerifyBlockAllocSize(block_alloc_size);
			return block_alloc_size;
		}
	}
	return optional_idx();
}

unique_ptr<AttachInfo> AttachInfo::Copy() const {
	auto result = make_uniq<AttachInfo>();
	result->name = name;
	result->path = path;
	result->options = options;
	result->on_conflict = on_conflict;
	return result;
}

} // namespace duckdb
