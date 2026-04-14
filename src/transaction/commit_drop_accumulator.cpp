#include "duckdb/transaction/commit_drop_accumulator.hpp"

#include "duckdb/execution/index/fixed_size_buffer.hpp"
#include "duckdb/storage/block_manager.hpp"

namespace duckdb {

void CommitDropAccumulator::Apply() {
	for (auto &m : block_marks) {
		m.bm.get().MarkBlockAsModified(m.id);
	}
	for (auto &m : buffer_marks) {
		m.bm.get().MarkBlockAsModified(m.id);
	}
	Clear();
}

} // namespace duckdb
