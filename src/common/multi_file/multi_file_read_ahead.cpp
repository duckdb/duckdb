#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/serializer/async_memory_governor.hpp"

namespace duckdb {

MultiFileReadAhead::MultiFileReadAhead(ClientContext &context, idx_t read_ahead_depth_p,
                                       unique_ptr<ManagedAsyncMemoryGovernor> memory_governor_p)
    : ScanReadAhead(context, read_ahead_depth_p, std::move(memory_governor_p)) {
}

MultiFileReadAhead::~MultiFileReadAhead() {
}

unique_ptr<MultiFileReadAhead> MultiFileReadAhead::Create(ClientContext &context) {
	optional_idx depth;
	if (!TryGetReadAheadDepth(context, depth)) {
		return nullptr;
	}
	if (!depth.IsValid()) {
		// automatic mode, unlimited depth, the backlog is bounded by a temp-memory reservation instead
		return make_uniq<MultiFileReadAhead>(context, NumericLimits<idx_t>::Maximum(),
		                                     make_uniq<ManagedAsyncMemoryGovernor>(context));
	}
	return make_uniq<MultiFileReadAhead>(context, depth.GetIndex(), nullptr);
}

MultiFileGlobalState::MultiFileGlobalState(MultiFileList &file_list_p) : file_list(file_list_p) {
}

MultiFileGlobalState::MultiFileGlobalState(unique_ptr<MultiFileList> owned_file_list_p)
    : file_list(*owned_file_list_p), owned_file_list(std::move(owned_file_list_p)) {
}

MultiFileGlobalState::~MultiFileGlobalState() = default;

} // namespace duckdb
