#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"

#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/serializer/async_memory_governor.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

MultiFileReadAhead::MultiFileReadAhead(ClientContext &context, idx_t read_ahead_depth_p,
                                       unique_ptr<ManagedAsyncMemoryGovernor> memory_governor_p)
    : ScanReadAhead(context, read_ahead_depth_p, std::move(memory_governor_p)) {
}

MultiFileReadAhead::~MultiFileReadAhead() {
}

unique_ptr<MultiFileReadAhead> MultiFileReadAhead::Create(ClientContext &context) {
	const auto configured_depth = Settings::Get<ReadAheadDepthSetting>(context);
	if (configured_depth == 0) {
		return nullptr;
	}
	if (configured_depth == -1) {
		// automatic mode, unlimited depth, the backlog is bounded by a temp-memory reservation instead
		return make_uniq<MultiFileReadAhead>(context, NumericLimits<idx_t>::Maximum(),
		                                     make_uniq<ManagedAsyncMemoryGovernor>(context));
	}
	return make_uniq<MultiFileReadAhead>(context, NumericCast<idx_t>(configured_depth), nullptr);
}

MultiFileGlobalState::MultiFileGlobalState(MultiFileList &file_list_p) : file_list(file_list_p) {
}

MultiFileGlobalState::MultiFileGlobalState(unique_ptr<MultiFileList> owned_file_list_p)
    : file_list(*owned_file_list_p), owned_file_list(std::move(owned_file_list_p)) {
}

MultiFileGlobalState::~MultiFileGlobalState() = default;

MultiFileLocalState::~MultiFileLocalState() {
	// job reads might still be going, wait for them before destroying ze job
	if (job_state == MultiFileJobState::WAIT_IO && job.io_completion) {
		job.io_completion->WaitForIO();
	}
}

} // namespace duckdb
