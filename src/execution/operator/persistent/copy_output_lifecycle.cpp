#include "duckdb/execution/operator/persistent/copy_output_lifecycle.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>

namespace duckdb {

CopyOutputLifecycle::CopyOutputLifecycle(ClientContext &context_p) : context(context_p) {
}

CopyOutputLifecycle::~CopyOutputLifecycle() {
	try {
		Cleanup();
	} catch (...) { // NOLINT
	}
}

void CopyOutputLifecycle::Cleanup() {
	vector<string> committed_files;
	vector<string> directories;
	{
		lock_guard<mutex> guard(lock);
		if (successful) {
			return;
		}
		for (auto &file : files) {
			if (file.committed) {
				committed_files.push_back(std::move(file.path));
			}
		}
		directories = std::move(created_directories);
	}

	auto &fs = FileSystem::GetFileSystem(context);
	if (!committed_files.empty()) {
		try {
			fs.RemoveFiles(committed_files);
		} catch (...) { // NOLINT
			for (auto &file : committed_files) {
				try {
					fs.TryRemoveFile(file);
				} catch (...) { // NOLINT
				}
			}
		}
	}

	std::sort(directories.begin(), directories.end(), [](const string &left, const string &right) {
		if (left.size() != right.size()) {
			return left.size() > right.size();
		}
		return left < right;
	});
	directories.erase(std::unique(directories.begin(), directories.end()), directories.end());
	for (auto &directory : directories) {
		try {
			fs.TryRemoveEmptyDirectory(directory);
		} catch (...) { // NOLINT
		}
	}
}

idx_t CopyOutputLifecycle::RegisterFile(string path) {
	lock_guard<mutex> guard(lock);
	auto result = files.size();
	files.push_back({std::move(path), false});
	return result;
}

void CopyOutputLifecycle::UpdateFile(idx_t file_index, string path) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(file_index < files.size());
	D_ASSERT(!files[file_index].committed);
	files[file_index].path = std::move(path);
}

void CopyOutputLifecycle::MarkFileCommitted(idx_t file_index) {
	lock_guard<mutex> guard(lock);
	D_ASSERT(file_index < files.size());
	files[file_index].committed = true;
}

void CopyOutputLifecycle::ReplaceCommittedFile(const string &source, string target) {
	lock_guard<mutex> guard(lock);
	for (auto &file : files) {
		if (file.committed && file.path == source) {
			file.path.swap(target);
			return;
		}
	}
	D_ASSERT(false);
}

void CopyOutputLifecycle::RegisterCreatedDirectory(string path) {
	lock_guard<mutex> guard(lock);
	created_directories.push_back(std::move(path));
}

void CopyOutputLifecycle::MarkSuccessful() {
	lock_guard<mutex> guard(lock);
	successful = true;
}

GlobalFileState::GlobalFileState(unique_ptr<GlobalFunctionData> data_p, string path_p, ClientContext &context_p,
                                 FunctionData &bind_data_p, copy_to_abort_t abort_p,
                                 CopyOutputLifecycle &output_lifecycle_p, idx_t lifecycle_file_index_p)
    : data(std::move(data_p)), path(std::move(path_p)), context(context_p), bind_data(bind_data_p), abort(abort_p),
      output_lifecycle(output_lifecycle_p), lifecycle_file_index(lifecycle_file_index_p) {
}

GlobalFileState::~GlobalFileState() {
	if (!data || !abort) {
		return;
	}
	try {
		abort(context, bind_data, *data);
	} catch (...) { // NOLINT
	}
}

void GlobalFileState::MarkFinalized() {
	output_lifecycle.MarkFileCommitted(lifecycle_file_index);
	data.reset();
}

} // namespace duckdb
