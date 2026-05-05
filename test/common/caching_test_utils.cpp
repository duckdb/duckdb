#include "caching_test_utils.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

CachingTestFileGuard::CachingTestFileGuard(const string &filename, const string &content)
    : file_path(TestCreatePath(filename)) {
	auto local_fs = FileSystem::CreateLocal();
	auto handle = local_fs->OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write(QueryContext(), const_cast<char *>(content.data()), content.size(), 0);
	handle->Sync();
}

CachingTestFileGuard::~CachingTestFileGuard() {
	auto local_fs = FileSystem::CreateLocal();
	local_fs->TryRemoveFile(file_path);
}

const string &CachingTestFileGuard::GetPath() const {
	return file_path;
}

string SimpleTrackingFileSystem::GetName() const {
	return "TrackingFileSystem";
}

bool SimpleTrackingFileSystem::CanHandleFile(const string &path) {
	return StringUtil::StartsWith(path, TestDirectoryPath());
}

bool SimpleTrackingFileSystem::CanSeek() {
	return true;
}

string SimpleTrackingFileSystem::GetVersionTag(FileHandle &handle) {
	return StringUtil::Format("%lld:%lld", GetFileSize(handle), GetLastModifiedTime(handle).value);
}

} // namespace duckdb
