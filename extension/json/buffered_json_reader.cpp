#include "buffered_json_reader.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

JSONFileHandle::JSONFileHandle(unique_ptr<FileHandle> file_handle_p)
    : file_handle(move(file_handle_p)), can_seek(file_handle->CanSeek()),
      plain_file_source(file_handle->OnDiskFile() && can_seek), file_size(file_handle->GetFileSize()),
      read_position(0) {
}

idx_t JSONFileHandle::Remaining() const {
	return file_size - read_position;
}

void JSONFileHandle::Read(data_ptr_t pointer, idx_t size) {
	if (plain_file_source) {
		D_ASSERT(size < Remaining());
		file_handle->Read((void *)pointer, size, read_position);
		read_position += size;
		return;
	}
	throw NotImplementedException("Non-plain file source JSON");
}

BufferedJSONReader::BufferedJSONReader(ClientContext &context, BufferedJSONReaderOptions options)
    : context(context), options(move(options)), allocator(BufferAllocator::Get(context)),
      file_system(FileSystem::GetFileSystem(context)) {
}

void BufferedJSONReader::Initialize() {
	OpenJSONFile();
	buffer_capacity = file_handle->Remaining() < INITIAL_BUFFER_CAPACITY
	                      ? file_handle->Remaining()
	                      : MaxValue<idx_t>(INITIAL_BUFFER_CAPACITY, 2 * options.maximum_object_size);
}

void BufferedJSONReader::OpenJSONFile() {
	file_handle = make_unique<JSONFileHandle>(file_system.OpenFile(options.file_path.c_str(),
	                                                               FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK,
	                                                               options.compression, FileOpener::Get(context)));
}

AllocatedData BufferedJSONReader::AllocateBuffer() {
	return allocator.Allocate(buffer_capacity);
}

} // namespace duckdb
