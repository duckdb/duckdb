#include "duckdb/function/table/direct_file_reader.hpp"
#include "duckdb/function/table/read_file.hpp"
#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

DirectFileReader::DirectFileReader(OpenFileInfo file_p, const LogicalType &type)
    : BaseFileReader(std::move(file_p)), done(false), type(type) {
	columns.push_back(MultiFileColumnDefinition("filename", LogicalType::VARCHAR));
	columns.push_back(MultiFileColumnDefinition("content", type));
	columns.push_back(MultiFileColumnDefinition("size", LogicalType::BIGINT));
	columns.push_back(MultiFileColumnDefinition("last_modified", LogicalType::TIMESTAMP_TZ));
}

DirectFileReader::~DirectFileReader() {
}

unique_ptr<BaseStatistics> DirectFileReader::GetStatistics(ClientContext &context, const string &name) {
	return nullptr;
}

bool DirectFileReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
                                         LocalTableFunctionState &lstate) {
	auto &state = gstate.Cast<ReadFileGlobalState>();
	return file_list_idx.GetIndex() < state.file_list->GetTotalFileCount() && !done;
};

static void AssertMaxFileSize(const string &file_name, idx_t file_size) {
	const auto max_file_size = NumericLimits<uint32_t>::Maximum();
	if (file_size > max_file_size) {
		auto max_byte_size_format = StringUtil::BytesToHumanReadableString(max_file_size);
		auto file_byte_size_format = StringUtil::BytesToHumanReadableString(file_size);
		auto error_msg = StringUtil::Format("File '%s' size (%s) exceeds maximum allowed file (%s)", file_name.c_str(),
		                                    file_byte_size_format, max_byte_size_format);
		throw InvalidInputException(error_msg);
	}
}

static inline void VERIFY(const string &filename, const string_t &content) {
	if (Utf8Proc::Analyze(content.GetData(), content.GetSize()) == UnicodeType::INVALID) {
		throw InvalidInputException("read_text: could not read content of file '%s' as valid UTF-8 encoded text. You "
		                            "may want to use read_blob instead.",
		                            filename);
	}
}

void DirectFileReader::Scan(ClientContext &context, GlobalTableFunctionState &global_state,
                            LocalTableFunctionState &local_state, DataChunk &output) {
	auto &state = global_state.Cast<ReadFileGlobalState>();
	if (done || file_list_idx.GetIndex() >= state.file_list->GetTotalFileCount()) {
		return;
	}

	auto files = state.file_list;
	auto fs = CachingFileSystem::Get(context);
	idx_t out_idx = 0;

	// We utilize projection pushdown here to only read the file content if the 'data' column is requested
	unique_ptr<CachingFileHandle> file_handle = nullptr;

	// Given the columns requested, do we even need to open the file?
	if (state.requires_file_open) {
		auto flags = FileFlags::FILE_FLAGS_READ;
		if (FileSystem::IsRemoteFile(file.path)) {
			flags |= FileFlags::FILE_FLAGS_DIRECT_IO;
		}
		file_handle = fs.OpenFile(QueryContext(context), file, flags);
	}

	for (idx_t col_idx = 0; col_idx < state.column_ids.size(); col_idx++) {
		// We utilize projection pushdown to avoid potentially expensive fs operations.
		auto proj_idx = state.column_ids[col_idx];
		if (proj_idx == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		try {
			switch (proj_idx) {
			case ReadFileBindData::FILE_NAME_COLUMN: {
				auto &file_name_vector = output.data[col_idx];
				auto file_name_string = StringVector::AddString(file_name_vector, file.path);
				FlatVector::GetData<string_t>(file_name_vector)[out_idx] = file_name_string;
			} break;
			case ReadFileBindData::FILE_CONTENT_COLUMN: {
				auto file_size_raw = file_handle->GetFileSize();
				AssertMaxFileSize(file.path, file_size_raw);
				auto file_size = UnsafeNumericCast<int64_t>(file_size_raw);
				auto &file_content_vector = output.data[col_idx];
				auto content_string = StringVector::EmptyString(file_content_vector, file_size_raw);

				auto remaining_bytes = UnsafeNumericCast<int64_t>(file_size);

				// Read in batches of 100mb
				constexpr auto MAX_READ_SIZE = 100LL * 1024 * 1024;
				while (remaining_bytes > 0) {
					const auto bytes_to_read = MinValue<int64_t>(remaining_bytes, MAX_READ_SIZE);
					const auto content_string_ptr = content_string.GetDataWriteable() + (file_size - remaining_bytes);

					idx_t actually_read;
					if (file_handle->IsRemoteFile()) {
						// Remote file: caching read
						data_ptr_t read_ptr;
						actually_read = NumericCast<idx_t>(bytes_to_read);
						auto buffer_handle = file_handle->Read(read_ptr, actually_read);
						memcpy(content_string_ptr, read_ptr, actually_read);
					} else {
						// Local file: non-caching read
						actually_read = NumericCast<idx_t>(file_handle->GetFileHandle().Read(
						    content_string_ptr, UnsafeNumericCast<idx_t>(bytes_to_read)));
					}

					if (actually_read == 0) {
						// Uh oh, random EOF?
						throw IOException("Failed to read file '%s' at offset %lu, unexpected EOF", file.path,
						                  file_size - remaining_bytes);
					}
					remaining_bytes -= NumericCast<int64_t>(actually_read);
				}

				content_string.Finalize();

				if (type == LogicalType::VARCHAR) {
					VERIFY(file.path, content_string);
				}

				FlatVector::GetData<string_t>(file_content_vector)[out_idx] = content_string;
			} break;
			case ReadFileBindData::FILE_SIZE_COLUMN: {
				auto &file_size_vector = output.data[col_idx];
				FlatVector::GetData<int64_t>(file_size_vector)[out_idx] =
				    NumericCast<int64_t>(file_handle->GetFileSize());
			} break;
			case ReadFileBindData::FILE_LAST_MODIFIED_COLUMN: {
				auto &last_modified_vector = output.data[col_idx];
				// This can sometimes fail (e.g. httpfs file system cant always parse the last modified time
				// correctly)
				try {
					auto timestamp_seconds = file_handle->GetLastModifiedTime();
					FlatVector::GetData<timestamp_tz_t>(last_modified_vector)[out_idx] =
					    timestamp_tz_t(timestamp_seconds);
				} catch (std::exception &ex) {
					ErrorData error(ex);
					if (error.Type() == ExceptionType::CONVERSION) {
						FlatVector::SetNull(last_modified_vector, out_idx, true);
					} else {
						throw;
					}
				}
			} break;
			default:
				break;
			}
		}
		// Filesystems are not required to support all operations, so we just set the column to NULL if not
		// implemented
		catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() == ExceptionType::NOT_IMPLEMENTED) {
				FlatVector::SetNull(output.data[col_idx], out_idx, true);
			} else {
				throw;
			}
		}
	}
	output.SetCardinality(1);
	done = true;
};

void DirectFileReader::FinishFile(ClientContext &context, GlobalTableFunctionState &gstate) {
	return;
};

} // namespace duckdb
