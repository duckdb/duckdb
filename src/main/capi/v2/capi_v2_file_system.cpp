#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/query_context.hpp"

namespace duckdb::capiv2 {

inline auto GetFileSystemSlot(ClientContext &context) -> shared_ptr<CV2FileSystem> {
	constexpr auto FILE_SYSTEM_SLOT_KEY = "c_api_v2_file_system";
	auto slot = context.registered_state->GetOrCreate<CV2FileSystem>(FILE_SYSTEM_SLOT_KEY);
	slot->fs = &FileSystem::GetFileSystem(context);
	slot->query = context;
	return slot;
}

// A lock request rides alongside the bit flags; exclusive wins over shared whichever order they are applied in.
static void SetLock(FileOpenFlags &flags, FileLockType lock) {
	if (flags.Lock() == FileLockType::WRITE_LOCK) {
		return;
	}
	auto caching = flags.GetCachingMode();
	flags = FileOpenFlags(flags.GetFlagsInternal(), lock, flags.Compression());
	flags.SetCachingMode(caching);
}

// Applies one C flag to the engine's flag set. The C enum is a list of names rather than a bitmask, so each value
// maps to exactly one engine flag and anything else is a caller error.
static void ApplyFileFlag(FileOpenFlags &flags, DUCKDB_V2_FILE_FLAG flag) {
	switch (flag) {
	case DUCKDB_V2_FILE_FLAG_READ:
		flags |= FileOpenFlags::FILE_FLAGS_READ;
		break;
	case DUCKDB_V2_FILE_FLAG_WRITE:
		flags |= FileOpenFlags::FILE_FLAGS_WRITE;
		break;
	case DUCKDB_V2_FILE_FLAG_CREATE:
		flags |= FileOpenFlags::FILE_FLAGS_FILE_CREATE;
		break;
	case DUCKDB_V2_FILE_FLAG_CREATE_NEW:
		flags |= FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW;
		break;
	case DUCKDB_V2_FILE_FLAG_APPEND:
		flags |= FileOpenFlags::FILE_FLAGS_APPEND;
		break;
	case DUCKDB_V2_FILE_FLAG_EXCLUSIVE_CREATE:
		flags |= FileOpenFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
		break;
	case DUCKDB_V2_FILE_FLAG_PARALLEL_ACCESS:
		flags |= FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS;
		break;
	case DUCKDB_V2_FILE_FLAG_SHARED_LOCK:
		SetLock(flags, FileLockType::READ_LOCK);
		break;
	case DUCKDB_V2_FILE_FLAG_EXCLUSIVE_LOCK:
		SetLock(flags, FileLockType::WRITE_LOCK);
		break;
	default:
		// Includes FILE_FLAG_INVALID, which names no behaviour.
		throw InvalidInputException("'%d' is not a file flag.", static_cast<int>(flag));
	}
}

static bool IsFileType(DUCKDB_V2_FILE_TYPE type) {
	switch (type) {
	case DUCKDB_V2_FILE_TYPE_REGULAR:
	case DUCKDB_V2_FILE_TYPE_DIRECTORY:
	case DUCKDB_V2_FILE_TYPE_PIPE:
	case DUCKDB_V2_FILE_TYPE_OTHER:
		return true;
	default:
		return false;
	}
}

static CV2FileListing::Entry &EntryAt(duckdb_v2_file_listing_handle listing, idx_t index, const char *function) {
	auto &entries = Convert(listing)->entries;
	if (index >= entries.size()) {
		throw InvalidInputException("Index out of bounds in %s", function);
	}
	return entries[index];
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_file_system_get_from_context(duckdb_v2_context_handle context,
                                                       duckdb_v2_file_system_handle *out_file_system,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(context);
	DUCKDB_CHECK_ARG(out_file_system);
	*out_file_system = nullptr;
	return WithErrorHandler(err, [&]() { *out_file_system = Convert(GetFileSystemSlot(*Convert(context)).get()); });
}

DUCKDB_V2_ERROR duckdb_v2_file_system_get_from_connection(duckdb_v2_connection_handle connection,
                                                          duckdb_v2_file_system_handle *out_file_system,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(connection);
	DUCKDB_CHECK_ARG(out_file_system);
	*out_file_system = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &context = *Convert(connection)->context;
		*out_file_system = Convert(GetFileSystemSlot(context).get());
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_system_open(duckdb_v2_file_system_handle file_system, duckdb_v2_str file_path,
                                           const DUCKDB_V2_FILE_FLAG *flags, idx_t flag_count,
                                           duckdb_v2_file_metadata_handle metadata,
                                           duckdb_v2_file_handle *out_file_handle, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(file_path);
	DUCKDB_CHECK_ARG(out_file_handle);
	*out_file_handle = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &slot = *Convert(file_system);
		if (!flags || flag_count == 0) {
			throw duckdb::InvalidInputException(
			    "No flags were passed, so the open cannot say whether the file is being read or written.");
		}
		duckdb::FileOpenFlags open_flags;
		for (idx_t i = 0; i < flag_count; i++) {
			ApplyFileFlag(open_flags, flags[i]);
		}

		duckdb::OpenFileInfo info(duckdb::string(Convert(file_path)));
		if (metadata && !Convert(metadata)->IsEmpty()) {
			// An absent extended info is meaningfully different from an empty one.
			info.extended_info = duckdb::make_shared_ptr<duckdb::ExtendedOpenFileInfo>();
			Convert(metadata)->FillOptions(*info.extended_info);
		}

		// No opener is passed: FileSystem::GetFileSystem hands back the context's own OpenerFileSystem, which
		// pushes the opener itself -- which is how a remote file system reaches settings and secrets. Supplying one
		// here is rejected outright ("the opener is pushed automatically").
		auto handle = slot.fs->OpenFile(info, open_flags);
		if (!handle) {
			throw duckdb::IOException("Failed to open file: %s", info.path);
		}
		auto file = duckdb::make_uniq<CV2File>();
		file->handle = std::move(handle);
		file->query = slot.query;
		*out_file_handle = Convert(file.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_system_stat(duckdb_v2_file_system_handle file_system, duckdb_v2_str path,
                                           duckdb_v2_file_metadata_handle *metadata, bool *exists,
                                           duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(path);
	DUCKDB_CHECK_ARG(metadata);
	DUCKDB_CHECK_ARG(exists);
	*metadata = nullptr;
	*exists = false;
	return WithErrorHandler(err, [&]() {
		auto &slot = *Convert(file_system);
		auto engine_metadata = slot.fs->GetStatsIfExists(duckdb::OpenFileInfo(duckdb::string(Convert(path))));
		if (!engine_metadata) {
			return;
		}
		auto result = duckdb::make_uniq<CV2FileMetadata>(std::move(*engine_metadata));
		if (!result->HasType()) {
			// It exists, and the engine could not say what it is.
			result->data.file_type = CV2FileMetadata::ToEngineType(DUCKDB_V2_FILE_TYPE_OTHER);
		}
		*metadata = Convert(result.release());
		*exists = true;
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_system_list(duckdb_v2_file_system_handle file_system, duckdb_v2_str path,
                                           duckdb_v2_file_listing_handle *listing, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(path);
	DUCKDB_CHECK_ARG(listing);
	*listing = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &slot = *Convert(file_system);
		auto p = duckdb::string(Convert(path));
		auto result = duckdb::make_uniq<CV2FileListing>();
		auto found = slot.fs->ListFiles(p, [&](duckdb::OpenFileInfo &info) { result->Add(info); });
		if (!found) {
			throw duckdb::FileNotFoundException("Cannot list \"%s\": no such directory", p);
		}
		*listing = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_system_glob(duckdb_v2_file_system_handle file_system, duckdb_v2_str pattern,
                                           duckdb_v2_file_listing_handle *listing, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(pattern);
	DUCKDB_CHECK_ARG(listing);
	*listing = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &slot = *Convert(file_system);
		auto result = duckdb::make_uniq<CV2FileListing>();
		auto files = slot.fs->GlobFileList(duckdb::string(Convert(pattern)), duckdb::FileGlobOptions::ALLOW_EMPTY);
		for (auto &info : files->GetAllFiles()) {
			result->Add(info);
		}
		*listing = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_system_remove_file(duckdb_v2_file_system_handle file_system, duckdb_v2_str path,
                                                  duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() {
		auto &slot = *Convert(file_system);
		slot.fs->RemoveFile(duckdb::string(Convert(path)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_system_create_directory(duckdb_v2_file_system_handle file_system, duckdb_v2_str path,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() {
		auto &slot = *Convert(file_system);
		slot.fs->CreateDirectoryExtended(duckdb::string(Convert(path)), {duckdb::CreateDirectoryMode::RECURSIVE});
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_system_remove_directory(duckdb_v2_file_system_handle file_system, duckdb_v2_str path,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() {
		auto &slot = *Convert(file_system);
		auto p = duckdb::string(Convert(path));
		if (slot.fs->RemoveDirectoryExtended(p, {duckdb::RemoveDirectoryMode::RECURSIVE})) {
			return;
		}
		if (slot.fs->DirectoryExists(p)) {
			throw duckdb::IOException("Could not remove directory \"%s\"", p);
		}
		throw duckdb::FileNotFoundException("Cannot remove \"%s\": no such directory", p);
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_system_move(duckdb_v2_file_system_handle file_system, duckdb_v2_str source,
                                           duckdb_v2_str target, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file_system);
	DUCKDB_CHECK_ARG(source);
	DUCKDB_CHECK_ARG(target);
	return WithErrorHandler(err, [&]() {
		auto &slot = *Convert(file_system);
		slot.fs->MoveFile(duckdb::string(Convert(source)), duckdb::string(Convert(target)));
	});
}

//----------------------------------------------------------------------------------------------------------------------
// File
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_file_read(duckdb_v2_file_handle file, void *buffer, idx_t buffer_size, idx_t *bytes_read,
                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	DUCKDB_CHECK_ARG(buffer);
	DUCKDB_CHECK_ARG(bytes_read);
	return WithErrorHandler(err, [&]() {
		auto &f = *Convert(file);
		*bytes_read = duckdb::NumericCast<idx_t>(f.Handle().Read(f.query, buffer, buffer_size));
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_write(duckdb_v2_file_handle file, const void *buffer, idx_t buffer_size,
                                     idx_t *bytes_written, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	DUCKDB_CHECK_ARG(buffer);
	DUCKDB_CHECK_ARG(bytes_written);
	return WithErrorHandler(err, [&]() {
		// The engine's Write takes a mutable pointer but does not write through it.
		auto *data = const_cast<void *>(buffer); // NOLINT: the engine's signature is not const-correct
		auto &f = *Convert(file);
		*bytes_written = duckdb::NumericCast<idx_t>(f.Handle().Write(f.query, data, buffer_size));
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_read_at(duckdb_v2_file_handle file, void *buffer, idx_t buffer_size, idx_t location,
                                       idx_t *bytes_read, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	DUCKDB_CHECK_ARG(buffer);
	DUCKDB_CHECK_ARG(bytes_read);
	*bytes_read = 0;
	return WithErrorHandler(err, [&]() {
		auto &f = *Convert(file);
		// The engine's positional read is all or nothing, so a read crossing the end of the file is clamped to it.
		auto count = buffer_size;
		if (count > duckdb::NumericLimits<idx_t>::Maximum() - location || location + count > f.known_size) {
			// The file may have grown since it last reported its size.
			auto size = duckdb::NumericCast<idx_t>(f.Handle().GetFileSize());
			f.known_size = size;
			count = location >= size ? 0 : duckdb::MinValue<idx_t>(count, size - location);
		}
		if (count > 0) {
			f.Handle().Read(f.query, buffer, count, location);
		}
		*bytes_read = count;
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_write_at(duckdb_v2_file_handle file, const void *buffer, idx_t buffer_size,
                                        idx_t location, idx_t *bytes_written, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	DUCKDB_CHECK_ARG(buffer);
	DUCKDB_CHECK_ARG(bytes_written);
	*bytes_written = 0;
	return WithErrorHandler(err, [&]() {
		auto *data = const_cast<void *>(buffer); // NOLINT: the engine's signature is not const-correct
		auto &f = *Convert(file);
		// The engine's positional write is all or nothing.
		f.Handle().Write(f.query, data, buffer_size, location);
		*bytes_written = buffer_size;
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_tell(duckdb_v2_file_handle file, idx_t *position, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	DUCKDB_CHECK_ARG(position);
	return WithErrorHandler(err, [&]() { *position = Convert(file)->Handle().SeekPosition(); });
}

DUCKDB_V2_ERROR duckdb_v2_file_size(duckdb_v2_file_handle file, idx_t *size, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	DUCKDB_CHECK_ARG(size);
	return WithErrorHandler(err, [&]() { *size = Convert(file)->Handle().GetFileSize(); });
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_create(duckdb_v2_file_metadata_handle *metadata,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	*metadata = nullptr;
	return WithErrorHandler(err, [&]() { *metadata = Convert(duckdb::make_uniq<CV2FileMetadata>().release()); });
}

DUCKDB_V2_ERROR duckdb_v2_file_stat(duckdb_v2_file_handle file, duckdb_v2_file_metadata_handle *metadata,
                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	DUCKDB_CHECK_ARG(metadata);
	*metadata = nullptr;
	return WithErrorHandler(err, [&]() {
		auto result = duckdb::make_uniq<CV2FileMetadata>(Convert(file)->Handle().Stats());
		*metadata = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_seek(duckdb_v2_file_handle file, idx_t position, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	return WithErrorHandler(err, [&]() { Convert(file)->Handle().Seek(position); });
}

DUCKDB_V2_ERROR duckdb_v2_file_sync(duckdb_v2_file_handle file, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	return WithErrorHandler(err, [&]() { Convert(file)->Handle().Sync(); });
}

DUCKDB_V2_ERROR duckdb_v2_file_truncate(duckdb_v2_file_handle file, idx_t size, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	return WithErrorHandler(err, [&]() { Convert(file)->Handle().Truncate(duckdb::NumericCast<int64_t>(size)); });
}

DUCKDB_V2_ERROR duckdb_v2_file_abort(duckdb_v2_file_handle file, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	return WithErrorHandler(err, [&]() { Convert(file)->Handle().AbortWrite(); });
}

DUCKDB_V2_ERROR duckdb_v2_file_close(duckdb_v2_file_handle file, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(file);
	return WithErrorHandler(err, [&]() { Convert(file)->Handle().Close(); });
}

//----------------------------------------------------------------------------------------------------------------------
// File Metadata
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_file_metadata_get_type(duckdb_v2_file_metadata_handle metadata, DUCKDB_V2_FILE_TYPE *type,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	DUCKDB_CHECK_ARG(type);
	return WithErrorHandler(err, [&]() { *type = Convert(metadata)->Type(); });
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_get_size(duckdb_v2_file_metadata_handle metadata, idx_t *size, bool *is_known,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	DUCKDB_CHECK_ARG(size);
	DUCKDB_CHECK_ARG(is_known);
	return WithErrorHandler(err, [&]() {
		auto &info = *Convert(metadata);
		*is_known = info.HasSize();
		*size = info.HasSize() ? duckdb::NumericCast<idx_t>(info.data.file_size) : 0;
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_get_last_modified(duckdb_v2_file_metadata_handle metadata,
                                                          int64_t *last_modified, bool *is_known,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	DUCKDB_CHECK_ARG(last_modified);
	DUCKDB_CHECK_ARG(is_known);
	return WithErrorHandler(err, [&]() {
		auto &info = *Convert(metadata);
		*is_known = info.HasLastModified();
		*last_modified = info.HasLastModified() ? info.data.last_modification_time.value : 0;
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_get_version_tag(duckdb_v2_file_metadata_handle metadata,
                                                        duckdb_v2_str *version_tag, bool *is_known,
                                                        duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	DUCKDB_CHECK_ARG(version_tag);
	DUCKDB_CHECK_ARG(is_known);
	return WithErrorHandler(err, [&]() {
		auto &info = *Convert(metadata);
		*is_known = info.HasVersionTag();
		*version_tag = info.HasVersionTag() ? Convert(info.data.version_tag) : duckdb_v2_str {nullptr, 0};
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_get_value(duckdb_v2_file_metadata_handle metadata, duckdb_v2_str name,
                                                  duckdb_v2_value_handle *value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(value);
	*value = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &values = Convert(metadata)->data.extended_file_info;
		// An option the engine knows is stored under its canonical name, whatever the case it is asked for in.
		duckdb::ExtendedOpenFileInfo canonical;
		canonical.SetUserOption(duckdb::string(Convert(name)), duckdb::Value());
		auto entry = values.find(canonical.options.begin()->first);
		if (entry != values.end()) {
			*value = Convert(new duckdb::Value(entry->second));
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_set_value(duckdb_v2_file_metadata_handle metadata, duckdb_v2_str name,
                                                  duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		auto key = duckdb::string(Convert(name));
		if (key.empty()) {
			throw duckdb::InvalidInputException("A file metadata value name cannot be empty.");
		}
		if (CV2FileMetadata::IsReservedName(key)) {
			throw duckdb::InvalidInputException("\"%s\" names one of the typed fields of the file metadata, which "
			                                    "has a setter of its own",
			                                    key);
		}
		// The engine stores an option it knows under its canonical name, cast to the type it reads it back as.
		duckdb::ExtendedOpenFileInfo option;
		option.SetUserOption(key, *Convert(value));
		auto &stored = *option.options.begin();
		Convert(metadata)->data.extended_file_info[stored.first] = stored.second;
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_copy(duckdb_v2_file_metadata_handle target,
                                             duckdb_v2_file_metadata_handle source, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(target);
	DUCKDB_CHECK_ARG(source);
	return WithErrorHandler(err, [&]() {
		if (target != source) {
			*Convert(target) = *Convert(source);
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_set_type(duckdb_v2_file_metadata_handle metadata, DUCKDB_V2_FILE_TYPE type,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	return WithErrorHandler(err, [&]() {
		if (!IsFileType(type)) {
			throw duckdb::InvalidInputException("'%d' is not a file type.", static_cast<int>(type));
		}
		Convert(metadata)->data.file_type = CV2FileMetadata::ToEngineType(type);
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_set_size(duckdb_v2_file_metadata_handle metadata, idx_t size,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	return WithErrorHandler(err, [&]() { Convert(metadata)->data.file_size = duckdb::NumericCast<int64_t>(size); });
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_set_last_modified(duckdb_v2_file_metadata_handle metadata,
                                                          int64_t last_modified, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	return WithErrorHandler(
	    err, [&]() { Convert(metadata)->data.last_modification_time = duckdb::timestamp_t(last_modified); });
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_set_version_tag(duckdb_v2_file_metadata_handle metadata,
                                                        duckdb_v2_str version_tag, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(metadata);
	DUCKDB_CHECK_ARG(version_tag);
	return WithErrorHandler(err, [&]() { Convert(metadata)->data.version_tag = duckdb::string(Convert(version_tag)); });
}

DUCKDB_V2_ERROR duckdb_v2_file_metadata_destroy(duckdb_v2_file_metadata_handle *metadata) {
	return WithErrorHandler(nullptr, [&]() {
		if (!metadata) {
			return;
		}
		if (*metadata) {
			delete Convert(*metadata);
			*metadata = nullptr;
		}
	});
}

//----------------------------------------------------------------------------------------------------------------------
// File Listing
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_file_listing_add_entry(duckdb_v2_file_listing_handle listing, duckdb_v2_str path,
                                                 DUCKDB_V2_FILE_TYPE type, duckdb_v2_file_metadata_handle *metadata,
                                                 duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(listing);
	DUCKDB_CHECK_ARG(path);
	if (metadata) {
		*metadata = nullptr;
	}
	return WithErrorHandler(err, [&]() {
		if (!IsFileType(type)) {
			throw duckdb::InvalidInputException("'%d' is not a file type.", static_cast<int>(type));
		}
		auto entry_path = duckdb::string(Convert(path));
		if (entry_path.empty()) {
			throw duckdb::InvalidInputException("A listing entry cannot have an empty path.");
		}
		auto &entries = Convert(listing)->entries;
		entries.push_back({std::move(entry_path), {}});
		entries.back().metadata.data.file_type = CV2FileMetadata::ToEngineType(type);
		if (metadata) {
			*metadata = Convert(&entries.back().metadata);
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_listing_get_entry_count(duckdb_v2_file_listing_handle listing, idx_t *count,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(listing);
	DUCKDB_CHECK_ARG(count);
	return WithErrorHandler(err, [&]() { *count = Convert(listing)->entries.size(); });
}

DUCKDB_V2_ERROR duckdb_v2_file_listing_get_entry_path(duckdb_v2_file_listing_handle listing, idx_t index,
                                                      duckdb_v2_str *path, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(listing);
	DUCKDB_CHECK_ARG(path);
	const char *function = __func__;
	return WithErrorHandler(err, [&]() { *path = Convert(EntryAt(listing, index, function).path); });
}

DUCKDB_V2_ERROR duckdb_v2_file_listing_get_entry_type(duckdb_v2_file_listing_handle listing, idx_t index,
                                                      DUCKDB_V2_FILE_TYPE *type, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(listing);
	DUCKDB_CHECK_ARG(type);
	const char *function = __func__;
	return WithErrorHandler(err, [&]() { *type = EntryAt(listing, index, function).metadata.Type(); });
}

DUCKDB_V2_ERROR duckdb_v2_file_listing_get_entry_metadata(duckdb_v2_file_listing_handle listing, idx_t index,
                                                          duckdb_v2_file_metadata_handle *metadata,
                                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(listing);
	DUCKDB_CHECK_ARG(metadata);
	*metadata = nullptr;
	const char *function = __func__;
	return WithErrorHandler(err, [&]() {
		// The entries are only ever appended to, so handing out a pointer into them is safe.
		auto &entry = EntryAt(listing, index, function);
		*metadata = Convert(&entry.metadata);
	});
}

DUCKDB_V2_ERROR duckdb_v2_file_listing_destroy(duckdb_v2_file_listing_handle *listing) {
	return WithErrorHandler(nullptr, [&]() {
		if (!listing) {
			return;
		}
		if (*listing) {
			delete Convert(*listing);
			*listing = nullptr;
		}
	});
}

//----------------------------------------------------------------------------------------------------------------------
// Destroy
//----------------------------------------------------------------------------------------------------------------------

DUCKDB_V2_ERROR duckdb_v2_file_destroy(duckdb_v2_file_handle *file_handle) {
	return WithErrorHandler(nullptr, [&]() {
		if (!file_handle) {
			return;
		}
		if (*file_handle) {
			auto file = Convert(*file_handle);
			*file_handle = nullptr;
			// Close explicitly: a file system may treat a handle destroyed unclosed as an abandoned write.
			std::exception_ptr close_error;
			try {
				if (file->handle) {
					file->handle->Close();
				}
			} catch (...) {
				close_error = std::current_exception();
			}
			delete file;
			if (close_error) {
				std::rethrow_exception(close_error);
			}
		}
	});
}
