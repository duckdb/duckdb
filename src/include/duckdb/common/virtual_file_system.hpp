//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/virtual_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/base_file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
struct FileSystemRegistry;
class DatabaseInstance;
class ClientContext;
class Context;

//! The root of the file systems of a database: it picks the file system that handles a path from the registered
//! protocol handlers, applies compression and pipes on top, and holds the registry of the file systems and their
//! compression. It is not a BaseFileSystem itself: the files it opens belong to the file system that handled them.
class VirtualFileSystem {
public:
	DUCKDB_API VirtualFileSystem();
	DUCKDB_API explicit VirtualFileSystem(unique_ptr<BaseFileSystem> &&inner_file_system);
	DUCKDB_API ~VirtualFileSystem();

	//! The root file system of a database
	DUCKDB_API static VirtualFileSystem &Get(DatabaseInstance &db);
	DUCKDB_API static VirtualFileSystem &Get(ClientContext &context);

	//! Opens a file with the file system that handles the path, and applies compression and pipes on top of it
	DUCKDB_API unique_ptr<FileHandle> OpenFile(Context &context, const OpenFileInfo &file, FileOpenFlags flags);
	//! Applies compression and pipes on top of a handle that was opened uncompressed
	DUCKDB_API unique_ptr<FileHandle> WrapOpenedFile(Context &context, unique_ptr<FileHandle> handle,
	                                                 const OpenFileInfo &file, const FileOpenFlags &flags);
	//! The file system that handles a path: a registered sub-system, or the default file system. Throws if it is
	//! disabled. The overload with a context first loads the extension that provides the file system, if any.
	DUCKDB_API BaseFileSystem &FindFileSystem(const string &path);
	DUCKDB_API BaseFileSystem &FindFileSystem(Context &context, const string &path);

	DUCKDB_API void RegisterSubSystem(unique_ptr<BaseFileSystem> fs);
	DUCKDB_API void RegisterCompressionFilesystem(unique_ptr<CompressedFileSystem> fs);
	DUCKDB_API void UnregisterSubSystem(const string &name);
	DUCKDB_API unique_ptr<BaseFileSystem> ExtractSubSystem(const string &name);
	DUCKDB_API vector<string> ListSubSystems();

	DUCKDB_API void SetDisabledFileSystems(const vector<string> &names);
	DUCKDB_API vector<string> GetDisabledFileSystems();
	DUCKDB_API bool SubSystemIsDisabled(const string &name);
	DUCKDB_API bool IsDisabledForPath(const string &path);

	//! The file system of paths no sub-system handles. Does not check whether it is disabled.
	DUCKDB_API BaseFileSystem &GetDefaultFileSystem();

	DUCKDB_API std::string GetName() const;

private:
	BaseFileSystem &FindFileSystem(shared_ptr<FileSystemRegistry> &registry, const string &path,
	                               optional_ptr<DatabaseInstance> autoload_db);
	optional_ptr<BaseFileSystem> FindFileSystemInternal(FileSystemRegistry &registry, const string &path);
	// Return nullptr if compression is not involved, throw exception if compression is requested but no usable
	// filesystem gets registered.
	optional_ptr<BaseFileSystem> FindCompressionFileSystem(FileSystemRegistry &registry,
	                                                       const FileCompressionType &compression, const string &path,
	                                                       bool for_writing);

private:
	mutex registry_lock;
	shared_ptr<FileSystemRegistry> file_system_registry;
	vector<unique_ptr<BaseFileSystem>> unregistered_file_systems;
};

} // namespace duckdb
