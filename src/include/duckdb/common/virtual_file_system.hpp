//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/virtual_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
struct FileSystemRegistry;
struct FileSystemHandle;
class DatabaseInstance;
class ClientContext;

//! The root of the file systems of a database: it picks the file system that handles a path from the registered
//! protocol handlers, applies compression and pipes on top, and holds the registry of the file systems and their
//! compression. It is not a FileSystem itself: the files it opens belong to the file system that handled them.
class VirtualFileSystem {
public:
	VirtualFileSystem();
	explicit VirtualFileSystem(unique_ptr<FileSystem> &&inner_file_system);
	~VirtualFileSystem();

	//! The root file system of a database
	DUCKDB_API static VirtualFileSystem &Get(DatabaseInstance &db);
	DUCKDB_API static VirtualFileSystem &Get(ClientContext &context);

	//! Opens a file with the file system that handles the path, and applies compression and pipes on top of it
	unique_ptr<FileHandle> OpenFile(const OpenFileInfo &file, FileOpenFlags flags, optional_ptr<FileOpener> opener);
	//! The file system that handles a path: a registered sub-system, or the default file system. Throws if it is
	//! disabled. Loads the extension that provides the file system if there is one.
	FileSystem &FindFileSystem(const string &path, optional_ptr<FileOpener> opener);

	void RegisterSubSystem(unique_ptr<FileSystem> fs);
	void RegisterCompressionFilesystem(unique_ptr<CompressedFileSystem> fs);
	void UnregisterSubSystem(const string &name);
	unique_ptr<FileSystem> ExtractSubSystem(const string &name);
	vector<string> ListSubSystems();

	void SetDisabledFileSystems(const vector<string> &names);
	vector<string> GetDisabledFileSystems();
	bool SubSystemIsDisabled(const string &name);
	bool IsDisabledForPath(const string &path);

	FileSystem &GetDefaultFileSystem();
	//! Replaces the default file system by the result of the function, which receives the current one. The current one
	//! stays alive for as long as this VirtualFileSystem does. Only valid before the file system is in use.
	void DecorateDefaultFileSystem(const std::function<unique_ptr<FileSystem>(FileSystem &)> &decorate);
	//! A VirtualFileSystem for local paths only: it shares the default file system if that is local, and has its own
	//! local file system otherwise
	unique_ptr<VirtualFileSystem> CreateLocalView();

	std::string GetName() const;

private:
	explicit VirtualFileSystem(shared_ptr<FileSystemHandle> default_file_system);

	FileSystem &FindFileSystem(shared_ptr<FileSystemRegistry> &registry, const string &path,
	                           optional_ptr<FileOpener> file_opener);
	optional_ptr<FileSystem> FindFileSystemInternal(FileSystemRegistry &registry, const string &path);
	// Return nullptr if compression is not involved, throw exception if compression is requested but no usable
	// filesystem gets registered.
	optional_ptr<FileSystem> FindCompressionFileSystem(FileSystemRegistry &registry,
	                                                   const FileCompressionType &compression, const string &path,
	                                                   bool for_writing);

private:
	mutex registry_lock;
	shared_ptr<FileSystemRegistry> file_system_registry;
	vector<unique_ptr<FileSystem>> unregistered_file_systems;
	//! Default file systems that were replaced by a decorated one
	vector<shared_ptr<FileSystemHandle>> decorated_file_systems;
};

} // namespace duckdb
