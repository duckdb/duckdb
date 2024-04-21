#include "duckdb_python/path_like.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pyfilesystem.hpp"
#include "duckdb_python/filesystem_object.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

struct PathLikeProcessor {
public:
	PathLikeProcessor(DuckDBPyConnection &connection, PythonImportCache &import_cache)
	    : connection(connection), import_cache(import_cache) {
	}

public:
	void AddFile(const py::object &object);
	PathLike Finalize();

protected:
	ModifiedMemoryFileSystem &GetFS() {
		if (!object_store) {
			object_store = &connection.GetObjectFileSystem();
		}
		return *object_store;
	}

public:
	DuckDBPyConnection &connection;
	optional_ptr<ModifiedMemoryFileSystem> object_store;
	PythonImportCache &import_cache;
	// The list containing every file
	vector<string> all_files;
	// The list of files that are registered in the object_store;
	vector<string> fs_files;
};

void PathLikeProcessor::AddFile(const py::object &object) {
	if (py::isinstance<py::str>(object)) {
		all_files.push_back(std::string(py::str(object)));
		return;
	}
	if (py::isinstance(object, import_cache.pathlib.Path())) {
		all_files.push_back(std::string(py::str(object)));
		return;
	}
	// This is (assumed to be) a file-like object
	auto generated_name =
	    StringUtil::Format("%s://%s", "DUCKDB_INTERNAL_OBJECTSTORE", StringUtil::GenerateRandomName());
	all_files.push_back(generated_name);
	fs_files.push_back(generated_name);

	auto &fs = GetFS();
	fs.attr("add_file")(object, generated_name);
}

PathLike PathLikeProcessor::Finalize() {
	PathLike result;

	if (all_files.empty()) {
		throw InvalidInputException("Please provide a non-empty list of paths or file-like objects");
	}
	result.files = std::move(all_files);

	if (fs_files.empty()) {
		// No file-like objects were registered in the filesystem
		// no need to make a dependency
		return result;
	}

	// Create the dependency, which contains the logic to clean up the files in its destructor
	auto &fs = GetFS();
	auto dependency = make_uniq<ExternalDependency>();
	auto dependency_item = PythonDependencyItem::Create(make_uniq<FileSystemObject>(fs, std::move(fs_files)));
	dependency->AddDependency("file_handles", std::move(dependency_item));
	result.dependency = std::move(dependency);
	return result;
}

PathLike PathLike::Create(const py::object &object, DuckDBPyConnection &connection) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();

	PathLikeProcessor processor(connection, import_cache);
	if (py::isinstance<py::list>(object)) {
		auto list = py::list(object);
		for (auto &item : list) {
			processor.AddFile(py::reinterpret_borrow<py::object>(item));
		}
	} else {
		// Single object
		processor.AddFile(object);
	}

	return processor.Finalize();
}

} // namespace duckdb
