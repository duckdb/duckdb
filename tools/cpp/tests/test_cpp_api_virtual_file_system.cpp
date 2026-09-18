#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: VirtualFileSystem. An in-memory file system under the
// "mem://" scheme, driven through SQL and through the consumer-side FileSystem.
//
// Callbacks must not use REQUIRE: it would throw through the callback boundary
// into the engine. They throw a duckdb::cxx::Exception to report a failure and
// latch what they saw for the test to assert on afterwards.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

using Info = VirtualFileSystem::Info;

constexpr const char *SCHEME = "mem://";
constexpr const char *CSV_A = "i,s\n1,one\n2,two\n";
constexpr const char *CSV_B = "i,s\n3,three\n";

// The file system's state, carried as user data by pointer so the test keeps hold of it.
struct MemStore {
	std::mutex lock;
	std::map<std::string, std::string> files;
	std::set<std::string> dirs;
	int opens = 0;
	int closes = 0;
	int open_had_context = 0;
	int reads_had_context = 0;
	std::string last_open_value;
};

// The per-file state: which file, in which store.
struct MemFile : VirtualFile {
	MemFile(MemStore &store, std::string path) : store(store), path(std::move(path)) {
	}
	MemStore &store;
	std::string path;
};

std::vector<std::string> CollectStrings(QueryResult result) {
	std::vector<std::string> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.emplace_back(view.Data<varchar_t>()[view.SelAt(i)].view());
		}
	}
	return out;
}

std::vector<int64_t> CollectBigints(QueryResult result) {
	std::vector<int64_t> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.push_back(view.Data<int64_t>()[view.SelAt(i)]);
		}
	}
	return out;
}

[[noreturn]] void NotFound(const std::string &path) {
	throw FileNotFoundException("IO Error: mem: no such file: " + path);
}

// `*` and `?` match within one path component, `**` across components.
bool Match(const char *pattern, const char *text) {
	while (*pattern) {
		if (*pattern == '*') {
			const bool deep = pattern[1] == '*';
			const char *rest = deep ? pattern + 2 : pattern + 1;
			for (const char *t = text;; t++) {
				if (Match(rest, t)) {
					return true;
				}
				if (!*t || (!deep && *t == '/')) {
					return false;
				}
			}
		}
		if (!*text || *text == '/' ? *pattern != *text : (*pattern != '?' && *pattern != *text)) {
			return false;
		}
		pattern++;
		text++;
	}
	return !*text;
}

auto StoreOf(Info &info) -> MemStore & {
	return *info.GetUserData<MemStore *>();
}

auto FileOf(VirtualFile &file) -> MemFile & {
	return static_cast<MemFile &>(file);
}

// ---- file callbacks: the file system leaves the cursor to the engine, so only positional operations ----

auto MemOpen(VirtualFileSystem::OpenInput &input) -> std::unique_ptr<VirtualFile> {
	auto &store = StoreOf(input);
	auto path = std::string(input.GetPath());
	const bool write = input.HasFlag(FileFlags::WRITE) || input.HasFlag(FileFlags::APPEND);
	const bool create = input.HasFlag(FileFlags::FILE_CREATE) || input.HasFlag(FileFlags::FILE_CREATE_NEW);

	std::lock_guard<std::mutex> guard(store.lock);
	// What the open saw, for the test to assert on.
	store.opens++;
	if (input.TryGetContext()) {
		store.open_had_context++;
	}
	if (auto hint = input.GetValue("mem_hint")) {
		store.last_open_value = std::to_string(hint->Get<int32_t>());
	} else {
		store.last_open_value.clear();
	}

	auto existing = store.files.find(path);
	if (existing == store.files.end()) {
		if (!write || !create) {
			NotFound(path);
		}
		store.files[path] = "";
	} else if (input.HasFlag(FileFlags::FILE_CREATE_NEW)) {
		existing->second.clear();
	}
	return std::unique_ptr<VirtualFile>(new MemFile(store, path));
}

auto MemReadAt(Info &info, VirtualFile &file, void *buffer, idx_t size, idx_t location) -> idx_t {
	auto &mem = FileOf(file);
	std::lock_guard<std::mutex> guard(mem.store.lock);
	if (info.TryGetContext()) {
		mem.store.reads_had_context++;
	}
	auto &data = mem.store.files[mem.path];
	if (location >= data.size()) {
		return 0;
	}
	auto count = std::min<idx_t>(size, data.size() - location);
	std::memcpy(buffer, data.data() + location, count);
	return count;
}

auto MemWriteAt(Info &, VirtualFile &file, const void *buffer, idx_t size, idx_t location) -> idx_t {
	auto &mem = FileOf(file);
	std::lock_guard<std::mutex> guard(mem.store.lock);
	auto &data = mem.store.files[mem.path];
	if (data.size() < location + size) {
		data.resize(location + size);
	}
	std::memcpy(&data[location], buffer, size);
	return size;
}

void MemStat(Info &, VirtualFile &file, FileMetadata &metadata) {
	auto &mem = FileOf(file);
	std::lock_guard<std::mutex> guard(mem.store.lock);
	metadata.SetSize(mem.store.files[mem.path].size());
}

void MemTruncate(Info &, VirtualFile &file, idx_t size) {
	auto &mem = FileOf(file);
	std::lock_guard<std::mutex> guard(mem.store.lock);
	mem.store.files[mem.path].resize(size);
}

void MemClose(Info &, VirtualFile &file) {
	auto &mem = FileOf(file);
	std::lock_guard<std::mutex> guard(mem.store.lock);
	mem.store.closes++;
}

// ---- path callbacks ----

bool IsDirectory(MemStore &store, const std::string &path) {
	if (store.dirs.count(path)) {
		return true;
	}
	auto prefix = path + "/";
	for (auto &entry : store.files) {
		if (entry.first.compare(0, prefix.size(), prefix) == 0) {
			return true;
		}
	}
	return false;
}

void MemStatPath(Info &info, std::string_view path_view, FileMetadata &metadata) {
	auto &store = StoreOf(info);
	auto path = std::string(path_view);
	std::lock_guard<std::mutex> guard(store.lock);
	auto file = store.files.find(path);
	if (file != store.files.end()) {
		metadata.SetType(FileType::REGULAR).SetSize(file->second.size());
		return;
	}
	if (IsDirectory(store, path)) {
		metadata.SetType(FileType::DIRECTORY);
	}
}

void MemList(Info &info, std::string_view path_view, FileListing &listing) {
	auto &store = StoreOf(info);
	auto prefix = std::string(path_view);
	if (prefix.back() != '/') {
		prefix += '/';
	}
	std::lock_guard<std::mutex> guard(store.lock);
	std::set<std::string> seen;
	for (auto &entry : store.files) {
		if (entry.first.compare(0, prefix.size(), prefix) != 0) {
			continue;
		}
		auto rest = entry.first.substr(prefix.size());
		auto slash = rest.find('/');
		auto name = slash == std::string::npos ? rest : rest.substr(0, slash);
		if (!seen.insert(name).second) {
			continue;
		}
		if (slash == std::string::npos) {
			listing.AddEntry(name, FileType::REGULAR).SetSize(entry.second.size());
		} else {
			listing.AddEntry(name, FileType::DIRECTORY);
		}
	}
}

void MemGlob(Info &info, std::string_view pattern_view, FileListing &listing) {
	auto &store = StoreOf(info);
	auto pattern = std::string(pattern_view);
	std::lock_guard<std::mutex> guard(store.lock);
	for (auto &entry : store.files) {
		if (Match(pattern.c_str(), entry.first.c_str())) {
			listing.AddEntry(entry.first, FileType::REGULAR).SetSize(entry.second.size());
		}
	}
}

void MemRemoveFile(Info &info, std::string_view path_view) {
	auto &store = StoreOf(info);
	auto path = std::string(path_view);
	std::lock_guard<std::mutex> guard(store.lock);
	if (store.files.erase(path) == 0) {
		NotFound(path);
	}
}

void MemCreateDirectory(Info &info, std::string_view path) {
	auto &store = StoreOf(info);
	std::lock_guard<std::mutex> guard(store.lock);
	store.dirs.insert(std::string(path));
}

void MemRemoveDirectory(Info &info, std::string_view path_view) {
	auto &store = StoreOf(info);
	auto path = std::string(path_view);
	auto prefix = path + "/";
	std::lock_guard<std::mutex> guard(store.lock);
	for (auto it = store.files.begin(); it != store.files.end();) {
		it = it->first.compare(0, prefix.size(), prefix) == 0 ? store.files.erase(it) : std::next(it);
	}
	for (auto it = store.dirs.begin(); it != store.dirs.end();) {
		it = it->compare(0, prefix.size(), prefix) == 0 ? store.dirs.erase(it) : std::next(it);
	}
	store.dirs.erase(path);
}

void MemMove(Info &info, std::string_view source_view, std::string_view target_view) {
	auto &store = StoreOf(info);
	auto source = std::string(source_view);
	std::lock_guard<std::mutex> guard(store.lock);
	auto it = store.files.find(source);
	if (it == store.files.end()) {
		NotFound(source);
	}
	store.files[std::string(target_view)] = std::move(it->second);
	store.files.erase(it);
}

auto CreateMem(MemStore &store) -> VirtualFileSystem {
	VirtualFileSystem vfs;
	vfs.SetName("mem")
	    .AddPrefix(SCHEME)
	    .SetUserData<MemStore *>(&store)
	    .SetFileOpenCallback(MemOpen)
	    .SetFileReadAtCallback(MemReadAt)
	    .SetFileWriteAtCallback(MemWriteAt)
	    .SetFileStatCallback(MemStat)
	    .SetFileTruncateCallback(MemTruncate)
	    .SetFileCloseCallback(MemClose)
	    .SetStatCallback(MemStatPath)
	    .SetListCallback(MemList)
	    .SetGlobCallback(MemGlob)
	    .SetRemoveFileCallback(MemRemoveFile)
	    .SetCreateDirectoryCallback(MemCreateDirectory)
	    .SetRemoveDirectoryCallback(MemRemoveDirectory)
	    .SetMoveCallback(MemMove);
	return vfs;
}

void RegisterMem(const Connection &conn, MemStore &store) {
	CreateMem(store).Register(conn);
}

} // namespace

TEST_CASE("Stable C++ API: a virtual file system serves read_csv and glob", "[cpp_api][vfs]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	MemStore store;
	store.files["mem://data/a.csv"] = CSV_A;
	store.files["mem://data/b.csv"] = CSV_B;
	store.files["mem://data/notes.txt"] = "not a csv";
	RegisterMem(conn, store);

	auto files = CollectStrings(conn.Execute("SELECT file FROM glob('mem://data/*.csv') ORDER BY file"));
	REQUIRE(files == std::vector<std::string> {"mem://data/a.csv", "mem://data/b.csv"});

	auto rows = CollectBigints(conn.Execute("SELECT i FROM read_csv('mem://data/*.csv') ORDER BY i"));
	REQUIRE(rows == std::vector<int64_t> {1, 2, 3});

	// Opens from a query carry its context; file operations get none; every open closed its file.
	REQUIRE(store.opens > 0);
	REQUIRE(store.open_had_context == store.opens);
	REQUIRE(store.reads_had_context == 0);
	REQUIRE(store.closes == store.opens);
}

TEST_CASE("Stable C++ API: a virtual file system takes COPY TO and the consumer path operations", "[cpp_api][vfs]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	MemStore store;
	RegisterMem(conn, store);

	conn.Execute("COPY (SELECT range AS i FROM range(5)) TO 'mem://out/x.csv' (HEADER)").Drain();
	REQUIRE(store.files.count("mem://out/x.csv") == 1);
	auto rows = CollectBigints(conn.Execute("SELECT i FROM read_csv('mem://out/x.csv') ORDER BY i"));
	REQUIRE(rows == std::vector<int64_t> {0, 1, 2, 3, 4});

	auto fs = conn.GetFileSystem();
	auto metadata = fs.Stat("mem://out/x.csv");
	REQUIRE(metadata.GetType() == FileType::REGULAR);
	REQUIRE(metadata.GetSize().has_value());
	REQUIRE(*metadata.GetSize() == store.files["mem://out/x.csv"].size());
	REQUIRE(!metadata.GetLastModified().has_value());
	REQUIRE(fs.Stat("mem://out").GetType() == FileType::DIRECTORY);
	REQUIRE(fs.Stat("mem://out/missing.csv").GetType() == FileType::INVALID);

	auto listing = fs.List("mem://out");
	REQUIRE(listing.GetEntryCount() == 1);
	REQUIRE(listing.GetEntryPath(0) == "x.csv");
	REQUIRE(listing.GetEntryType(0) == FileType::REGULAR);
	REQUIRE(listing.GetEntryMetadata(0).GetSize() == metadata.GetSize());
	REQUIRE_THROWS_MATCHES(listing.GetEntryPath(1), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// The open file reports the same size, and reads back at offsets.
	{
		auto file = fs.OpenFile("mem://out/x.csv", {FileFlags::READ});
		REQUIRE(file.Stat().GetSize() == metadata.GetSize());
		char head[2] = {0, 0};
		file.ReadAt(head, 1, 0);
		REQUIRE(head[0] == 'i');
	}

	fs.Move("mem://out/x.csv", "mem://out/y.csv");
	REQUIRE(fs.Glob("mem://out/*.csv").GetEntryPath(0) == "mem://out/y.csv");
	fs.CreateDirectory("mem://out/sub");
	REQUIRE(fs.Stat("mem://out/sub").GetType() == FileType::DIRECTORY);
	fs.RemoveFile("mem://out/y.csv");
	// The callback's "not found" reaches the consumer as the typed exception, carrying its text.
	REQUIRE_THROWS_AS(fs.RemoveFile("mem://out/y.csv"), FileNotFoundException);
	REQUIRE_THROWS_WITH(fs.RemoveFile("mem://out/y.csv"), Catch::Contains("mem: no such file"));
	fs.RemoveDirectory("mem://out");
	REQUIRE(store.files.empty());
	REQUIRE(store.dirs.empty());
}

TEST_CASE("Stable C++ API: virtual file system callback errors and open values", "[cpp_api][vfs]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	MemStore store;
	store.files["mem://v.txt"] = "x";
	RegisterMem(conn, store);

	// A missing file surfaces the open callback's error as the typed exception, carrying its text.
	auto fs = conn.GetFileSystem();
	REQUIRE_THROWS_AS(fs.OpenFile("mem://missing.csv", {FileFlags::READ}), FileNotFoundException);
	REQUIRE_THROWS_MATCHES(fs.OpenFile("mem://missing.csv", {FileFlags::READ}), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_IO_FILE_NOT_FOUND));
	REQUIRE_THROWS_WITH(fs.OpenFile("mem://missing.csv", {FileFlags::READ}), Catch::Contains("mem: no such file"));

	// Values attached to an open reach the callback; without one the lookup comes back empty.
	auto options = fs.CreateOpenOptions();
	options.SetFlag(FileFlags::READ).SetValue("mem_hint", Value::Create(conn, static_cast<int32_t>(42)));
	{
		auto file = fs.OpenFile("mem://v.txt", options);
		REQUIRE(store.last_open_value == "42");
	}
	{
		auto file = fs.OpenFile("mem://v.txt", {FileFlags::READ});
		REQUIRE(store.last_open_value.empty());
	}
}

TEST_CASE("Stable C++ API: virtual file system registration is validated", "[cpp_api][vfs]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	MemStore store;

	// No name.
	{
		VirtualFileSystem vfs;
		vfs.AddPrefix(SCHEME).SetFileOpenCallback(MemOpen).SetFileReadAtCallback(MemReadAt).SetFileStatCallback(
		    MemStat);
		REQUIRE_THROWS_MATCHES(vfs.Register(conn), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
	// No open callback.
	{
		VirtualFileSystem vfs;
		vfs.SetName("mem").AddPrefix(SCHEME).SetFileReadAtCallback(MemReadAt).SetFileStatCallback(MemStat);
		REQUIRE_THROWS_MATCHES(vfs.Register(conn), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
	// Owning the cursor without reporting where it is.
	{
		auto vfs = CreateMem(store);
		vfs.SetFileSeekCallback([](Info &, VirtualFile &, idx_t) {});
		REQUIRE_THROWS_MATCHES(vfs.Register(conn), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	}
	// A callback can be taken back again before registering.
	{
		auto vfs = CreateMem(store);
		vfs.SetFileSeekCallback([](Info &, VirtualFile &, idx_t) {}).SetFileSeekCallback(nullptr);
		vfs.Register(conn);
	}
}
