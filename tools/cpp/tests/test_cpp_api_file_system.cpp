#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"
#include "test_helpers.hpp"

#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: FileSystem and FileHandle.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

void WriteAll(FileHandle &file, const std::string &data) {
	REQUIRE(file.Write(data.data(), data.size()) == data.size());
}

std::string ReadAll(FileHandle &file, idx_t capacity) {
	std::vector<char> buffer(capacity, '\0');
	auto read = file.Read(buffer.data(), capacity);
	return std::string(buffer.data(), read);
}

} // namespace

TEST_CASE("Stable C++API: file system round-trip", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto fs = conn.GetFileSystem();
	auto path = duckdb::TestCreatePath("cpp_fs_roundtrip.bin");

	{
		auto file = fs.OpenFile(path, {FileFlags::WRITE, FileFlags::FILE_CREATE_NEW});
		WriteAll(file, "hello world");
		REQUIRE(file.Tell() == 11);
		REQUIRE(file.Size() == 11);
		file.Sync();
	}

	auto file = fs.OpenFile(path, {FileFlags::READ});
	REQUIRE(file.Size() == 11);
	REQUIRE(ReadAll(file, 32) == "hello world");
	// At the end, a read yields nothing rather than failing.
	REQUIRE(ReadAll(file, 32).empty());

	file.Seek(6);
	REQUIRE(file.Tell() == 6);
	REQUIRE(ReadAll(file, 5) == "world");
}

TEST_CASE("Stable C++API: file flags", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto fs = conn.GetFileSystem();
	auto path = duckdb::TestCreatePath("cpp_fs_flags.bin");

	{
		auto file = fs.OpenFile(path, {FileFlags::WRITE, FileFlags::FILE_CREATE});
		WriteAll(file, "hello");
	}
	{
		// FILE_CREATE leaves an existing file alone.
		auto file = fs.OpenFile(path, {FileFlags::WRITE, FileFlags::FILE_CREATE});
		REQUIRE(file.Size() == 5);
	}
	{
		// FILE_CREATE_NEW truncates it.
		auto file = fs.OpenFile(path, {FileFlags::WRITE, FileFlags::FILE_CREATE_NEW});
		REQUIRE(file.Size() == 0);
		WriteAll(file, "hi");
	}
	// EXCLUSIVE_CREATE refuses it, and leaves the contents intact.
	REQUIRE_THROWS_AS(fs.OpenFile(path, {FileFlags::WRITE, FileFlags::FILE_CREATE, FileFlags::EXCLUSIVE_CREATE}),
	                  Exception);
	{
		auto file = fs.OpenFile(path, {FileFlags::READ});
		REQUIRE(ReadAll(file, 32) == "hi");
	}
	{
		// APPEND writes at the end.
		auto file = fs.OpenFile(path, {FileFlags::WRITE, FileFlags::APPEND});
		WriteAll(file, "there");
	}
	{
		auto file = fs.OpenFile(path, {FileFlags::READ});
		REQUIRE(ReadAll(file, 32) == "hithere");
	}
}

TEST_CASE("Stable C++API: file system refusals", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto fs = conn.GetFileSystem();

	// A missing file without a create flag.
	REQUIRE_THROWS_AS(fs.OpenFile(duckdb::TestCreatePath("cpp_fs_missing.bin"), {FileFlags::READ}), Exception);
	// No capability at all.
	REQUIRE_THROWS_MATCHES(fs.OpenFile(duckdb::TestCreatePath("cpp_fs_none.bin"), {}), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	// INVALID names no behaviour.
	REQUIRE_THROWS_MATCHES(fs.OpenFile(duckdb::TestCreatePath("cpp_fs_none.bin"), {FileFlags::INVALID}), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: file handle close then destroy", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto fs = conn.GetFileSystem();
	auto path = duckdb::TestCreatePath("cpp_fs_close.bin");

	auto file = fs.OpenFile(path, {FileFlags::WRITE, FileFlags::FILE_CREATE_NEW});
	WriteAll(file, "data");
	file.Close();
	// The handle is still alive; its destructor runs at the end of the scope.
}

TEST_CASE("Stable C++API: positional file read and write", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto fs = conn.GetFileSystem();
	auto path = duckdb::TestCreatePath("cpp_fs_positional.bin");

	auto file =
	    fs.OpenFile(path, {FileFlags::READ, FileFlags::WRITE, FileFlags::FILE_CREATE_NEW, FileFlags::PARALLEL_ACCESS});
	WriteAll(file, "abcdefghij");
	REQUIRE(file.Tell() == 10);

	// Positional access leaves the file's own position alone.
	char buffer[4] = {0};
	file.ReadAt(buffer, 4, 2);
	REQUIRE(std::string(buffer, 4) == "cdef");
	REQUIRE(file.Tell() == 10);

	file.WriteAt("XY", 2, 4);
	REQUIRE(file.Tell() == 10);
	file.ReadAt(buffer, 4, 2);
	REQUIRE(std::string(buffer, 4) == "cdXY");

	// A short positional read is an error, not a result.
	REQUIRE_THROWS_AS(file.ReadAt(buffer, 4, 8), Exception);
}

TEST_CASE("Stable C++API: file open options", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto fs = conn.GetFileSystem();
	auto path = duckdb::TestCreatePath("cpp_fs_options.bin");

	auto options = fs.CreateOpenOptions();
	options.SetFlag(FileFlags::WRITE)
	    .SetFlag(FileFlags::FILE_CREATE_NEW)
	    .SetValue("file_size", Value::Create(conn, int64_t {4}))
	    .SetValue("made_up_option", Value::Create(conn, varchar_t("nobody-reads-this")));

	{
		auto file = fs.OpenFile(path, options);
		WriteAll(file, "data");
	}
	// One options object opens as many files as you like.
	{
		auto file = fs.OpenFile(duckdb::TestCreatePath("cpp_fs_options_second.bin"), options);
		WriteAll(file, "more");
	}
	{
		auto file = fs.OpenFile(path, {FileFlags::READ});
		REQUIRE(ReadAll(file, 32) == "data");
	}

	// Options carrying no flags cannot say whether the file is read or written.
	auto empty = fs.CreateOpenOptions();
	REQUIRE_THROWS_MATCHES(fs.OpenFile(path, empty), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}
