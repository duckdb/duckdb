#include "catch.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

static void create_dummy_file(string fname) {
	string normalized_string;
	if (StringUtil::StartsWith(fname, "file:///")) {
#ifdef _WIN32
		normalized_string = fname.substr(8);
#else
		normalized_string = fname.substr(7);
#endif

	} else if (StringUtil::StartsWith(fname, "file://localhost/")) {
#ifdef _WIN32
		normalized_string = fname.substr(18);
#else
		normalized_string = fname.substr(18);
#endif
	} else {
		normalized_string = fname;
	}

	ofstream outfile(normalized_string);
	outfile << "I_AM_A_DUMMY" << endl;
	outfile.close();
}

TEST_CASE("Make sure the file:// protocol works as expected", "[file_system]") {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	auto dname = fs->JoinPath(fs->GetWorkingDirectory(), TestCreatePath("TEST_DIR"));
	auto dname_converted_slashes = StringUtil::Replace(dname, "\\", "/");

	// handle differences between windows and linux
	if (StringUtil::StartsWith(dname_converted_slashes, "/")) {
		dname_converted_slashes = dname_converted_slashes.substr(1);
	}

	// Path of format file:///bla/bla on 'nix and file:///X:/bla/bla on Windows
	auto dname_triple_slash = string("file:///") + dname_converted_slashes;
	// Path of format file://localhost/bla/bla on 'nix and file://localhost/X:/bla/bla on Windows
	auto dname_localhost = string("file://localhost/") + dname_converted_slashes;
	auto dname_no_host = string("file:/") + dname_converted_slashes;

	string fname = "TEST_FILE";
	string fname2 = "TEST_FILE_TWO";

	if (fs->DirectoryExists(dname_triple_slash)) {
		fs->RemoveDirectory(dname_triple_slash);
	}

	fs->CreateDirectory(dname_triple_slash);
	REQUIRE(fs->DirectoryExists(dname_triple_slash));
	REQUIRE(!fs->FileExists(dname_triple_slash));

	// we can call this again and nothing happens
	fs->CreateDirectory(dname_triple_slash);

	auto fname_in_dir = fs->JoinPath(dname_triple_slash, fname);
	auto fname_in_dir2 = fs->JoinPath(dname_localhost, fname2);
	auto fname_in_dir3 = fs->JoinPath(dname_no_host, fname2);

	create_dummy_file(fname_in_dir);
	REQUIRE(fs->FileExists(fname_in_dir));
	REQUIRE(!fs->DirectoryExists(fname_in_dir));

	size_t n_files = 0;
	REQUIRE(fs->ListFiles(dname_triple_slash, [&n_files](const string &path, bool) { n_files++; }));

	REQUIRE(n_files == 1);

	REQUIRE(fs->FileExists(fname_in_dir));
	REQUIRE(!fs->FileExists(fname_in_dir2));

	auto file_listing = fs->Glob(fs->JoinPath(dname_triple_slash, "*"));
	REQUIRE(file_listing[0].path == fname_in_dir);

	fs->MoveFile(fname_in_dir, fname_in_dir2);

	REQUIRE(!fs->FileExists(fname_in_dir));
	REQUIRE(fs->FileExists(fname_in_dir2));

	auto file_listing_after_move = fs->Glob(fs->JoinPath(dname_no_host, "*"));
	REQUIRE(file_listing_after_move[0].path == fname_in_dir3);

	fs->RemoveDirectory(dname_triple_slash);

	REQUIRE(!fs->DirectoryExists(dname_triple_slash));
	REQUIRE(!fs->FileExists(fname_in_dir));
	REQUIRE(!fs->FileExists(fname_in_dir2));
}

TEST_CASE("Make sure file system operators work as advertised", "[file_system]") {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	auto dname = TestCreatePath("TEST_DIR");
	string fname = "TEST_FILE";
	string fname2 = "TEST_FILE_TWO";

	if (fs->DirectoryExists(dname)) {
		fs->RemoveDirectory(dname);
	}

	fs->CreateDirectory(dname);
	REQUIRE(fs->DirectoryExists(dname));
	REQUIRE(!fs->FileExists(dname));

	// we can call this again and nothing happens
	fs->CreateDirectory(dname);

	auto fname_in_dir = fs->JoinPath(dname, fname);
	auto fname_in_dir2 = fs->JoinPath(dname, fname2);

	create_dummy_file(fname_in_dir);
	REQUIRE(fs->FileExists(fname_in_dir));
	REQUIRE(!fs->DirectoryExists(fname_in_dir));

	size_t n_files = 0;
	REQUIRE(fs->ListFiles(dname, [&n_files](const string &path, bool) { n_files++; }));

	REQUIRE(n_files == 1);

	REQUIRE(fs->FileExists(fname_in_dir));
	REQUIRE(!fs->FileExists(fname_in_dir2));

	fs->MoveFile(fname_in_dir, fname_in_dir2);

	REQUIRE(!fs->FileExists(fname_in_dir));
	REQUIRE(fs->FileExists(fname_in_dir2));

	fs->RemoveDirectory(dname);

	REQUIRE(!fs->DirectoryExists(dname));
	REQUIRE(!fs->FileExists(fname_in_dir));
	REQUIRE(!fs->FileExists(fname_in_dir2));
}

TEST_CASE("JoinPath normalizes separators and dot segments", "[file_system]") {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	auto sep = fs->PathSeparator("dummy");
	auto collapse = [&](const string &path) {
		return StringUtil::Replace(path, "/", sep);
	};

	auto normalized = fs->JoinPath("dir//subdir/", "./file");
	CHECK(normalized == collapse("dir/subdir/file"));

	auto parent = fs->JoinPath("dir/subdir", "../sibling");
	CHECK(parent == collapse("dir/sibling"));

	CHECK_THROWS(fs->JoinPath("dir", "/abs/path"));

	auto dedup = fs->JoinPath("dir///", "nested///child");
	CHECK(dedup == collapse("dir/nested/child"));

	auto zero_rel = fs->JoinPath("foo/bar", "../..");
	CHECK(zero_rel == fs->ConvertSeparators("."));
}

TEST_CASE("JoinPath handles edge cases", "[file_system]") {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	auto sep = fs->PathSeparator("dummy");
	auto collapse = [&](const string &path) {
		return StringUtil::Replace(path, "/", sep);
	};

	auto lhs_parent = fs->JoinPath("dir/subdir/..", "sibling");
	CHECK(lhs_parent == collapse("dir/sibling"));

	auto mixed_dots = fs->JoinPath("./dir/./subdir/./..", "./sibling/.");
	CHECK(mixed_dots == collapse("dir/sibling"));

	auto overflowing_rel = fs->JoinPath("dir/..", "../..");
	CHECK(overflowing_rel == fs->ConvertSeparators("../.."));

	auto walk_up_absolute = fs->JoinPath("/usr/local", "../..");
	CHECK(walk_up_absolute == fs->ConvertSeparators("/"));

	auto root_child = fs->JoinPath("/", "usr/local");
	CHECK(root_child == fs->ConvertSeparators("/usr/local"));

	auto past_root = fs->JoinPath("/usr/local", "../../..");
	CHECK(past_root == fs->ConvertSeparators("/"));

	auto file1_join = fs->JoinPath("file:/usr/local", "../bin");
	CHECK(file1_join == "file:/usr/bin");

	auto file2_join = fs->JoinPath("file://localhost/usr/local", "../bin");
	CHECK(file2_join == "file://localhost/usr/bin");

	auto file3_join = fs->JoinPath("file:///usr/local", "../bin");
	CHECK(file3_join == "file:///usr/bin");

	auto s3_join = fs->JoinPath("s3://foo", "bar/baz");
	CHECK(s3_join == "s3://foo/bar/baz");

	auto s3_parent = fs->JoinPath("s3://foo", "..");
	CHECK(s3_parent == "s3://foo/");

	auto s3_parent_twice = fs->JoinPath("s3://foo", "../..");
	CHECK(s3_parent_twice == "s3://foo/");

	CHECK_THROWS(fs->JoinPath("s3://foo", "az://foo"));
	CHECK_THROWS(fs->JoinPath("s3://foo", "/foo/bar/baz"));

	auto absolute_child = fs->JoinPath("/usr/local", "/usr/local/bin");
	CHECK(absolute_child == fs->ConvertSeparators("/usr/local/bin"));

	CHECK_THROWS(fs->JoinPath("/usr/local", "/var/log"));

	auto scheme_like_embed = fs->JoinPath("/foo/proto://bar", "a");
	CHECK(scheme_like_embed == fs->ConvertSeparators("/foo/proto:/bar/a"));

#ifdef _WIN32
	auto clamp_drive_root = fs->JoinPath(R"(C:\)", R"(..)");
	CHECK(clamp_drive_root == fs->ConvertSeparators("C:/"));

	auto clamp_drive_root_twice = fs->JoinPath(R"(C:\)", R"(..\..)");
	CHECK(clamp_drive_root_twice == fs->ConvertSeparators("C:/"));

	auto drive_absolute_child = fs->JoinPath(R"(C:\)", "system32");
	CHECK(drive_absolute_child == fs->ConvertSeparators("C:/system32"));

	auto drive_relative = fs->JoinPath("C:", "system32");
	CHECK(drive_relative == fs->ConvertSeparators("C:system32"));

	auto drive_relative_child = fs->JoinPath("C:drive_relative_path", "path");
	CHECK(drive_relative_child == fs->ConvertSeparators("C:drive_relative_path/path"));

	auto drive_relative_parent = fs->JoinPath("C:drive_relative_path", R"(..\path)");
	CHECK(drive_relative_parent == "C:path");

	auto unc_path = fs->JoinPath(R"(\\server\share)", R"(child)");
	CHECK(unc_path == fs->ConvertSeparators(R"(\\server\share\child)"));

	auto unc_long_path = fs->JoinPath(R"(\\?\UNC\server\share)", R"(nested\dir)");
	CHECK(unc_long_path == fs->ConvertSeparators(R"(\\?\UNC\server\share\nested\dir)"));

	auto long_drive_path = fs->JoinPath(R"(\\?\C:\base)", R"(folder)");
	CHECK(long_drive_path == fs->ConvertSeparators(R"(\\?\C:\base\folder)"));

	auto ci_prefix = fs->JoinPath(R"(C:\Data)", R"(C:\data\file)");
	CHECK(ci_prefix == fs->ConvertSeparators(R"(C:\data\file)"));

	CHECK_THROWS(fs->JoinPath(R"(C:\\foo)", R"(D:\\bar)"));
#endif
}

#ifdef _WIN32
TEST_CASE("Glob handles absolute drive paths", "[file_system]") {
	auto fs = FileSystem::CreateLocal();
	auto base_dir = fs->NormalizeAbsolutePath(TestCreatePath("glob_drive"));
	// base_dir resolves to an absolute drive path (e.g., C:\...\glob_drive) to ensure globbing works from a drive root
	if (fs->DirectoryExists(base_dir)) {
		fs->RemoveDirectory(base_dir);
	}
	fs->CreateDirectory(base_dir);

	auto nested_dir = fs->JoinPath(base_dir, "nested");
	fs->CreateDirectory(nested_dir);
	auto fname = fs->JoinPath(nested_dir, "file.csv");
	create_dummy_file(fname);

	auto pattern = fs->JoinPath(base_dir, "*.csv");
	auto deep_pattern = fs->JoinPath(fs->JoinPath(base_dir, "*"), "*.csv");

	auto entries_shallow = fs->Glob(pattern);
	auto entries_deep = fs->Glob(deep_pattern);

	REQUIRE(entries_shallow.size() == 0); // file is nested, so shallow glob should not see it
	REQUIRE(entries_deep.size() == 1);
	REQUIRE(fs->ConvertSeparators(entries_deep[0].path) == fs->ConvertSeparators(fname));

	fs->RemoveFile(fname);
	fs->RemoveDirectory(nested_dir);
	fs->RemoveDirectory(base_dir);
}
#endif

// note: the integer count is chosen as 512 so that we write 512*8=4096 bytes to the file
// this is required for the Direct-IO as on Windows Direct-IO can only write multiples of sector sizes
// sector sizes are typically one of [512/1024/2048/4096] bytes, hence a 4096 bytes write succeeds.
#define INTEGER_COUNT 512

TEST_CASE("Test file operations", "[file_system]") {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	duckdb::unique_ptr<FileHandle> handle, handle2;
	int64_t test_data[INTEGER_COUNT];
	for (int i = 0; i < INTEGER_COUNT; i++) {
		test_data[i] = i;
	}

	auto fname = TestCreatePath("test_file");

	// standard reading/writing test

	// open file for writing
	REQUIRE_NOTHROW(handle = fs->OpenFile(fname, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE));
	// write 10 integers
	REQUIRE_NOTHROW(handle->Write(QueryContext(), (void *)test_data, sizeof(int64_t) * INTEGER_COUNT, 0));
	// close the file
	handle.reset();

	for (int i = 0; i < INTEGER_COUNT; i++) {
		test_data[i] = 0;
	}
	// now open the file for reading
	REQUIRE_NOTHROW(handle = fs->OpenFile(fname, FileFlags::FILE_FLAGS_READ));
	// Check file stats.
	const auto file_metadata = fs->Stats(*handle);
	REQUIRE(file_metadata.file_size == 4096);
	REQUIRE(file_metadata.file_type == FileType::FILE_TYPE_REGULAR);
	REQUIRE(file_metadata.last_modification_time > timestamp_t {-1});

	// read the 10 integers back
	REQUIRE_NOTHROW(handle->Read(QueryContext(), (void *)test_data, sizeof(int64_t) * INTEGER_COUNT, 0));
	// check the values of the integers
	for (int i = 0; i < 10; i++) {
		REQUIRE(test_data[i] == i);
	}
	handle.reset();
	fs->RemoveFile(fname);
}

TEST_CASE("absolute paths", "[file_system]") {
	duckdb::LocalFileSystem fs;

#ifndef _WIN32
	REQUIRE(fs.IsPathAbsolute("/home/me"));
	REQUIRE(!fs.IsPathAbsolute("./me"));
	REQUIRE(!fs.IsPathAbsolute("me"));
#else
	const std::string long_path = "\\\\?\\D:\\very long network\\";
	REQUIRE(fs.IsPathAbsolute(long_path));
	const std::string network = "\\\\network_drive\\filename.csv";
	REQUIRE(fs.IsPathAbsolute(network));
	REQUIRE(fs.IsPathAbsolute("C:\\folder\\filename.csv"));
	REQUIRE(fs.IsPathAbsolute("C:/folder\\filename.csv"));
	REQUIRE(fs.NormalizeAbsolutePath("C:/folder\\filename.csv") == "C:\\folder\\filename.csv");
	REQUIRE(fs.NormalizeAbsolutePath(network) == network);
	REQUIRE(fs.NormalizeAbsolutePath(long_path) == "\\\\?\\D:\\very long network\\");
#endif
}

TEST_CASE("Test RemoveFiles", "[file_system]") {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	auto dname = TestCreatePath("test_remove_files");

	if (fs->DirectoryExists(dname)) {
		fs->RemoveDirectory(dname);
	}
	fs->CreateDirectory(dname);

	auto file1 = fs->JoinPath(dname, "file1.txt");
	auto file2 = fs->JoinPath(dname, "file2.txt");
	auto file3 = fs->JoinPath(dname, "file3.txt");
	auto file4 = fs->JoinPath(dname, "file4.txt");
	create_dummy_file(file1);
	create_dummy_file(file2);
	create_dummy_file(file3);

	REQUIRE(fs->FileExists(file1));
	REQUIRE(fs->FileExists(file2));
	REQUIRE(fs->FileExists(file3));
	REQUIRE(!fs->FileExists(file4));

	fs->RemoveFiles({file1, file2});
	REQUIRE(!fs->FileExists(file1));
	REQUIRE(!fs->FileExists(file2));
	REQUIRE(fs->FileExists(file3));
	REQUIRE(!fs->FileExists(file4));

	fs->RemoveDirectory(dname);
}

TEST_CASE("extract subsystem", "[file_system]") {
	duckdb::VirtualFileSystem vfs;
	auto local_filesystem = FileSystem::CreateLocal();
	auto *local_filesystem_ptr = local_filesystem.get();
	vfs.RegisterSubSystem(std::move(local_filesystem));

	// Extract a non-existent filesystem gets nullptr.
	REQUIRE(vfs.ExtractSubSystem("non-existent") == nullptr);

	// Extract an existing filesystem.
	auto extracted_filesystem = vfs.ExtractSubSystem(local_filesystem_ptr->GetName());
	REQUIRE(extracted_filesystem.get() == local_filesystem_ptr);

	// Re-extraction gets nullptr.
	REQUIRE(vfs.ExtractSubSystem("non-existent") == nullptr);

	// Register a subfilesystem and disable, which is not allowed to extract.
	const ::duckdb::string target_fs = extracted_filesystem->GetName();
	const ::duckdb::vector<string> disabled_subfilesystems {target_fs};
	vfs.RegisterSubSystem(std::move(extracted_filesystem));
	vfs.SetDisabledFileSystems(disabled_subfilesystems);
	REQUIRE(vfs.ExtractSubSystem(target_fs) == nullptr);
}

TEST_CASE("re-register subsystem", "[file_system]") {
	duckdb::VirtualFileSystem vfs;

	// First time registration should succeed.
	auto local_filesystem = FileSystem::CreateLocal();
	vfs.RegisterSubSystem(std::move(local_filesystem));

	// Re-register an already registered subfilesystem should throw.
	auto second_local_filesystem = FileSystem::CreateLocal();
	REQUIRE_THROWS(vfs.RegisterSubSystem(std::move(second_local_filesystem)));
}

TEST_CASE("filesystem concurrent access and deletion", "[file_system]") {
	auto fs = FileSystem::CreateLocal();

	auto fname = TestCreatePath("concurrent_delete_test_file");
	const string payload = "DELETE_WHILE_OPEN_CONTENT";

	// Open first handle to create/write, then close it.
	auto write_handle = fs->OpenFile(fname, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	write_handle->Write(QueryContext(), static_cast<void *>(const_cast<char *>(payload.c_str())), payload.size(),
	                    /*location=*/0);
	write_handle->Sync();
	write_handle.reset();
	REQUIRE(fs->FileExists(fname));

	// Open second handle for reading.
	auto read_handle = fs->OpenFile(fname, FileFlags::FILE_FLAGS_READ);

	// Delete the file while the read handle is still open.
	fs->RemoveFile(fname);
	REQUIRE(!fs->FileExists(fname));

	// Reading from the already-open read handle should still return the content.
	string read_back(payload.size(), '\0');
	read_handle->Read(QueryContext(), static_cast<void *>(const_cast<char *>(read_back.data())), payload.size(),
	                  /*location=*/0);
	REQUIRE(read_back == payload);

	// Close the remaining handle; the file should not exist.
	read_handle.reset();
	REQUIRE(!fs->FileExists(fname));
}

// ------------------------------------------------------------------------------------------------
// Path struct tests (ported from playground/test-playground.cpp)
// ------------------------------------------------------------------------------------------------

TEST_CASE("Path parses and correctly structures fields", "[file_system]") {
	Path output;

	SECTION("local files") {
		output = Path::FromString("a/b");
		CHECK(output.scheme == "");
		CHECK(output.authority == "");
		CHECK(output.anchor == "");
		CHECK(output.is_absolute == false);
		CHECK(output.GetPath() == "a/b");

		output = Path::FromString("/..////a/./b/../c");
		CHECK(output.scheme == "");
		CHECK(output.authority == "");
		CHECK(output.anchor == "/");
		CHECK(output.GetPath() == "a/c");
		CHECK(output.is_absolute == true);
	}

	SECTION("file schemes") {
		output = Path::FromString("file:/a/b");
		CHECK(output.scheme == "file:");
		CHECK(output.authority == "");
		CHECK(output.anchor == "/");
		CHECK(output.is_absolute == true);
		CHECK(output.GetPath() == "a/b");

		output = Path::FromString("file://localhost/a/b");
		CHECK(output.scheme == "file://");
		CHECK(output.authority == "localhost");
		CHECK(output.anchor == "/");
		CHECK(output.is_absolute == true);
		CHECK(output.GetPath() == "a/b");

		output = Path::FromString("file:///a/b");
		CHECK(output.scheme == "file://");
		CHECK(output.authority == "");
		CHECK(output.anchor == "/");
		CHECK(output.is_absolute == true);
		CHECK(output.GetPath() == "a/b");

		output = Path::FromString("file://LOCALHOST/a/b");
		CHECK(output.scheme == "file://");
		CHECK(output.authority == "LOCALHOST");
		CHECK(output.anchor == "/");
		CHECK(output.GetPath() == "a/b");

#if defined(_WIN32)
		output = Path::FromString(R"(c:..\foo)");
		CHECK(output.scheme == "");
		CHECK(output.authority == "");
		CHECK(output.anchor == "C:");
		CHECK(output.is_absolute == false);
		CHECK(output.GetPath() == R"(..\foo)");

		output = Path::FromString(R"(c:\..\foo)");
		CHECK(output.scheme == "");
		CHECK(output.authority == "");
		CHECK(output.anchor == R"(C:\)");
		CHECK(output.is_absolute == true);
		CHECK(output.GetPath() == "foo");
#endif
	}

	SECTION("URI schemes") {
		output = Path::FromString("az://container/b/c");
		CHECK(output.scheme == "az://");
		CHECK(output.authority == "container");
		CHECK(output.anchor == "/");
		CHECK(output.is_absolute);
		CHECK(output.GetPath() == "b/c");
	}

#if defined(_WIN32)
	SECTION("UNC schemes") {
		output = Path::FromString(R"(\\foo\bar)");
		CHECK(output.scheme == R"(\\)");
		CHECK(output.authority == R"(foo\bar)");
		CHECK(output.anchor == R"(\)");
		CHECK(output.GetPath().empty());

		CHECK_THROWS(Path::FromString(R"(\\\\ab)"));
		CHECK_THROWS(Path::FromString(R"(\\foo\)"));
		CHECK_THROWS(Path::FromString(R"(\\foo\\)"));
	}
#endif
}

TEST_CASE("Path::FromString/ToString round-trips", "[file_system]") {
	using std::make_tuple;

	enum ResultType { ERR = false, OK_ = true };

	ResultType return_exp;
	std::string input;
	std::string output_exp;

	// L() converts / to the local separator (mirrors playground L helper)
	auto L = [](const string &path) {
		string out(path);
#ifdef _WIN32
		for (size_t pos = 0; (pos = out.find('/', pos)) != string::npos; pos++) {
			out[pos] = '\\';
		}
#endif
		return out;
	};

	// clang-format off
	SECTION("local files") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "",         "."),
		    make_tuple(OK_, "a",        "a"),
		    make_tuple(OK_, "a/b",      "a/b"),
		    make_tuple(OK_, ".",        "."),
		    make_tuple(OK_, "..",       ".."),
		    make_tuple(OK_, "/",        "/"),
		    make_tuple(OK_, "/a",       "/a"),
		    make_tuple(OK_, "/a/b",     "/a/b"),
		    make_tuple(OK_, "/a/b/",    "/a/b"),

		    make_tuple(OK_, "foo/bar://baz",  "foo/bar:/baz"),
		    make_tuple(OK_, "/foo/bar://baz", "/foo/bar:/baz"),
		}));
	}

#if defined(_WIN32)
	SECTION("local files (windows drive)") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "C:",   "C:"),
		    make_tuple(OK_, "c:b",  "C:b"),
		    make_tuple(OK_, "c:/b", "C:/b"),
		}));
	}
#endif

	SECTION("file schemes") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "file:/",       "file:/"),
		    make_tuple(OK_, "file:/a",      "file:/a"),
		    make_tuple(OK_, "file:/a/b",    "file:/a/b"),

		    make_tuple(OK_, "file://localhost/",    "file://localhost/"),
		    make_tuple(OK_, "file://localhost/a",   "file://localhost/a"),
		    make_tuple(OK_, "file://localhost/a/b", "file://localhost/a/b"),

		    make_tuple(ERR, "file://otherhost/a/b", ""),

		    make_tuple(OK_, "file:///",     "file:///"),
		    make_tuple(OK_, "file:///a",    "file:///a"),
		    make_tuple(OK_, "file:///a/b",  "file:///a/b"),
		}));
	}

#if defined(_WIN32)
	SECTION("file schemes (windows drive)") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "file:/c:/b",               "file:/C:/b"),
		    make_tuple(OK_, "file://localhost/c://b",   "file://localhost/C:/b"),
		    make_tuple(OK_, "file:///c:///b",           "file:///C:/b"),
		    make_tuple(OK_, "file://localhost//c://b",  "file://localhost/c:/b"),
		    make_tuple(OK_, "file://localhost/c://localhost/b", "file://localhost/C:/localhost/b"),
		}));
	}
#endif

	SECTION("URI schemes") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "S3://bucket/bar/baz",     "s3://bucket/bar/baz"),
		    make_tuple(OK_, "s3://bucket/bar/baz",     "s3://bucket/bar/baz"),
		    make_tuple(OK_, "s3://bucket/bar/../baz",  "s3://bucket/baz"),
		    make_tuple(OK_, "s3://bucket/..",          "s3://bucket/"),
		    make_tuple(OK_, "s3://bucket/../..",       "s3://bucket/"),
		    make_tuple(OK_, "s3://bucket/c:/B/",       "s3://bucket/c:/B"),
		}));
	}
	// clang-format on

	CAPTURE(input);
	if (return_exp == OK_) {
		CHECK(L(Path::FromString(input).ToString()) == L(output_exp));
	} else {
		CHECK_THROWS(Path::FromString(input).ToString());
	}
}

TEST_CASE("Path::JoinPath table-based tests", "[file_system]") {
	using std::make_tuple;

	enum ResultType { ERR = false, OK_ = true };

	ResultType return_exp;
	std::string lhs, rhs, joined_exp;

	auto L = [](const string &path) {
		string out(path);
#ifdef _WIN32
		for (size_t pos = 0; (pos = out.find('/', pos)) != string::npos; pos++) {
			out[pos] = '\\';
		}
#endif
		return out;
	};

	auto do_join = [](const string &a, const string &b) {
		return Path::FromString(a).Join(b).ToString();
	};

	// clang-format off
	SECTION("local files") {
		std::tie(return_exp, lhs, rhs, joined_exp) = GENERATE(table<ResultType, std::string, std::string, std::string>({
		    make_tuple(OK_, "",   "",  "."),
		    make_tuple(OK_, "",   ".", "."),
		    make_tuple(OK_, ".",  "",  "."),
		    make_tuple(OK_, ".",  ".", "."),
		    make_tuple(OK_, "",   "a", "a"),
		    make_tuple(OK_, "a",  "",  "a"),
		    make_tuple(OK_, "a",  "b", "a/b"),
		    make_tuple(OK_, "/.", ".", "/"),
		    make_tuple(OK_, "/",  "a", "/a"),
		    make_tuple(OK_, "/a", "",  "/a"),
		    make_tuple(OK_, "/a", "b", "/a/b"),

		    // abs path sub-path helper
		    make_tuple(OK_, "/",  "/a",     "/a"),
		    make_tuple(OK_, "/",  "/a/b/c", "/a/b/c"),
		    make_tuple(OK_, "/a", "/a",     "/a"),
		    make_tuple(OK_, "/a", "/a/b",   "/a/b"),

		    // abs path fails
		    make_tuple(ERR, "/a", "/",  ""),
		    make_tuple(ERR, "/a", "/b", ""),

		    // extra slashes
		    make_tuple(OK_, "dir//sub/", "./file",      "dir/sub/file"),
		    make_tuple(OK_, "dir/sub", "../sibling",    "dir/sibling"),
		    make_tuple(OK_, "dir///", "nested///child", "dir/nested/child"),

		    // single & double dots
		    make_tuple(OK_, "/", "./..",         "/"),
		    make_tuple(OK_, "", "./..",          ".."),
		    make_tuple(OK_, "..", "a/..",        ".."),
		    make_tuple(OK_, "..", "../a",        "../../a"),
		    make_tuple(OK_, "./..", "./..",      "../.."),
		    make_tuple(OK_, "a/bar", "..",       "a"),
		    make_tuple(OK_, "a/bar", "../..",    "."),
		    make_tuple(OK_, "a/bar", "../../..", ".."),

		    make_tuple(OK_, "./a/././b/./", "././c/././", "a/b/c"),

		    make_tuple(OK_, "dir/sub/..", "sibling",        "dir/sibling"),
		    make_tuple(OK_, "./dir/sub/..", "sibling",      "dir/sibling"),
		    make_tuple(OK_, "./dir/sub/./..", "sibling",    "dir/sibling"),
		    make_tuple(OK_, "dir/..", "../..",              "../.."),
		    make_tuple(OK_, "/usr/local", "../..",          "/"),
		    make_tuple(OK_, "/", "usr/local",               "/usr/local"),
		    make_tuple(OK_, "/usr/local", "../../..",       "/"),

		    make_tuple(ERR, "dir", "/abs/path", ""),
		    make_tuple(ERR, "/fo",  "/foobar",  ""),
		}));
	}

	SECTION("file and URI schemes") {
		std::tie(return_exp, lhs, rhs, joined_exp) = GENERATE(table<ResultType, std::string, std::string, std::string>({
		    make_tuple(OK_, "file:/", "",                   "file:/"),
		    make_tuple(OK_, "file:/", "../..",              "file:/"),
		    make_tuple(OK_, "file:/", "bin",                "file:/bin"),
		    make_tuple(OK_, "file:/usr", "",                "file:/usr"),
		    make_tuple(OK_, "file:/usr", "bin",             "file:/usr/bin"),
		    make_tuple(OK_, "file:/usr/local", "../bin",    "file:/usr/bin"),

		    make_tuple(OK_, "file://localhost/usr", "../bin", "file://localhost/bin"),
		    make_tuple(OK_, "file:///usr/local", "../bin",    "file:///usr/bin"),

		    make_tuple(OK_, "s3://host", "bar/baz", "s3://host/bar/baz"),
		    make_tuple(OK_, "s3://host", "..",      "s3://host/"),
		    make_tuple(OK_, "s3://host", "../..",   "s3://host/"),

		    make_tuple(ERR, "/usr/local", "/var/log",   ""),
		    make_tuple(ERR, "s3://foo", "/foo/bar/baz", ""),
		    make_tuple(ERR, "s3://foo", "az://foo",     ""),

		    make_tuple(ERR, "s3://b/foo",      "s3://b/foobar",       ""),
		    make_tuple(ERR, "s3://BUCKET/foo", "s3://bucket/foo/bar", ""),
		    make_tuple(ERR, "s3://bucket/FOO", "s3://bucket/foo/bar", ""),

		    // sub-path joins
		    make_tuple(OK_, "file:/usr",            "file:/usr/local",            "file:/usr/local"),
		    make_tuple(OK_, "file://localhost/usr",  "file://localhost/usr/local", "file://localhost/usr/local"),
		    make_tuple(OK_, "file:///usr",           "file:///usr/local",          "file:///usr/local"),
		    make_tuple(OK_, "s3://bucket/a",         "s3://bucket/a/b",            "s3://bucket/a/b"),
		    make_tuple(OK_, "s3://bucket/a",         "s3://bucket/a",              "s3://bucket/a"),
		    // scheme normalized to lowercase (RFC 3986)
		    make_tuple(OK_, "S3://bucket/a",         "s3://bucket/a/b",            "s3://bucket/a/b"),
		}));
	}

#if defined(_WIN32)
	SECTION("windows files and schemes") {
		std::tie(return_exp, lhs, rhs, joined_exp) = GENERATE(table<ResultType, std::string, std::string, std::string>({
		    make_tuple(OK_, R"(C:\)", R"(..)",              R"(C:\)"),
		    make_tuple(OK_, R"(C:\)", R"(..\..)",           R"(C:\)"),
		    make_tuple(OK_, R"(C:\)", "system32",           R"(C:\system32)"),

		    make_tuple(OK_, "C:", "system32",               "C:system32"),
		    make_tuple(OK_, "C:relpath", "sub",             R"(C:relpath\sub)"),
		    make_tuple(OK_, "C:relpath", R"(..\sib)",       "C:sib"),

		    make_tuple(ERR, R"(C:\foo)", R"(D:\bar)",       ""),
		    make_tuple(ERR, R"(C:\foo)", R"(D:bar)",        ""),
		    make_tuple(ERR, R"(C:foo)", R"(D:bar)",         ""),
		    make_tuple(ERR, R"(C:\foo)", R"(D:\foo\bar)",   ""),

		    // extended UNC variants
		    make_tuple(OK_, R"(\\server\share)", R"(sub)",              R"(\\server\share\sub)"),
		    make_tuple(OK_, R"(\\?\UNC\server\share)", R"(sub\.\dir)",  R"(\\?\UNC\server\share\sub\dir)"),
		    make_tuple(OK_, R"(\\?\c:\sub)", R"(dir)",                  R"(\\?\C:\sub\dir)"),

		    // '..' dropping at front of abs path
		    make_tuple(OK_, R"(\\server\share)", R"(..\sibling)", R"(\\server\share\sibling)"),

		    // case-insensitive path prefix
		    make_tuple(OK_, R"(C:\FOO)",  R"(C:\foo\bar)",  R"(C:\foo\bar)"),
		    make_tuple(OK_, "C:/FOO",     "C:/foo/bar",     "C:/foo/bar"),

		    // case-insensitive UNC authority
		    make_tuple(OK_, R"(\\Server\Share\foo)",  R"(\\server\share\foo\bar)",  R"(\\Server\Share\foo\bar)"),
		}));
	}
#endif
	// clang-format on

	CAPTURE(lhs, rhs);
	if (return_exp == OK_) {
		CHECK(L(do_join(lhs, rhs)) == L(joined_exp));
	} else {
		CHECK_THROWS(do_join(lhs, rhs));
	}
}
