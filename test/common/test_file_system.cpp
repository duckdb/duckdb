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
	auto dname_triple_slash = fs->JoinPath("file://", dname_converted_slashes);
	// Path of format file://localhost/bla/bla on 'nix and file://localhost/X:/bla/bla on Windows
	auto dname_localhost = fs->JoinPath("file://localhost", dname_converted_slashes);
	auto dname_no_host = fs->JoinPath("file:", dname_converted_slashes);

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
	REQUIRE(normalized == collapse("dir/subdir/file"));

	auto parent = fs->JoinPath("dir/subdir", "../sibling");
	REQUIRE(parent == collapse("dir/sibling"));

	auto abs_override = fs->JoinPath("dir", "/abs/path");
	REQUIRE(abs_override == fs->ConvertSeparators("/abs/path"));

	auto dedup = fs->JoinPath("dir///", "nested///child");
	REQUIRE(dedup == collapse("dir/nested/child"));
}

TEST_CASE("JoinPath handles edge cases", "[file_system]") {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	auto sep = fs->PathSeparator("dummy");
	auto collapse = [&](const string &path) {
		return StringUtil::Replace(path, "/", sep);
	};

	auto lhs_parent = fs->JoinPath("dir/subdir/..", "sibling");
	REQUIRE(lhs_parent == collapse("dir/sibling"));

	auto mixed_dots = fs->JoinPath("./dir/./subdir/./..", "./sibling/.");
	REQUIRE(mixed_dots == collapse("dir/sibling"));

	auto overflowing_rel = fs->JoinPath("dir/..", "../..");
	REQUIRE(overflowing_rel == fs->ConvertSeparators("../.."));

	auto walk_up_absolute = fs->JoinPath("/usr/local", "../..");
	REQUIRE(walk_up_absolute == fs->ConvertSeparators("/"));

	auto root_child = fs->JoinPath("/", "usr/local");
	REQUIRE(root_child == fs->ConvertSeparators("/usr/local"));

	auto past_root = fs->JoinPath("/usr/local", "../../..");
	REQUIRE(past_root == fs->ConvertSeparators("/"));

	auto uri_join = fs->JoinPath("file:/usr/local", "../bin");
	REQUIRE(uri_join == "file:/usr/local/../bin");

#ifdef _WIN32
	auto clamp_drive_root = fs->JoinPath(R"(C:\)", R"(..)");
	REQUIRE(clamp_drive_root == fs->ConvertSeparators("C:/"));

	auto clamp_drive_root_twice = fs->JoinPath(R"(C:\)", R"(..\..)");
	REQUIRE(clamp_drive_root_twice == fs->ConvertSeparators("C:/"));

	auto drive_relative = fs->JoinPath("C:", "system32");
	REQUIRE(drive_relative == "C:system32");

	auto drive_absolute_child = fs->JoinPath(R"(C:\)", "system32");
	REQUIRE(drive_absolute_child == fs->ConvertSeparators("C:/system32"));

	auto drive_relative_child = fs->JoinPath("C:drive_relative_path", "path");
	REQUIRE(drive_relative_child == fs->ConvertSeparators("C:drive_relative_path/path"));

	auto drive_relative_parent = fs->JoinPath("C:drive_relative_path", R"(..\path)");
	REQUIRE(drive_relative_parent == "C:path");
#endif
}

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
	REQUIRE(fs.NormalizeAbsolutePath("C:/folder\\filename.csv") == "c:\\folder\\filename.csv");
	REQUIRE(fs.NormalizeAbsolutePath(network) == network);
	REQUIRE(fs.NormalizeAbsolutePath(long_path) == "\\\\?\\d:\\very long network\\");
#endif
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
