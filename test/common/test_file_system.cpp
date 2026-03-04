#include "catch.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/fstream.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/database_manager.hpp"
#include "test_helpers.hpp"
#if (!defined(_WIN32) && !defined(WIN32))
#define TEST_HAVE_SYMLINK
#include <unistd.h>
#else
#include "duckdb/common/windows.hpp"
#endif

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

	fs->MoveFile(fname_in_dir, fname_in_dir2);

	REQUIRE(!fs->FileExists(fname_in_dir));
	REQUIRE(fs->FileExists(fname_in_dir2));

	auto file_listing_after_move = fs->Glob(fs->JoinPath(dname_no_host, "*"));

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

struct CanonicalizationTest {
	CanonicalizationTest(string a_p, string b_p, string description_p)
	    : a(std::move(a_p)), b(std::move(b_p)), description(std::move(description_p)) {
	}

	string a;
	string b;
	string description;

	static void Test(const std::vector<CanonicalizationTest> &test_cases);
};

void CanonicalizationTest::Test(const std::vector<CanonicalizationTest> &test_cases) {
	auto fs = FileSystem::CreateLocal();
	std::vector<string> failures;
	for (auto &test : test_cases) {
		// test canonicalization
		auto canonical_a = fs->CanonicalizePath(test.a);
		auto canonical_b = fs->CanonicalizePath(test.b);

		if (canonical_a != canonical_b) {
			failures.emplace_back(StringUtil::Format("Canonical path mismatch %s <> %s (original: %s - %s)",
			                                         canonical_a, canonical_b, test.a, test.b));
		}
		if (!fs->IsPathAbsolute(test.a)) {
			// verify absolute and relative paths resolve to the same path
			auto abs_a = fs->JoinPath(fs->GetWorkingDirectory(), test.a);
			auto abs_b = fs->JoinPath(fs->GetWorkingDirectory(), test.b);
			auto canonical_abs_a = fs->CanonicalizePath(abs_a);
			auto canonical_abs_b = fs->CanonicalizePath(abs_b);
			if (canonical_abs_a != canonical_abs_b) {
				failures.emplace_back(StringUtil::Format("Canonical abs path mismatch %s <> %s (original: %s - %s)",
				                                         canonical_abs_a, canonical_abs_b, test.a, test.b));
			}
			// absolute path matches relative path
			if (canonical_abs_a != canonical_a) {
				failures.emplace_back(
				    StringUtil::Format("Canonical abs - relative path mismatch %s <> %s (original: %s)",
				                       canonical_abs_a, canonical_a, test.a));
			}
			REQUIRE(1 == 1);
		}
		REQUIRE(1 == 1);
	}
	if (!failures.empty()) {
		Printer::PrintF("------------- FAILURE -------------");
		for (auto &failure : failures) {
			Printer::PrintF("%s", failure);
		}
		FAIL();
	}
}

TEST_CASE("Test path canonicalization", "[file_system]") {
	std::vector<CanonicalizationTest> test_cases;
	test_cases.emplace_back("src/./common", "src/common", "single dot removal");
	test_cases.emplace_back("src/../common", "common", "parent directory removal");
	test_cases.emplace_back("src/common/..", "src", "trailing parent removal");
	test_cases.emplace_back("./src", "src", "leading dot removal");
	test_cases.emplace_back("src/.", "src", "trailing dot removal");
	test_cases.emplace_back("src/common/../../CMakeLists.txt", "CMakeLists.txt", "multiple parent traversal");
	test_cases.emplace_back("src/./common/./CMakeLists.txt", "src/common/CMakeLists.txt", "multiple dot removal");
	test_cases.emplace_back("./././src", "src", "repeated leading dots");
	test_cases.emplace_back("src/common/../../../CMakeLists.txt", "../CMakeLists.txt", "parent beyond root (relative)");
	test_cases.emplace_back("src//common", "src/common", "double slash");
	test_cases.emplace_back("src///common", "src/common", "triple slash");
	test_cases.emplace_back("src/common/", "src/common", "trailing slash");
	test_cases.emplace_back("src/common//", "src/common", "multiple trailing slashes");
	test_cases.emplace_back(".", ".", "single dot");
	test_cases.emplace_back("..", "..", "single parent");
	test_cases.emplace_back("", "", "empty path");
	test_cases.emplace_back("/", "/", "root only");
	test_cases.emplace_back("src", "src", "simple filename");
	test_cases.emplace_back("src/./common/../CMakeLists.txt/./qux/..", "src/CMakeLists.txt", "mixed operations");
	test_cases.emplace_back("./src/../common/./CMakeLists.txt", "common/CMakeLists.txt", "leading dot with parent");
	test_cases.emplace_back("a/b/c/../../d/e/../f", "a/d/f", "interleaved parents");

#ifdef _WIN32
	auto fs = FileSystem::CreateLocal();
	auto current_drive = fs->GetWorkingDirectory().substr(0, 1);

	// Backslash handling
	test_cases.emplace_back("src\\common", "src/common", "backslash separator equivalant to forward slash");
	test_cases.emplace_back("src\\..\\common", "common", "parent with backslash");
	test_cases.emplace_back("src/common\\CMakeLists.txt", "src\\common\\CMakeLists.txt", "mixed separators");

	// Drive letters
	test_cases.emplace_back("C:\\src\\common", "C:\\src\\common", "absolute with drive");
	test_cases.emplace_back("C:\\src\\..\\common", "C:\\common", "parent with drive");
	test_cases.emplace_back("C:src", "src", "drive-relative path");
	test_cases.emplace_back("C:src\\..\\file", "file", "drive-relative with dots");
	test_cases.emplace_back("C:", ".", "drive-relative root");
	test_cases.emplace_back("C:\\", "\\\\?\\C:\\", "drive root");
	test_cases.emplace_back("C:\\..\\common", "C:\\common", ".. in root of known drive");
	test_cases.emplace_back("Z:file", "Z:\\file", "drive-relative in unknown drive");
	test_cases.emplace_back("Z:\\src\\..\\common", "Z:\\common", "parent with drive in non-existent drive");
	test_cases.emplace_back("Z:\\..\\common", "Z:\\common", ".. in root of unknown drive");

	// UNC paths
	test_cases.emplace_back("\\\\server\\share", "\\\\server\\share", "UNC path");
	test_cases.emplace_back("\\\\server\\share\\src\\..\\common", "\\\\server\\share\\common", "UNC with parent");

	// if the current drive is not C:
	for (auto &test_case : test_cases) {
		test_case.a = StringUtil::Replace(test_case.a, "C:", current_drive + ":");
		test_case.b = StringUtil::Replace(test_case.b, "C:", current_drive + ":");
	}
#else
	test_cases.emplace_back("//src/common", "/src/common", "leading double slash");
#endif

	CanonicalizationTest::Test(test_cases);
}

void TestCreateSymlink(const string &source, const string &target, bool directory_link) {
#ifdef TEST_HAVE_SYMLINK
	if (symlink(target.c_str(), source.c_str()) != 0) {
		perror("symlink");
		FAIL();
	}
#else
	// windows - use native APIs
	if (!CreateSymbolicLinkA(source.c_str(), target.c_str(), directory_link ? SYMBOLIC_LINK_FLAG_DIRECTORY : 0)) {
		std::cerr << "Error: " << GetLastError() << std::endl;
		FAIL();
	}
#endif
}

TEST_CASE("Test path canonicalization with symlinks", "[file_system]") {
	auto fs = FileSystem::CreateLocal();

	TestCreateDirectory("real_dir");
	auto real_dir = TestCreatePath("real_dir");
	auto test_dir = TestDirectoryPath();
	auto fake_dir = TestCreatePath("fake_dir");
	auto real_file = fs->JoinPath(real_dir, "real_file");

	TestCreateDirectory(real_dir);
	// create a real file
	{
		DuckDB db(real_file);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl AS SELECT 42 i"));
	}
	// create symlink from fake_dir -> real_dir
	TestCreateSymlink(fake_dir, fs->JoinPath(fs->GetWorkingDirectory(), real_dir), true);
	// create symlink from fake_file -> real_file
	TestCreateSymlink(fs->JoinPath(fake_dir, "fake_file"), fs->JoinPath(fs->GetWorkingDirectory(), real_file), false);

	// all of these tests are in the test dir
	std::vector<CanonicalizationTest> test_cases;
	test_cases.emplace_back("fake_dir", "real_dir", "base directory");
	test_cases.emplace_back("fake_dir/file", "real_dir/file", "follow directory symlink");
	test_cases.emplace_back("fake_dir/real_file", "real_dir/real_file", "follow directory symlink for real file");
	test_cases.emplace_back("./real_dir/.././fake_dir/.//file", "real_dir/file", "follow directory symlink with dots");
	test_cases.emplace_back("fake_dir/fake_file", "real_dir/real_file", "follow file symlink");
	test_cases.emplace_back("real_dir/.././fake_dir/././fake_file", "././real_dir/././real_file",
	                        "follow file symlink with dots");

	// prepend everything with the test dir
	for (auto &test_case : test_cases) {
		test_case.a = fs->JoinPath(test_dir, test_case.a);
		test_case.b = fs->JoinPath(test_dir, test_case.b);
	}

	CanonicalizationTest::Test(test_cases);
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
