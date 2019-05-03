#include "catch.hpp"
#include "common/file_system.hpp"
#include "common/fstream.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

static void create_dummy_file(string fname) {
	ofstream outfile(fname);
	outfile << "I_AM_A_DUMMY" << endl;
	outfile.close();
}

static string random_string(size_t length) {
	auto randchar = []() -> char {
		const char charset[] = "0123456789"
		                       "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		                       "abcdefghijklmnopqrstuvwxyz";
		const size_t max_index = (sizeof(charset) - 1);
		return charset[rand() % max_index];
	};
	std::string str(length, 0);
	std::generate_n(str.begin(), length, randchar);
	return str;
}

TEST_CASE("Make sure file system operators work as advertised", "[file_system]") {
	string dname = "TEST_DIR_" + random_string(10);
	string fname = "TEST_FILE";
	string fname2 = "TEST_FILE_TWO";

	REQUIRE(!FileSystem::DirectoryExists(dname));

	FileSystem::CreateDirectory(dname);
	REQUIRE(FileSystem::DirectoryExists(dname));
	REQUIRE(!FileSystem::FileExists(dname));

	// we can call this again and nothing happens
	FileSystem::CreateDirectory(dname);

	auto fname_in_dir = FileSystem::JoinPath(dname, fname);
	auto fname_in_dir2 = FileSystem::JoinPath(dname, fname2);

	create_dummy_file(fname_in_dir);
	REQUIRE(FileSystem::FileExists(fname_in_dir));
	REQUIRE(!FileSystem::DirectoryExists(fname_in_dir));

	size_t n_files = 0;
	REQUIRE(FileSystem::ListFiles(dname, [&n_files](const string &path) { n_files++; }));

	REQUIRE(n_files == 1);

	REQUIRE(FileSystem::FileExists(fname_in_dir));
	REQUIRE(!FileSystem::FileExists(fname_in_dir2));

	FileSystem::MoveFile(fname_in_dir, fname_in_dir2);

	REQUIRE(!FileSystem::FileExists(fname_in_dir));
	REQUIRE(FileSystem::FileExists(fname_in_dir2));

	FileSystem::RemoveDirectory(dname);

	REQUIRE(!FileSystem::DirectoryExists(dname));
	REQUIRE(!FileSystem::FileExists(fname_in_dir));
	REQUIRE(!FileSystem::FileExists(fname_in_dir2));
}

TEST_CASE("Test file operations", "[file_system]") {
	unique_ptr<FileHandle> handle, handle2;
	int64_t test_data[10];
	int64_t test_data2[10];
	for(int i = 0; i < 10; i++) {
		test_data[i] = i;
		test_data2[i] = 0;
	}

	auto fname = FileSystem::JoinPath(TESTING_DIRECTORY_NAME, "test_file");

	// standard reading/writing test

	// open file for writing
	REQUIRE_NOTHROW(handle = FileSystem::OpenFile(fname, FileFlags::WRITE | FileFlags::CREATE, FileLockType::NO_LOCK));
	// write 10 integers
	REQUIRE_NOTHROW(handle->Write((void*)test_data, sizeof(int64_t) * 10, 0));
	// close the file
	handle.reset();

	for(int i = 0; i < 10; i++) {
		test_data[i] = 0;
	}
	// now open the file for reading
	REQUIRE_NOTHROW(handle = FileSystem::OpenFile(fname, FileFlags::READ, FileLockType::NO_LOCK));
	// read the 10 integers back
	REQUIRE_NOTHROW(handle->Read((void*)test_data, sizeof(int64_t) * 10, 0));
	// check the values of the integers
	for(int i = 0; i < 10; i++) {
		REQUIRE(test_data[i] == i);
	}
	handle.reset();
	FileSystem::RemoveFile(fname);

	// now test direct IO
	REQUIRE_NOTHROW(handle = FileSystem::OpenFile(fname, FileFlags::WRITE | FileFlags::CREATE | FileFlags::DIRECT_IO, FileLockType::NO_LOCK));
	// write 10 integers
	REQUIRE_NOTHROW(handle->Write((void*)test_data, sizeof(int64_t) * 10, 0));
	// now read the integers using a separate handle, they should be there already
	REQUIRE_NOTHROW(handle2 = FileSystem::OpenFile(fname, FileFlags::READ, FileLockType::NO_LOCK));
	REQUIRE_NOTHROW(handle2->Read((void*)test_data2, sizeof(int64_t) * 10, 0));
	for(int i = 0; i < 10; i++) {
		REQUIRE(test_data2[i] == i);
	}
	handle.reset();
	handle2.reset();
	FileSystem::RemoveFile(fname);

	// test file locks
	// NOTE: we can't actually test contention of locks, as the locks are held per process
	// i.e. if we got two write locks to the same file, they would both succeed because our process would hold the write lock already
	// the only way to properly test these locks is to use multiple processes

	// we can get a write lock to a file
	REQUIRE_NOTHROW(handle = FileSystem::OpenFile(fname, FileFlags::WRITE | FileFlags::CREATE, FileLockType::WRITE_LOCK));
	handle.reset();

	// we can get a read lock on a file
	REQUIRE_NOTHROW(handle = FileSystem::OpenFile(fname, FileFlags::READ, FileLockType::READ_LOCK));
	handle.reset();

	FileSystem::RemoveFile(fname);
}
