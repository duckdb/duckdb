#include "catch.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/fstream.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

static void create_dummy_file(string fname) {
	ofstream outfile(fname);
	outfile << "I_AM_A_DUMMY" << endl;
	outfile.close();
}

TEST_CASE("Make sure file system operators work as advertised", "[file_system]") {
	FileSystem fs;
	auto dname = TestCreatePath("TEST_DIR");
	string fname = "TEST_FILE";
	string fname2 = "TEST_FILE_TWO";

	if (fs.DirectoryExists(dname)) {
		fs.RemoveDirectory(dname);
	}

	fs.CreateDirectory(dname);
	REQUIRE(fs.DirectoryExists(dname));
	REQUIRE(!fs.FileExists(dname));

	// we can call this again and nothing happens
	fs.CreateDirectory(dname);

	auto fname_in_dir = fs.JoinPath(dname, fname);
	auto fname_in_dir2 = fs.JoinPath(dname, fname2);

	create_dummy_file(fname_in_dir);
	REQUIRE(fs.FileExists(fname_in_dir));
	REQUIRE(!fs.DirectoryExists(fname_in_dir));

	size_t n_files = 0;
	REQUIRE(fs.ListFiles(dname, [&n_files](const string &path) { n_files++; }));

	REQUIRE(n_files == 1);

	REQUIRE(fs.FileExists(fname_in_dir));
	REQUIRE(!fs.FileExists(fname_in_dir2));

	fs.MoveFile(fname_in_dir, fname_in_dir2);

	REQUIRE(!fs.FileExists(fname_in_dir));
	REQUIRE(fs.FileExists(fname_in_dir2));

	fs.RemoveDirectory(dname);

	REQUIRE(!fs.DirectoryExists(dname));
	REQUIRE(!fs.FileExists(fname_in_dir));
	REQUIRE(!fs.FileExists(fname_in_dir2));
}

// note: the integer count is chosen as 512 so that we write 512*8=4096 bytes to the file
// this is required for the Direct-IO as on Windows Direct-IO can only write multiples of sector sizes
// sector sizes are typically one of [512/1024/2048/4096] bytes, hence a 4096 bytes write succeeds.
#define INTEGER_COUNT 512

TEST_CASE("Test file operations", "[file_system]") {
	FileSystem fs;
	unique_ptr<FileHandle> handle, handle2;
	int64_t test_data[INTEGER_COUNT];
	for (int i = 0; i < INTEGER_COUNT; i++) {
		test_data[i] = i;
	}

	auto fname = TestCreatePath("test_file");

	// standard reading/writing test

	// open file for writing
	REQUIRE_NOTHROW(handle = fs.OpenFile(fname, FileFlags::WRITE | FileFlags::CREATE, FileLockType::NO_LOCK));
	// write 10 integers
	REQUIRE_NOTHROW(handle->Write((void *)test_data, sizeof(int64_t) * INTEGER_COUNT, 0));
	// close the file
	handle.reset();

	for (int i = 0; i < INTEGER_COUNT; i++) {
		test_data[i] = 0;
	}
	// now open the file for reading
	REQUIRE_NOTHROW(handle = fs.OpenFile(fname, FileFlags::READ, FileLockType::NO_LOCK));
	// read the 10 integers back
	REQUIRE_NOTHROW(handle->Read((void *)test_data, sizeof(int64_t) * INTEGER_COUNT, 0));
	// check the values of the integers
	for (int i = 0; i < 10; i++) {
		REQUIRE(test_data[i] == i);
	}
	handle.reset();
	fs.RemoveFile(fname);
}

TEST_CASE("Test file buffers for reading/writing to file", "[file_system]") {
	FileSystem fs;
	unique_ptr<FileHandle> handle;

	auto fname = TestCreatePath("test_file");

	// create the buffer and fill it with data
	auto buf = make_unique<FileBuffer>(FileBufferType::MANAGED_BUFFER, 4096);
	int64_t *ptr = (int64_t *)buf->buffer;
	for (size_t i = 0; i < 10; i++) {
		ptr[i] = i;
	}

	// open file for writing
	REQUIRE_NOTHROW(handle = fs.OpenFile(fname, FileFlags::WRITE | FileFlags::CREATE | FileFlags::DIRECT_IO,
	                                     FileLockType::WRITE_LOCK));
	// write the buffer
	REQUIRE_NOTHROW(buf->Write(*handle, 0));
	// clear the buffer
	buf->Clear();
	// now read data back into the buffer
	REQUIRE_NOTHROW(buf->Read(*handle, 0));
	for (size_t i = 0; i < 10; i++) {
		REQUIRE(ptr[i] == i);
	}
	// close the file
	handle.reset();

	fs.RemoveFile(fname);
}
