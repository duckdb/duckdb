#include "catch.hpp"
#include "common/file_system.hpp"
#include "common/fstream.hpp"

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

	REQUIRE(!DirectoryExists(dname));

	CreateDirectory(dname);
	REQUIRE(DirectoryExists(dname));
	REQUIRE(!FileExists(dname));

	// we can call this again and nothing happens
	CreateDirectory(dname);

	auto fname_in_dir = JoinPath(dname, fname);
	auto fname_in_dir2 = JoinPath(dname, fname2);

	create_dummy_file(fname_in_dir);
	REQUIRE(FileExists(fname_in_dir));
	REQUIRE(!DirectoryExists(fname_in_dir));

	size_t n_files = 0;
	REQUIRE(ListFiles(dname, [&n_files](const string &path) { n_files++; }));

	REQUIRE(n_files == 1);

	REQUIRE(FileExists(fname_in_dir));
	REQUIRE(!FileExists(fname_in_dir2));

	MoveFile(fname_in_dir, fname_in_dir2);

	REQUIRE(!FileExists(fname_in_dir));
	REQUIRE(FileExists(fname_in_dir2));

	RemoveDirectory(dname);

	REQUIRE(!DirectoryExists(dname));
	REQUIRE(!FileExists(fname_in_dir));
	REQUIRE(!FileExists(fname_in_dir2));
}
