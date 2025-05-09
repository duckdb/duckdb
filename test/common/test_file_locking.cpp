#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/file_system.hpp"

#include <thread>
#include <atomic>

using namespace duckdb;
using namespace std;

static constexpr const int THREAD_COUNT = 8;
static constexpr const idx_t ITERATIONS = 100;
static atomic<int> finished_threads;
static atomic<bool> encountered_error;

static void read_file(FileSystem *fs, const string &path) {
	for (idx_t i = 0; i < ITERATIONS; i++) {
		try {
			// Open with READ_LOCK
			auto handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_READ | FileLockType::READ_LOCK);
			// Read some data
			data_t buffer[1024];
			handle->Read(buffer, sizeof(buffer), 0);
			// Keep file open briefly to increase chance of concurrent access
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			handle.reset();
		} catch (...) {
			encountered_error = true;
		}
	}
	finished_threads++;
}

static void write_file(FileSystem *fs, const string &path) {
	for (idx_t i = 0; i < ITERATIONS; i++) {
		try {
			// Open with WRITE_LOCK
			auto handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileLockType::WRITE_LOCK);
			// Write some data
			string data = "test data " + to_string(i);
			handle->Write((void *)data.data(), data.size(), 0);
			// Keep file open briefly to increase chance of concurrent access
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			handle.reset();
		} catch (...) {
			encountered_error = true;
		}
	}
	finished_threads++;
}

static void read_then_write_file(FileSystem *fs, const string &path) {
	for (idx_t i = 0; i < ITERATIONS; i++) {
		try {
			// First get READ_LOCK
			auto handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_READ | FileLockType::READ_LOCK);
			data_t buffer[1024];
			handle->Read(buffer, sizeof(buffer), 0);
			handle.reset();

			// Then try to get WRITE_LOCK
			handle = fs->OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileLockType::WRITE_LOCK);
			string data = "test data " + to_string(i);
			handle->Write((void *)data.data(), data.size(), 0);
			handle.reset();
		} catch (...) {
			encountered_error = true;
		}
	}
	finished_threads++;
}

// Single writer should block other writers
TEST_CASE("Test concurrent writers block each other", "[file_lock]") {
	auto fs = FileSystem::CreateLocal();
	auto test_path = TestCreatePath("test_write_lock");

	// Create initial file
	auto handle = fs->OpenFile(test_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                                          FileLockType::WRITE_LOCK);
	string initial_data = "initial";
	handle->Write((void *)initial_data.data(), initial_data.size(), 0);
	handle.reset();

	finished_threads = 0;
	encountered_error = false;

	thread threads[THREAD_COUNT];
	// Launch multiple writer threads
	for (int i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(write_file, fs.get(), test_path);
	}

	for (int i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// All threads finished
	REQUIRE(finished_threads == THREAD_COUNT);
	// Should have encountered some errors due to lock conflicts
	REQUIRE(encountered_error);
}

// Multiple readers should work concurrently
TEST_CASE("Test concurrent readers can access simultaneously", "[file_lock]") {
	auto fs = FileSystem::CreateLocal();
	auto test_path = TestCreatePath("test_read_lock");

	// Create initial file
	auto handle = fs->OpenFile(test_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                                          FileLockType::WRITE_LOCK);
	string initial_data = "initial";
	handle->Write((void *)initial_data.data(), initial_data.size(), 0);
	handle.reset();

	finished_threads = 0;
	encountered_error = false;

	thread threads[THREAD_COUNT];
	// Launch multiple reader threads
	for (int i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(read_file, fs.get(), test_path);
	}

	for (int i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// All threads finished
	REQUIRE(finished_threads == THREAD_COUNT);
	// Should not encounter errors since readers can run concurrently
	REQUIRE(!encountered_error);
}

// Writers should block readers and vice versa
TEST_CASE("Test writers block readers and vice versa", "[file_lock]") {
	auto fs = FileSystem::CreateLocal();
	auto test_path = TestCreatePath("test_read_write_lock");

	// Create initial file
	auto handle = fs->OpenFile(test_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                                          FileLockType::WRITE_LOCK);
	string initial_data = "initial";
	handle->Write((void *)initial_data.data(), initial_data.size(), 0);
	handle.reset();

	finished_threads = 0;
	encountered_error = false;

	thread threads[THREAD_COUNT];
	// Launch mix of reader and writer threads
	for (int i = 0; i < THREAD_COUNT; i++) {
		if (i % 2 == 0) {
			threads[i] = thread(read_file, fs.get(), test_path);
		} else {
			threads[i] = thread(write_file, fs.get(), test_path);
		}
	}

	for (int i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// All threads finished
	REQUIRE(finished_threads == THREAD_COUNT);
	// Should have encountered some errors due to lock conflicts
	REQUIRE(encountered_error);
}

// Test upgrading from read lock to write lock
TEST_CASE("Test read lock to write lock upgrade", "[file_lock]") {
	auto fs = FileSystem::CreateLocal();
	auto test_path = TestCreatePath("test_lock_upgrade");

	// Create initial file
	auto handle = fs->OpenFile(test_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                                          FileLockType::WRITE_LOCK);
	string initial_data = "initial";
	handle->Write((void *)initial_data.data(), initial_data.size(), 0);
	handle.reset();

	finished_threads = 0;
	encountered_error = false;

	thread threads[THREAD_COUNT];
	// Launch threads that try to upgrade from read to write
	for (int i = 0; i < THREAD_COUNT; i++) {
		threads[i] = thread(read_then_write_file, fs.get(), test_path);
	}

	for (int i = 0; i < THREAD_COUNT; i++) {
		threads[i].join();
	}

	// All threads finished
	REQUIRE(finished_threads == THREAD_COUNT);
	// Should have encountered some errors due to lock conflicts during upgrades
	REQUIRE(encountered_error);
}
