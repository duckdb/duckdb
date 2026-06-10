#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/serializer/async_file_writer.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

class TrackingWriteFileSystem : public LocalFileSystem {
public:
	string GetName() const override {
		return "TrackingWriteFileSystem";
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		if (fail_writes) {
			throw IOException("Injected async write failure");
		}
		write_sizes.push_back(UnsafeNumericCast<idx_t>(nr_bytes));
		return LocalFileSystem::Write(handle, buffer, nr_bytes);
	}

public:
	vector<idx_t> write_sizes;
	bool fail_writes = false;
};

class StringAsyncWriteBuffer : public AsyncWriteBuffer {
public:
	explicit StringAsyncWriteBuffer(string data_p) : data(std::move(data_p)) {
	}

	const_data_ptr_t Ptr() const override {
		return const_data_ptr_cast(data.data());
	}

	idx_t Size() const override {
		return data.size();
	}

private:
	string data;
};

static string ReadFile(const string &path) {
	LocalFileSystem fs;
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = NumericCast<idx_t>(handle->GetFileSize());
	string result(file_size, '\0');
	handle->Read(data_ptr_cast(result.data()), file_size);
	return result;
}

static AsyncFileWriterOptions TestOptions(idx_t coalesce_threshold = 8, idx_t high_watermark = 0,
                                          idx_t low_watermark = 0) {
	AsyncFileWriterOptions options;
	options.local_coalesce_threshold = coalesce_threshold;
	options.remote_coalesce_threshold = coalesce_threshold;
	options.high_watermark = high_watermark;
	options.low_watermark = low_watermark;
	return options;
}

static unique_ptr<Connection> CreateConnectionWithNoAsyncThreads(DuckDB &db) {
	auto con = make_uniq<Connection>(db);
	REQUIRE_NO_FAIL(con->Query("SET async_threads=0"));
	return con;
}

} // namespace

TEST_CASE("AsyncFileWriter registers writes before async drain", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_register.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions());
	writer.WriteData(const_data_ptr_cast("ab"), 2);
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("cd"));

	REQUIRE(writer.GetTotalWritten() == 4);
	REQUIRE(fs.write_sizes.empty());

	writer.Close();
	REQUIRE(ReadFile(path) == "abcd");
	REQUIRE(fs.write_sizes.size() == 1);
	REQUIRE(fs.write_sizes[0] == 4);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter copies transient WriteData input", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_copy.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	string payload = "abcdef";
	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions());
	writer.WriteData(const_data_ptr_cast(payload.data()), payload.size());
	payload = "XXXXXX";
	writer.Close();

	REQUIRE(ReadFile(path) == "abcdef");
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter keeps large owned buffers as separate writes", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_large.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions(4));
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("ef"));
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("gh"));
	writer.Close();

	REQUIRE(ReadFile(path) == "abcdefgh");
	REQUIRE(fs.write_sizes.size() == 2);
	REQUIRE(fs.write_sizes[0] == 4);
	REQUIRE(fs.write_sizes[1] == 4);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter applies backpressure outside registration", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_backpressure.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions(1, 4, 0));
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("efgh"));
	REQUIRE(fs.write_sizes.empty());

	writer.ApplyBackpressure();
	REQUIRE(fs.write_sizes.size() == 2);
	writer.Close();
	REQUIRE(ReadFile(path) == "abcdefgh");
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter rethrows asynchronous write errors on close", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_error.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
	fs.fail_writes = true;

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions());
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
	REQUIRE_THROWS(writer.Close());

	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}
