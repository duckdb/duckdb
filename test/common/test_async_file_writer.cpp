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

	data_ptr_t Ptr() override {
		return data_ptr_cast(data.data());
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

static AsyncFileWriterOptions TestOptions(idx_t coalesce_threshold = 8, idx_t max_pending_bytes_per_thread = 1024) {
	AsyncFileWriterOptions options;
	options.local_coalesce_threshold = coalesce_threshold;
	options.remote_coalesce_threshold = coalesce_threshold;
	options.max_pending_bytes_per_thread = max_pending_bytes_per_thread;
	return options;
}

static unique_ptr<Connection> CreateConnectionWithAsyncThreads(DuckDB &db, idx_t async_threads = 1) {
	auto con = make_uniq<Connection>(db);
	REQUIRE_NO_FAIL(con->Query("SET async_threads=" + to_string(async_threads)));
	return con;
}

static unique_ptr<Connection> CreateConnectionWithNoAsyncThreads(DuckDB &db) {
	auto con = make_uniq<Connection>(db);
	REQUIRE_NO_FAIL(con->Query("SET async_threads=0"));
	return con;
}

} // namespace

TEST_CASE("AsyncFileWriter requires a client context", "[async_file_writer]") {
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_no_context.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	REQUIRE_THROWS_AS(AsyncFileWriter(QueryContext(), fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions()),
	                  InvalidInputException);
	REQUIRE(!fs.FileExists(path));
}

TEST_CASE("AsyncFileWriter registers writes before async drain", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_register.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions());
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(const_data_ptr_cast("ab"), 2);
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("cd"));

		REQUIRE(writer.GetTotalWritten() == 4);
		REQUIRE(fs.write_sizes.empty());
	}

	writer.Close();
	REQUIRE(ReadFile(path) == "abcd");
	REQUIRE(fs.write_sizes.size() == 1);
	REQUIRE(fs.write_sizes[0] == 4);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter writes synchronously without async threads", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithNoAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_sync.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions());
	writer.WriteData(const_data_ptr_cast("ab"), 2);
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("cd"));

	REQUIRE(writer.GetTotalWritten() == 4);
	REQUIRE(fs.write_sizes.size() == 2);
	REQUIRE(fs.write_sizes[0] == 2);
	REQUIRE(fs.write_sizes[1] == 2);

	writer.Close();
	REQUIRE(ReadFile(path) == "abcd");
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

TEST_CASE("AsyncFileWriter flush waits for pending writes", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_flush.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions());
	auto batch_guard = writer.StartBatch();
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
	REQUIRE(fs.write_sizes.empty());

	writer.Flush();
	REQUIRE(ReadFile(path) == "abcd");
	REQUIRE(fs.write_sizes.size() == 1);
	writer.Close();
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter keeps large owned buffers as separate writes", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_large.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions(4));
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("ef"));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("gh"));
	}
	writer.Close();

	REQUIRE(ReadFile(path) == "abcdefgh");
	REQUIRE(fs.write_sizes.size() == 2);
	REQUIRE(fs.write_sizes[0] == 4);
	REQUIRE(fs.write_sizes[1] == 4);
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter applies backpressure outside registration", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_backpressure.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions(1, 0));
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("efgh"));
		REQUIRE(fs.write_sizes.empty());
	}

	writer.ApplyBackpressure();
	REQUIRE(fs.write_sizes.size() == 2);
	writer.Close();
	REQUIRE(ReadFile(path) == "abcdefgh");
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter batches writes before scheduling", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_batch.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions(1, 0));
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("efgh"));
		REQUIRE(writer.GetTotalWritten() == 8);

		writer.ApplyBackpressure();
		REQUIRE(fs.write_sizes.empty());
	}

	writer.ApplyBackpressure();
	REQUIRE(fs.write_sizes.size() == 2);
	writer.Close();
	REQUIRE(ReadFile(path) == "abcdefgh");
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter close drains an open batch", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_close_batch.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions());
	auto batch_guard = writer.StartBatch();
	writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
	writer.Close();

	REQUIRE(ReadFile(path) == "abcd");
	fs.RemoveFile(path);
}

TEST_CASE("AsyncFileWriter rethrows asynchronous write errors on close", "[async_file_writer]") {
	DuckDB db(nullptr);
	auto con = CreateConnectionWithAsyncThreads(db);
	TrackingWriteFileSystem fs;
	auto path = TestCreatePath("async_file_writer_error.tmp");
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
	fs.fail_writes = true;

	AsyncFileWriter writer(*con->context, fs, path, AsyncFileWriter::DEFAULT_OPEN_FLAGS, TestOptions());
	{
		auto batch_guard = writer.StartBatch();
		writer.WriteData(make_uniq<StringAsyncWriteBuffer>("abcd"));
	}
	REQUIRE_THROWS(writer.Close());

	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}
