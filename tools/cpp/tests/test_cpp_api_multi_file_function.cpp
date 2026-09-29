#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/vector_size.hpp"

#include <atomic>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: MultiFileFunction, and what a table function can report about the file it reads for one
// (column identifiers, file metadata), batch claiming, and the other additions made for file readers.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

// ---------------------------------------------------------------------------
// cpp_numbers(path): reads a text file holding one integer per line. Columns: n BIGINT, info STRUCT(doubled BIGINT)
// and tags MAP(VARCHAR, BIGINT) holding {'n': n}. The file is scanned in batches of BATCH_LINES lines, one claimed at
// a time.
// ---------------------------------------------------------------------------

constexpr idx_t BATCH_LINES = 3;

struct NumbersBind {
	std::vector<int64_t> numbers;
};

struct NumbersGlobal {
	std::atomic<idx_t> next_batch {0};
};

struct NumbersLocal {
	idx_t position = 0;
	idx_t end = 0;
};

std::vector<int64_t> ReadNumbers(Context &context, const std::string &path) {
	auto fs = context.GetFileSystem();
	auto file = fs.OpenFile(path, {FileFlags::READ, FileFlags::EXTERNAL_FILE_CACHE});
	std::string contents(file.Size(), '\0');
	if (!contents.empty()) {
		file.ReadAt(&contents[0], contents.size(), 0);
	}
	std::vector<int64_t> numbers;
	size_t start = 0;
	while (start < contents.size()) {
		auto end = contents.find('\n', start);
		if (end == std::string::npos) {
			end = contents.size();
		}
		if (end > start) {
			numbers.push_back(std::stoll(contents.substr(start, end - start)));
		}
		start = end + 1;
	}
	return numbers;
}

void NumbersBind_(TableFunction::BindInput &input) {
	auto ctx = input.GetContext();
	auto path = std::string(input.GetArgument(0).Get<varchar_t>().view());
	auto numbers = ReadNumbers(ctx, path);

	input.AddResultColumn("n", ctx.ParseType("BIGINT"));
	input.AddResultColumn("info", ctx.ParseType("STRUCT(doubled BIGINT)"));
	input.AddResultColumn("tags", ctx.ParseType("MAP(VARCHAR, BIGINT)"));
	input.SetColumnIdentifier(0, Value::Create(ctx, int32_t {1}));
	input.SetColumnIdentifier(1, Value::Create(ctx, int32_t {2}));
	input.SetColumnIdentifier(1, {0}, Value::Create(ctx, int32_t {3}));
	input.SetColumnIdentifier(2, Value::Create(ctx, int32_t {4}));
	input.SetColumnIdentifier(2, {0}, Value::Create(ctx, int32_t {5}));
	input.SetColumnIdentifier(2, {1}, Value::Create(ctx, int32_t {6}));
	input.AddFileMetadata("line_count", Value::Create(ctx, static_cast<int64_t>(numbers.size())));
	input.SetBindData<NumbersBind>(NumbersBind {std::move(numbers)});
}

void NumbersInitGlobal(TableFunction::InitGlobalInput &input) {
	const auto &bind = input.GetBindData<NumbersBind>();
	input.SetGlobalState<NumbersGlobal>();
	input.SetMaxThreads((bind.numbers.size() + BATCH_LINES - 1) / BATCH_LINES + 1);
}

void NumbersInitLocal(TableFunction::InitLocalInput &input) {
	input.SetLocalState<NumbersLocal>();
}

void NumbersClaimBatch(TableFunction::ClaimBatchInput &input) {
	const auto &bind = input.GetBindData<NumbersBind>();
	auto &global = input.GetGlobalState<NumbersGlobal>();
	auto &local = input.GetLocalState<NumbersLocal>();
	auto batch = global.next_batch++;
	local.position = batch * BATCH_LINES;
	if (local.position >= bind.numbers.size()) {
		return;
	}
	local.end = std::min<idx_t>(local.position + BATCH_LINES, bind.numbers.size());
	input.SetClaimed(true);
}

void NumbersExec(TableFunction::ExecInput &input) {
	const auto &bind = input.GetBindData<NumbersBind>();
	auto &local = input.GetLocalState<NumbersLocal>();
	auto chunk = input.GetOutputChunk();

	// the batch is produced in a single chunk - the next call finds it exhausted and ends it
	idx_t count = local.end - local.position;
	for (idx_t col = 0; col < input.GetColumnCount(); col++) {
		auto vec = chunk.GetVector(col);
		switch (input.GetColumnIndex(col)) {
		case 0: {
			auto data = vec.GetDataMutable<int64_t>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = bind.numbers[local.position + i];
			}
			break;
		}
		case 1: {
			vec.SetSize(count);
			auto data = vec.GetChild(0).GetDataMutable<int64_t>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = 2 * bind.numbers[local.position + i];
			}
			break;
		}
		case 2: {
			vec.SetSize(count);
			// one entry per row: the elements of the MAP are sized with SetListSize
			vec.SetListSize(count);
			auto entries = vec.GetDataMutable<list_entry_t>();
			auto keys = vec.GetChild(0);
			auto values = vec.GetChild(1).GetDataMutable<int64_t>();
			for (idx_t i = 0; i < count; i++) {
				entries[i] = list_entry_t {i, 1};
				keys.AssignString(i, "n");
				values[i] = bind.numbers[local.position + i];
			}
			break;
		}
		default:
			throw InvalidInputException("unexpected column");
		}
	}
	local.position = local.end;
	chunk.GetVector(0).SetSize(count);
}

void RegisterNumbers(Connection &conn, const std::string &name) {
	auto function = TableFunction::Create(conn);
	function.SetName(name);
	function.GetSignature().AddParameter("path", conn.ParseType("VARCHAR"));
	function.SetBindCallback(NumbersBind_)
	    .SetInitGlobalCallback(NumbersInitGlobal)
	    .SetInitLocalCallback(NumbersInitLocal)
	    .SetClaimBatchCallback(NumbersClaimBatch)
	    .SetExecCallback(NumbersExec)
	    .SetProjectionPushdown(true);
	function.Register();
}

void RegisterMultiFile(Connection &conn, const std::string &name, const std::string &single_file_function,
                       const std::string &extension = "") {
	auto function = MultiFileFunction::Create(conn);
	function.SetName(name).SetSingleFileFunction(single_file_function).SetReaderType("Numbers");
	if (!extension.empty()) {
		function.SetFileExtension(extension);
	}
	function.Register();
}

std::string WriteNumbers(const std::string &path, int64_t from, int64_t to) {
	std::ofstream out(path, std::ios::binary);
	for (int64_t i = from; i <= to; i++) {
		out << i << "\n";
	}
	return path;
}

std::vector<int64_t> CollectBigints(QueryResult result) {
	std::vector<int64_t> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.push_back(view.Data<int64_t>()[view.SelAt(i)]);
		}
	}
	return out;
}

std::vector<std::string> CollectStrings(QueryResult result) {
	std::vector<std::string> out;
	while (auto chunk = result.FetchChunk()) {
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			out.push_back(chunk.GetVector(0).GetValue(i).ToText());
		}
	}
	return out;
}

// A bind that attaches an identifier to a nested field the column does not have.
void BadIdentifierBind(TableFunction::BindInput &input) {
	auto ctx = input.GetContext();
	input.AddResultColumn("n", ctx.ParseType("BIGINT"));
	input.SetColumnIdentifier(0, {0}, Value::Create(ctx, int32_t {1}));
}

void NoopExec(TableFunction::ExecInput &) {
}

// ---------------------------------------------------------------------------
// cpp_counting_copy: writes nothing but a line per batch, and reports the rows and bytes it wrote.
// ---------------------------------------------------------------------------

struct CountingInit {
	std::mutex lock;
	idx_t rows = 0;
};

struct CountingBatch {
	idx_t rows = 0;
};

void CountingInit_(CopyFunction::CopyToInitInput &input) {
	input.SetInitData<CountingInit>();
}

void CountingBatch_(CopyFunction::CopyToBatchInput &input) {
	input.SetBatchData<CountingBatch>(CountingBatch {input.TakeBatch().GetRowCount()});
}

void CountingFlush(CopyFunction::CopyToFlushInput &input) {
	auto &init = input.GetInitData<CountingInit>();
	std::lock_guard<std::mutex> guard(init.lock);
	init.rows += input.GetBatchData<CountingBatch>().rows;
}

void CountingStatistics(CopyFunction::CopyToStatisticsInput &input) {
	auto &init = input.GetInitData<CountingInit>();
	input.SetRowCount(init.rows);
	input.SetFileSize(42 + init.rows);
}

} // namespace

TEST_CASE("Stable C++API: multi-file function reads lists and globs of files", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	RegisterNumbers(conn, "cpp_numbers");
	RegisterMultiFile(conn, "cpp_numbers_multi", "cpp_numbers");

	auto a = WriteNumbers(duckdb::TestCreatePath("cpp_numbers_a.txt"), 1, 10);
	auto b = WriteNumbers(duckdb::TestCreatePath("cpp_numbers_b.txt"), 11, 20);

	REQUIRE(CollectBigints(conn.Execute("SELECT sum(n) FROM cpp_numbers_multi('" + a + "')")) ==
	        std::vector<int64_t> {55});
	REQUIRE(CollectBigints(conn.Execute("SELECT sum(n) FROM cpp_numbers_multi(['" + a + "', '" + b + "'])")) ==
	        std::vector<int64_t> {210});
	auto glob = duckdb::TestCreatePath("cpp_numbers_*.txt");
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_numbers_multi('" + glob + "')")) ==
	        std::vector<int64_t> {20});
	// the options of every multi-file function, e.g. the filename column
	REQUIRE(CollectBigints(conn.Execute("SELECT count(DISTINCT filename) FROM cpp_numbers_multi('" + glob +
	                                    "', filename = true)")) == std::vector<int64_t> {2});
	// the nested columns, including a MAP sized with SetListSize
	REQUIRE(CollectBigints(conn.Execute("SELECT info.doubled + tags['n'] FROM cpp_numbers_multi('" + a +
	                                    "') WHERE n = 7")) == std::vector<int64_t> {21});
	// a scan that only counts rows requests no columns
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_numbers_multi('" + a + "')")) ==
	        std::vector<int64_t> {10});
	// a missing file is reported by the multi-file function
	REQUIRE_THROWS_AS(
	    conn.Execute("SELECT * FROM cpp_numbers_multi('" + duckdb::TestCreatePath("nope.txt") + "')").Drain(),
	    Exception);
}

TEST_CASE("Stable C++API: batches claimed by a multi-file function keep their order", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	RegisterNumbers(conn, "cpp_numbers");
	RegisterMultiFile(conn, "cpp_numbers_multi", "cpp_numbers");
	conn.Execute("SET threads = 8").Drain();

	auto path = WriteNumbers(duckdb::TestCreatePath("cpp_numbers_ordered.txt"), 1, 3000);
	std::vector<int64_t> expected;
	for (int64_t i = 1; i <= 3000; i++) {
		expected.push_back(i);
	}
	// every batch of the file is scanned on its own, and the batches are put back in order
	REQUIRE(CollectBigints(conn.Execute("SELECT n FROM cpp_numbers_multi('" + path + "')")) == expected);

	// scanned on its own, the function claims its batches itself
	REQUIRE(CollectBigints(conn.Execute("SELECT sum(n), count(*) FROM cpp_numbers('" + path + "')")) ==
	        std::vector<int64_t> {4501500});
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_numbers('" + path + "')")) ==
	        std::vector<int64_t> {3000});
}

TEST_CASE("Stable C++API: multi-file function with a file extension reads directories", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	RegisterNumbers(conn, "cpp_numbers");
	RegisterMultiFile(conn, "cpp_numbers_dir", "cpp_numbers", "txt");

	auto dir = duckdb::TestCreatePath("cpp_numbers_dir");
	duckdb::TestCreateDirectory(dir);
	WriteNumbers(dir + "/first.txt", 1, 4);
	WriteNumbers(dir + "/second.txt", 5, 6);
	REQUIRE(CollectBigints(conn.Execute("SELECT sum(n) FROM cpp_numbers_dir('" + dir + "')")) ==
	        std::vector<int64_t> {21});
}

TEST_CASE("Stable C++API: multi-file function registration validation", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	RegisterNumbers(conn, "cpp_numbers");

	auto missing_name = MultiFileFunction::Create(conn);
	missing_name.SetSingleFileFunction("cpp_numbers");
	REQUIRE_THROWS_MATCHES(missing_name.Register(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	auto missing_function = MultiFileFunction::Create(conn);
	missing_function.SetName("cpp_multi");
	REQUIRE_THROWS_MATCHES(missing_function.Register(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	auto unknown_function = MultiFileFunction::Create(conn);
	unknown_function.SetName("cpp_multi").SetSingleFileFunction("cpp_does_not_exist");
	REQUIRE_THROWS_MATCHES(unknown_function.Register(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// range(n) does not take the path of a file
	auto wrong_signature = MultiFileFunction::Create(conn);
	wrong_signature.SetName("cpp_multi").SetSingleFileFunction("range");
	REQUIRE_THROWS_MATCHES(wrong_signature.Register(), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	auto dotted_extension = MultiFileFunction::Create(conn);
	REQUIRE_THROWS_MATCHES(dotted_extension.SetFileExtension(".txt"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: column identifiers must address a field of the column", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();

	auto function = TableFunction::Create(conn);
	function.SetName("cpp_bad_identifier");
	function.SetBindCallback(BadIdentifierBind).SetExecCallback(NoopExec);
	function.Register();
	REQUIRE_THROWS_MATCHES(conn.Execute("SELECT * FROM cpp_bad_identifier()").Drain(), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: copy function reports written statistics", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("SET threads = 1").Drain();

	auto function = CopyFunction::Create(conn);
	function.SetName("cpp_counting_copy")
	    .SetCopyToInitCallback(CountingInit_)
	    .SetCopyToBatchCallback(CountingBatch_)
	    .SetCopyToFlushCallback(CountingFlush)
	    .SetCopyToStatisticsCallback(CountingStatistics);
	function.Register();

	auto path = duckdb::TestCreatePath("cpp_counting_copy.txt");
	{
		auto result =
		    conn.Execute("COPY (SELECT * FROM range(5000)) TO '" + path + "' (FORMAT cpp_counting_copy, RETURN_STATS)");
		auto chunk = result.FetchChunk();
		REQUIRE(chunk);
		REQUIRE(chunk.GetRowCount() == 1);
		// (filename, count, file_size_bytes, ...)
		REQUIRE(chunk.GetVector(1).GetValue(0).Get<uint64_t>() == 5000);
		REQUIRE(chunk.GetVector(2).GetValue(0).Get<uint64_t>() == 5042);
	}

	// without a statistics callback, RETURN_STATS is rejected
	auto no_statistics = CopyFunction::Create(conn);
	no_statistics.SetName("cpp_no_statistics_copy")
	    .SetCopyToInitCallback(CountingInit_)
	    .SetCopyToBatchCallback(CountingBatch_)
	    .SetCopyToFlushCallback(CountingFlush);
	no_statistics.Register();
	REQUIRE_THROWS_MATCHES(conn.Execute("COPY (SELECT 42) TO '" + path +
	                                    "' (FORMAT cpp_no_statistics_copy, "
	                                    "RETURN_STATS)")
	                           .Drain(),
	                       Exception, HasErrorCode(DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED));
}

TEST_CASE("Stable C++API: reading through the external file cache", "[cpp_api]") {
	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	conn.Execute("SET cache_local_files = true").Drain();

	auto path = WriteNumbers(duckdb::TestCreatePath("cpp_cached.txt"), 1, 3);
	auto fs = conn.GetFileSystem();
	{
		auto file = fs.OpenFile(path, {FileFlags::READ, FileFlags::EXTERNAL_FILE_CACHE});
		std::string contents(file.Size(), '\0');
		file.ReadAt(&contents[0], contents.size(), 0);
		REQUIRE(contents == "1\n2\n3\n");
	}
	REQUIRE(CollectStrings(conn.Execute("SELECT path FROM duckdb_external_file_cache()")) ==
	        std::vector<std::string> {path});
	// only files opened for reading can be read through the cache
	REQUIRE_THROWS_MATCHES(fs.OpenFile(path, {FileFlags::WRITE, FileFlags::EXTERNAL_FILE_CACHE}), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: standard vector size and type copies", "[cpp_api]") {
	REQUIRE(StandardVectorSize() == STANDARD_VECTOR_SIZE);

	Environment env;
	auto db = env.Open(":memory:");
	auto conn = db.Connect();
	auto type = conn.ParseType("STRUCT(a INTEGER, b VARCHAR[])");
	auto copy = type.Copy();
	REQUIRE(copy == type);
	REQUIRE(copy.ToText() == type.ToText());
}
