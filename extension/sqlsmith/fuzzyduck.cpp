#include "fuzzyduck.hpp"
#include "duckdb/common/random_engine.hpp"
#include "statement_generator.hpp"
#include <algorithm>
#include <random>
#include <thread>

namespace duckdb {

FuzzyDuck::FuzzyDuck(ClientContext &context) : context(context) {
}

FuzzyDuck::~FuzzyDuck() {
}

void FuzzyDuck::BeginFuzzing() {
	auto &random_engine = RandomEngine::Get(context);
	if (seed == 0) {
		seed = random_engine.NextRandomInteger();
	}
	if (max_queries == 0) {
		max_queries = NumericLimits<idx_t>::Maximum();
	}
	if (!complete_log.empty()) {
		auto &fs = FileSystem::GetFileSystem(context);
		TryRemoveFile(complete_log);
		complete_log_handle =
		    fs.OpenFile(complete_log, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	}
}

void FuzzyDuck::EndFuzzing() {
	if (complete_log_handle) {
		complete_log_handle->Close();
	}
}

void FuzzyDuck::Fuzz() {
	BeginFuzzing();
	for (idx_t i = 0; i < max_queries; i++) {
		LogMessage("Query " + to_string(i) + "\n");
		auto query = GenerateQuery();
		RunQuery(std::move(query));
	}
	EndFuzzing();
}

void FuzzyDuck::FuzzAllFunctions() {
	StatementGenerator generator(context);
	auto queries = generator.GenerateAllFunctionCalls();

	std::default_random_engine e(seed);
	std::shuffle(std::begin(queries), std::end(queries), e);
	BeginFuzzing();
	for (auto &query : queries) {
		RunQuery(std::move(query));
	}
	EndFuzzing();
}

string FuzzyDuck::GenerateQuery() {
	LogTask("Generating query with seed " + to_string(seed));
	auto &engine = RandomEngine::Get(context);
	// set the seed
	engine.SetSeed(seed);
	// get the next seed
	seed = engine.NextRandomInteger();

	// generate the statement
	StatementGenerator generator(context);
	auto statement = generator.GenerateStatement();
	return statement->ToString();
}

void sleep_thread(Connection *con, atomic<bool> *is_active, atomic<bool> *timed_out, idx_t timeout_duration) {
	// timeout is given in seconds
	// we wait 10ms per iteration, so timeout * 100 gives us the amount of
	// iterations
	for (size_t i = 0; i < (size_t)(timeout_duration * 100) && *is_active; i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	if (*is_active) {
		*timed_out = true;
		con->Interrupt();
	}
}

void FuzzyDuck::RunQuery(string query) {
	LogQuery(query + ";");

	Connection con(*context.db);
	atomic<bool> is_active(true);
	atomic<bool> timed_out(false);
	std::thread interrupt_thread(sleep_thread, &con, &is_active, &timed_out, timeout);

	auto result = con.Query(query);
	is_active = false;
	interrupt_thread.join();
	if (timed_out) {
		LogMessage("TIMEOUT");
	} else if (result->HasError()) {
		LogMessage("EXECUTION ERROR: " + result->GetError());
	} else {
		LogMessage("EXECUTION SUCCESS!");
	}
}

void FuzzyDuck::TryRemoveFile(const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

void FuzzyDuck::LogMessage(const string &message) {
	if (!verbose_output) {
		return;
	}
	Printer::Print(message);
}

void FuzzyDuck::LogTask(const string &message) {
	if (verbose_output) {
		LogMessage(message + "\n");
	}
	LogToCurrent(message);
}

void FuzzyDuck::LogQuery(const string &message) {
	if (verbose_output) {
		LogMessage(message + "\n");
	}
	LogToCurrent(message);
	LogToComplete(message);
}

void FuzzyDuck::LogToCurrent(const string &message) {
	if (log.empty()) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	TryRemoveFile(log);
	auto file = fs.OpenFile(log, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	file->Write((void *)message.c_str(), message.size());
	file->Sync();
	file->Close();
}
void FuzzyDuck::LogToComplete(const string &message) {
	if (!complete_log_handle) {
		return;
	}
	complete_log_handle->Write((void *)message.c_str(), message.size());
	complete_log_handle->Write((void *)"\n", 1);
	complete_log_handle->Sync();
}

} // namespace duckdb
