#include "fuzzyduck.hpp"
#include "duckdb/common/random_engine.hpp"
#include "statement_generator.hpp"

namespace duckdb {

FuzzyDuck::FuzzyDuck(ClientContext &context) : context(context) {
}

FuzzyDuck::~FuzzyDuck() {
}

void FuzzyDuck::Fuzz() {
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
	for (idx_t i = 0; i < max_queries; i++) {
		LogMessage("Query " + to_string(i) + "\n");
		auto query = GenerateQuery();
		RunQuery(std::move(query));
	}
	if (complete_log_handle) {
		complete_log_handle->Close();
	}
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

void FuzzyDuck::RunQuery(string query) {
	LogQuery(query + ";");

	Connection con(*context.db);
	auto result = con.Query(query);
	if (result->HasError()) {
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
