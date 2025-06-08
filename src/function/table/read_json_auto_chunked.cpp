#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include <fstream>
#include <nlohmann/json.hpp>

using namespace duckdb;
using json = nlohmann::json;

struct ChunkedJSONReaderState : public TableFunctionState {
	std::ifstream file;
	idx_t lines_read = 0;
	idx_t chunk_size = 100;
	vector<json> buffered_lines;
	idx_t current_index = 0;
};

static unique_ptr<FunctionData> BindChunkedJSON(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("json_line");
	return_types.push_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<TableFunctionState> InitChunkedJSON(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<ChunkedJSONReaderState>();
	string path = input.inputs[0].GetValue<string>();

	state->file.open(path);
	if (!state->file.is_open()) {
		throw IOException("Could not open file: " + path);
	}

	return std::move(state);
}

static void ChunkedJSONFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = (ChunkedJSONReaderState &)*input.state;

	idx_t count = 0;
	std::string line;
	while (count < STANDARD_VECTOR_SIZE && std::getline(state.file, line)) {
		if (line.empty()) continue;
		try {
			json parsed = json::parse(line);
			output.SetValue(0, count, Value(parsed.dump()));
			count++;
		} catch (...) {
			continue; 
		}
	}

	output.SetCardinality(count);
}

void LoadInternalChunkedReaderFunction(BuiltinFunctions &set) {
	TableFunction func("read_json_auto_chunked", {LogicalType::VARCHAR}, ChunkedJSONFunc, BindChunkedJSON,
	                   InitChunkedJSON);
	set.AddFunction(func);
}
