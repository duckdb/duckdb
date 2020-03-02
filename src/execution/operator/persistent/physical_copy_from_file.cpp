#include "duckdb/execution/operator/persistent/physical_copy_from_file.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/gzip_stream.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <fstream>

using namespace duckdb;
using namespace std;

class PhysicalCopyFromFileOperatorState : public PhysicalOperatorState {
public:
	PhysicalCopyFromFileOperatorState();
	~PhysicalCopyFromFileOperatorState();

	//! The istream to read from
	unique_ptr<std::istream> csv_stream;
	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
};

void PhysicalCopyFromFile::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto &state = (PhysicalCopyFromFileOperatorState &)*state_;
	auto &info = *this->info;

	if (!state.csv_stream) {
		// initialize CSV reader
		// open the file
		assert(info.is_from);
		if (!FileSystem::GetFileSystem(context).FileExists(info.file_path)) {
			throw IOException("File \"%s\" not found", info.file_path.c_str());
		}

		// decide based on the extension which stream to use
		if (StringUtil::EndsWith(StringUtil::Lower(info.file_path), ".gz")) {
			state.csv_stream = make_unique<GzipStream>(info.file_path);
		} else {
			auto csv_local = make_unique<ifstream>();
			csv_local->open(info.file_path);
			state.csv_stream = move(csv_local);
		}

		state.csv_reader = make_unique<BufferedCSVReader>(info, sql_types, *state.csv_stream);
	}
	// read from the CSV reader
	state.csv_reader->ParseCSV(chunk);
}

unique_ptr<PhysicalOperatorState> PhysicalCopyFromFile::GetOperatorState() {
	return make_unique<PhysicalCopyFromFileOperatorState>();
}

PhysicalCopyFromFileOperatorState::PhysicalCopyFromFileOperatorState() : PhysicalOperatorState(nullptr) {
}

PhysicalCopyFromFileOperatorState::~PhysicalCopyFromFileOperatorState() {
}
