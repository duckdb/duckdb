#include "execution/operator/persistent/physical_copy_from_file.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/file_system.hpp"
#include "common/gzip_stream.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

#include <algorithm>
#include <fstream>

using namespace duckdb;
using namespace std;

void PhysicalCopyFromFile::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto &state = (PhysicalCopyFromFileOperatorState &)*state_;
	auto &info = *this->info;

	if (!state.csv_stream) {
		// initialize CSV reader
		// open the file
		assert(info.is_from);
		if (!context.db.file_system->FileExists(info.file_path)) {
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
