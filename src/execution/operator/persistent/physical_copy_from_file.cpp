#include "duckdb/execution/operator/persistent/physical_copy_from_file.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

class PhysicalCopyFromFileOperatorState : public PhysicalOperatorState {
public:
	PhysicalCopyFromFileOperatorState();
	~PhysicalCopyFromFileOperatorState();

	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
};

void PhysicalCopyFromFile::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                            PhysicalOperatorState *state_) {
	auto &state = (PhysicalCopyFromFileOperatorState &)*state_;
	auto &info = *this->info;

	if (!state.csv_reader) {
		// initialize CSV reader
		state.csv_reader = make_unique<BufferedCSVReader>(context.client, info, sql_types);
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

}
