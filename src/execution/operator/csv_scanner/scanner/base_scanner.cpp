#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"

namespace duckdb {
BaseScanner::BaseScanner(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
                         ScannerBoundary boundary_p)
    : boundary(boundary_p), buffer_manager(buffer_manager_p), state_machine(state_machine_p) {
	D_ASSERT(buffer_manager);
	D_ASSERT(state_machine);
};

bool BaseScanner::Finished(){

}

void BaseScanner::Reset(){
	pos.buffer_id = boundary.buffer_idx;
	pos.pos = boundary.buffer_pos;
}

void BaseScanner::ParseChunk(){
	throw InternalException("ParseChunk() from CSV Base Scanner is mot implemented");
}


void BaseScanner::Initialize(){
	throw InternalException("Initialize() from CSV Base Scanner is mot implemented");
}

void BaseScanner::Process(){
	throw InternalException("Process() from CSV Base Scanner is mot implemented");
}

void BaseScanner::FinalizeChunkProcess(){
	throw InternalException("FinalizeChunkProcess() from CSV Base Scanner is mot implemented");
}

void BaseScanner::ParseChunkInternal(){
	if (!initialized){
		Initialize();
	}
	Process();
	FinalizeChunkProcess();
}


} // namespace duckdb
