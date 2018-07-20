
#pragma once

#include "execution/physicaloperator.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

class PhysicalSeqScan : public PhysicalOperator {
 public:
	PhysicalSeqScan(DataTable* table, std::vector<size_t> column_ids) : PhysicalOperator(PhysicalOperatorType::SEQ_SCAN), table(table), column_ids(column_ids) {}

	DataTable* table;
	std::vector<size_t> column_ids;

	virtual void GetChunk(DataChunk& chunk, PhysicalOperatorState* state) override;

	virtual std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalSeqScanOperatorState : public PhysicalOperatorState {
  public:
  	PhysicalSeqScanOperatorState(size_t current_offset) : current_offset(current_offset) { }

  	size_t current_offset;
};

}
