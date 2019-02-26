//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/persistent/physical_copy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "parser/parsed_data.hpp"

namespace duckdb {

//! Physically copy file into a table
class PhysicalCopy : public PhysicalOperator {
public:
	PhysicalCopy(LogicalOperator &op, TableCatalogEntry *table, unique_ptr<CopyInformation> info)
	    : PhysicalOperator(PhysicalOperatorType::COPY, op.types), table(table), info(move(info)) {
	}

	PhysicalCopy(LogicalOperator &op, unique_ptr<CopyInformation> info)
	    : PhysicalOperator(PhysicalOperatorType::COPY, op.types), table(nullptr), info(move(info)) {
	}

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	void AcceptExpressions(SQLNodeVisitor *v) override{};

	TableCatalogEntry *table;
	unique_ptr<CopyInformation> info;

private:
	void Flush(ClientContext &context, DataChunk &chunk, int64_t &nr_elements, int64_t &total,
	           vector<bool> &set_to_default);
};
} // namespace duckdb
