//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_export.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/exported_table_data.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class LogicalExport : public LogicalOperator {
public:
	LogicalExport(CopyFunction function, unique_ptr<CopyInfo> copy_info, BoundExportData exported_tables)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPORT), function(function), copy_info(std::move(copy_info)),
	      exported_tables(std::move(exported_tables)) {
	}
	CopyFunction function;
	unique_ptr<CopyInfo> copy_info;
	BoundExportData exported_tables;

public:
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}
};

} // namespace duckdb
