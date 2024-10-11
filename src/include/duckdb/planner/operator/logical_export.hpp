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
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXPORT;

public:
	LogicalExport(CopyFunction function, unique_ptr<CopyInfo> copy_info, BoundExportData exported_tables)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPORT), function(std::move(function)),
	      copy_info(std::move(copy_info)), exported_tables(std::move(exported_tables)) {
	}
	CopyFunction function;
	unique_ptr<CopyInfo> copy_info;
	BoundExportData exported_tables;

	void Serialize(Serializer &serializer) const override {
		throw NotImplementedException("FIXME: Serialize LogicalExport statement");
	}
	unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer) {
		throw NotImplementedException("FIXME: Deserialize LogicalExport statement");
	}

public:
protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}
};

} // namespace duckdb
