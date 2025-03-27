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
	LogicalExport(CopyFunction function, unique_ptr<CopyInfo> copy_info, unique_ptr<BoundExportData> exported_tables);

	unique_ptr<CopyInfo> copy_info;
	CopyFunction function;
	unique_ptr<BoundExportData> exported_tables;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	LogicalExport(ClientContext &context, unique_ptr<ParseInfo> copy_info, unique_ptr<ParseInfo> exported_tables);

	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}

	CopyFunction GetCopyFunction(ClientContext &context, CopyInfo &info);
};

} // namespace duckdb
