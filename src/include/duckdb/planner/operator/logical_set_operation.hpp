//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_set_operation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/table_index.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

class LogicalSetOperation : public LogicalOperator {
	LogicalSetOperation(TableIndex table_index, idx_t column_count, LogicalOperatorType type, bool setop_all,
	                    bool allow_out_of_order);

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	LogicalSetOperation(TableIndex table_index, idx_t column_count, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom, LogicalOperatorType type, bool setop_all,
	                    bool allow_out_of_order = true);
	LogicalSetOperation(TableIndex table_index, idx_t column_count, vector<unique_ptr<LogicalOperator>> children,
	                    LogicalOperatorType type, bool setop_all, bool allow_out_of_order = true);

	TableIndex table_index;
	idx_t column_count;
	bool setop_all;
	//! Whether or not UNION statements can be executed out of order
	bool allow_out_of_order;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, column_count);
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
