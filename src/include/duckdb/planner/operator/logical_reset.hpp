//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_reset.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>

#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;
class Deserializer;
class Serializer;
enum class SetScope : uint8_t;

class LogicalReset : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_RESET;

public:
	LogicalReset(std::string name_p, SetScope scope_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_RESET), name(std::move(name_p)), scope(scope_p) {
	}

	std::string name;
	SetScope scope;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}
};

} // namespace duckdb
