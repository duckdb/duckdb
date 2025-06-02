//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/log_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalOperatorLogger {
public:
	PhysicalOperatorLogger(ClientContext &context_p, const PhysicalOperator &physical_operator);

public:
	void Log(const string &info) const;
	template <typename... ARGS>
	void Log(const string fmt_str, ARGS... params) const {
		Log(StringUtil::Format(fmt_str, params...));
	}

private:
	ClientContext &context;
	const PhysicalOperatorType operator_type;
	const InsertionOrderPreservingMap<string> parameters;
};

} // namespace duckdb
