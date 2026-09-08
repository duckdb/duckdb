//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_window_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreateWindowFunctionInfo : public CreateFunctionInfo {
	explicit CreateWindowFunctionInfo(WindowFunction function);
	explicit CreateWindowFunctionInfo(WindowFunctionSet set);

	WindowFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb
