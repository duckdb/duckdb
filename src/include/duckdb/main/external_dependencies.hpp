//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/external_dependencies.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

enum class ExternalDependenciesType : uint8_t { PYTHON_DEPENDENCY };

class ExternalDependency {
public:
	explicit ExternalDependency(ExternalDependenciesType type_p) : type(type_p) {};
	virtual ~ExternalDependency() {};
	ExternalDependenciesType type;
};

} // namespace duckdb
