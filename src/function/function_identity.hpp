//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/function/function_identity.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

//! Transfers authenticated SQL identity across narrowly audited DuckDB-owned callbacks
class FunctionIdentityPreservation {
public:
	static void PreserveStatistics(ScalarFunction &function) {
		auto preserve_deserialization = function.DeserializationPreservesFunctionIdentity();
		function.RefreshFunctionIdentitySnapshot();
		function.statistics_preserves_function_identity = true;
		function.deserialization_preserves_function_identity = preserve_deserialization;
	}
	static void PreserveStatistics(AggregateFunction &function) {
		auto preserve_deserialization = function.DeserializationPreservesFunctionIdentity();
		function.RefreshFunctionIdentitySnapshot();
		function.statistics_preserves_function_identity = true;
		function.deserialization_preserves_function_identity = preserve_deserialization;
	}
	static void PreserveDeserialization(ScalarFunction &function) {
		auto preserve_statistics = function.StatisticsPreservesFunctionIdentity();
		function.RefreshFunctionIdentitySnapshot();
		function.statistics_preserves_function_identity = preserve_statistics;
		function.deserialization_preserves_function_identity = true;
	}
	static void PreserveDeserialization(AggregateFunction &function) {
		auto preserve_statistics = function.StatisticsPreservesFunctionIdentity();
		function.RefreshFunctionIdentitySnapshot();
		function.statistics_preserves_function_identity = preserve_statistics;
		function.deserialization_preserves_function_identity = true;
	}
};

} // namespace duckdb
