//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/util/csv_casting.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/options/csv_reader_options.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"

namespace duckdb {
class CSVCast {
	template <class OP, class T>
	static bool TemplatedTryCastFloatingVector(const CSVReaderOptions &options, Vector &input_vector,
	                                           Vector &result_vector, idx_t count, string &error_message,
	                                           idx_t &line_error) {
		D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
		bool all_converted = true;
		idx_t row = 0;
		UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
			T result;
			if (!OP::Operation(input, result, &error_message)) {
				line_error = row;
				all_converted = false;
			} else {
				row++;
			}
			return result;
		});
		return all_converted;
	}

	template <class OP, class T>
	static bool TemplatedTryCastDecimalVector(const CSVReaderOptions &options, Vector &input_vector,
	                                          Vector &result_vector, idx_t count, string &error_message, uint8_t width,
	                                          uint8_t scale) {
		D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
		bool all_converted = true;
		UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
			T result;
			if (!OP::Operation(input, result, &error_message, width, scale)) {
				all_converted = false;
			}
			return result;
		});
		return all_converted;
	}

	struct TryCastDateOperator {
		static bool Operation(const map<LogicalTypeId, CSVOption<StrpTimeFormat>> &options, string_t input,
		                      date_t &result, string &error_message) {
			return options.at(LogicalTypeId::DATE).GetValue().TryParseDate(input, result, error_message);
		}
	};

	struct TryCastTimestampOperator {
		static bool Operation(const map<LogicalTypeId, CSVOption<StrpTimeFormat>> &options, string_t input,
		                      timestamp_t &result, string &error_message) {
			return options.at(LogicalTypeId::TIMESTAMP).GetValue().TryParseTimestamp(input, result, error_message);
		}
	};

	template <class OP, class T>
	static bool TemplatedTryCastDateVector(const map<LogicalTypeId, CSVOption<StrpTimeFormat>> &options,
	                                       Vector &input_vector, Vector &result_vector, idx_t count,
	                                       string &error_message, idx_t &line_error) {
		D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
		bool all_converted = true;
		idx_t cur_line = 0;
		UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
			T result;
			if (!OP::Operation(options, input, result, error_message)) {
				line_error = cur_line;
				all_converted = false;
			}
			cur_line++;
			return result;
		});
		return all_converted;
	}

public:
	static bool TryCastDateVector(const map<LogicalTypeId, CSVOption<StrpTimeFormat>> &options, Vector &input_vector,
	                              Vector &result_vector, idx_t count, string &error_message, idx_t &line_error) {
		return TemplatedTryCastDateVector<TryCastDateOperator, date_t>(options, input_vector, result_vector, count,
		                                                               error_message, line_error);
	}
	static bool TryCastTimestampVector(const map<LogicalTypeId, CSVOption<StrpTimeFormat>> &options,
	                                   Vector &input_vector, Vector &result_vector, idx_t count,
	                                   string &error_message) {
		idx_t line_error;
		return TemplatedTryCastDateVector<TryCastTimestampOperator, timestamp_t>(options, input_vector, result_vector,
		                                                                         count, error_message, line_error);
	}
	static bool TryCastFloatingVectorCommaSeparated(const CSVReaderOptions &options, Vector &input_vector,
	                                                Vector &result_vector, idx_t count, string &error_message,
	                                                const LogicalType &result_type, idx_t &line_error) {
		switch (result_type.InternalType()) {
		case PhysicalType::DOUBLE:
			return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, double>(
			    options, input_vector, result_vector, count, error_message, line_error);
		case PhysicalType::FLOAT:
			return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, float>(
			    options, input_vector, result_vector, count, error_message, line_error);
		default:
			throw InternalException("Unimplemented physical type for floating");
		}
	}
	static bool TryCastDecimalVectorCommaSeparated(const CSVReaderOptions &options, Vector &input_vector,
	                                               Vector &result_vector, idx_t count, string &error_message,
	                                               const LogicalType &result_type) {
		auto width = DecimalType::GetWidth(result_type);
		auto scale = DecimalType::GetScale(result_type);
		switch (result_type.InternalType()) {
		case PhysicalType::INT16:
			return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int16_t>(
			    options, input_vector, result_vector, count, error_message, width, scale);
		case PhysicalType::INT32:
			return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int32_t>(
			    options, input_vector, result_vector, count, error_message, width, scale);
		case PhysicalType::INT64:
			return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int64_t>(
			    options, input_vector, result_vector, count, error_message, width, scale);
		case PhysicalType::INT128:
			return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, hugeint_t>(
			    options, input_vector, result_vector, count, error_message, width, scale);
		default:
			throw InternalException("Unimplemented physical type for decimal");
		}
	}
};
} // namespace duckdb
