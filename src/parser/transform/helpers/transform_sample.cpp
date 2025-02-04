#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static SampleMethod GetSampleMethod(const string &method) {
	auto lmethod = StringUtil::Lower(method);
	if (lmethod == "system") {
		return SampleMethod::SYSTEM_SAMPLE;
	} else if (lmethod == "bernoulli") {
		return SampleMethod::BERNOULLI_SAMPLE;
	} else if (lmethod == "reservoir") {
		return SampleMethod::RESERVOIR_SAMPLE;
	} else {
		throw ParserException("Unrecognized sampling method %s, expected system, bernoulli or reservoir", method);
	}
}

unique_ptr<SampleOptions> Transformer::TransformSampleOptions(optional_ptr<duckdb_libpgquery::PGNode> options) {
	if (!options) {
		return nullptr;
	}
	auto result = make_uniq<SampleOptions>();
	auto &sample_options = PGCast<duckdb_libpgquery::PGSampleOptions>(*options);
	auto &sample_size = *PGPointerCast<duckdb_libpgquery::PGSampleSize>(sample_options.sample_size);
	auto sample_expression = TransformExpression(sample_size.sample_size);
	if (sample_expression->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		throw ParserException(sample_expression->GetQueryLocation(),
		                      "Only constants are supported in sample clause currently");
	}
	auto &const_expr = sample_expression->Cast<ConstantExpression>();
	auto &sample_value = const_expr.value;
	result->is_percentage = sample_size.is_percentage;
	if (sample_size.is_percentage) {
		// sample size is given in sample_size: use system sampling
		auto percentage = sample_value.GetValue<double>();
		if (percentage < 0 || percentage > 100) {
			throw ParserException("Sample sample_size %llf out of range, must be between 0 and 100", percentage);
		}
		result->sample_size = Value::DOUBLE(percentage);
		result->method = SampleMethod::SYSTEM_SAMPLE;
	} else {
		// sample size is given in rows: use reservoir sampling
		auto rows = sample_value.GetValue<int64_t>();
		if (rows < 0) {
			throw ParserException("Sample rows %lld out of range, must be bigger than or equal to 0", rows);
		}
		result->sample_size = Value::BIGINT(rows);
		result->method = SampleMethod::RESERVOIR_SAMPLE;
	}
	if (sample_options.method) {
		result->method = GetSampleMethod(sample_options.method);
	}
	if (sample_options.has_seed && sample_options.seed >= 0) {
		result->seed = static_cast<idx_t>(sample_options.seed);
		result->repeatable = true;
	}
	return result;
}

} // namespace duckdb
