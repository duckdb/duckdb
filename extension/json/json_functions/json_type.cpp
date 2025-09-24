#include "json_executors.hpp"

namespace duckdb {

static inline string_t GetType(yyjson_val *val, yyjson_alc *, Vector &, ValidityMask &mask, idx_t idx) {
	return JSONCommon::ValTypeToStringT(val);
}

static void UnaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::UnaryExecute<string_t>(args, state, result, GetType);
}

static void BinaryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::BinaryExecute<string_t>(args, state, result, GetType);
}

static void ManyTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::ExecuteMany<string_t>(args, state, result, GetType);
}

static void ListJSONContainerTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const idx_t count = args.size();
	UnifiedVectorFormat input;
	args.data[0].ToUnifiedFormat(count, input);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto data = ConstantVector::GetData<string_t>(result);
		auto &validity = ConstantVector::Validity(result);
		
		if (!input.validity.RowIsValid(0)) {
			validity.SetInvalid(0);
		} else {
			*data = StringVector::AddString(result, "ARRAY");
		}
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto data = FlatVector::GetData<string_t>(result);
		auto &validity = FlatVector::Validity(result);

		for (idx_t i = 0; i < count; i++) {
			const auto idx = input.sel->get_index(i);
			if (!input.validity.RowIsValid(idx)) {
				validity.SetInvalid(i);
				continue;
			}
			data[i] = StringVector::AddString(result, "ARRAY");
		}
	}
}

static void GetTypeFunctionsInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type}, LogicalType::VARCHAR, UnaryTypeFunction, nullptr, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::VARCHAR, BinaryTypeFunction,
	                               JSONReadFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
	set.AddFunction(ScalarFunction({input_type, LogicalType::LIST(LogicalType::VARCHAR)},
	                               LogicalType::LIST(LogicalType::VARCHAR), ManyTypeFunction,
	                               JSONReadManyFunctionData::Bind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

static void GetTypeFunctionsForListJSON(ScalarFunctionSet &set) {
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::JSON())}, LogicalType::VARCHAR,
	                               ListJSONContainerTypeFunction, nullptr, nullptr, nullptr,
	                               JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetTypeFunction() {
	ScalarFunctionSet set("json_type");
	GetTypeFunctionsInternal(set, LogicalType::VARCHAR);
	GetTypeFunctionsInternal(set, LogicalType::JSON());
	GetTypeFunctionsForListJSON(set);
	return set;
}

} // namespace duckdb
