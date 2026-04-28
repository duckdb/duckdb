#include "duckdb.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb; // NOLINT

// Identical BOUNDED type and bounded_max as loadable_extension_demo —
// intentionally so, to test that aliasing correctly tracks which extension
// is the current owner of a shared function name.

struct BoundedType {
	static LogicalType Bind(BindLogicalTypeInput &input) {
		auto &modifiers = input.modifiers;
		if (modifiers.size() != 1) {
			throw BinderException("BOUNDED type must have one modifier");
		}
		if (modifiers[0].GetValue().type() != LogicalType::INTEGER) {
			throw BinderException("BOUNDED type modifier must be integer");
		}
		if (modifiers[0].GetValue().IsNull()) {
			throw BinderException("BOUNDED type modifier cannot be NULL");
		}
		return Get(modifiers[0].GetValue().GetValue<int32_t>());
	}

	static LogicalType Get(int32_t max_val) {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("BOUNDED");
		auto info = make_uniq<ExtensionTypeInfo>();
		info->modifiers.emplace_back(Value::INTEGER(max_val));
		type.SetExtensionInfo(std::move(info));
		return type;
	}

	static LogicalType GetDefault() {
		auto type = LogicalType(LogicalTypeId::INTEGER);
		type.SetAlias("BOUNDED");
		return type;
	}

	static int32_t GetMaxValue(const LogicalType &type) {
		if (!type.HasExtensionInfo()) {
			throw InvalidInputException("BOUNDED type must have a max value");
		}
		return type.GetExtensionInfo()->modifiers[0].value.GetValue<int32_t>();
	}
};

static void BoundedMaxFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value::INTEGER(BoundedType::GetMaxValue(args.data[0].GetType())), count_t(args.size()));
}

static unique_ptr<FunctionData> BoundedMaxBind(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (arguments[0]->return_type == BoundedType::GetDefault()) {
		bound_function.GetArguments()[0] = arguments[0]->return_type;
	} else {
		throw BinderException("bounded_max expects a BOUNDED type");
	}
	return nullptr;
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(loadable_extension_demo_copy, loader) {
	auto bounded_type = BoundedType::GetDefault();
	loader.RegisterType("BOUNDED", bounded_type, BoundedType::Bind);
	loader.RegisterFunction(
	    ScalarFunction("bounded_max", {bounded_type}, LogicalType::INTEGER, BoundedMaxFunc, BoundedMaxBind));
}
}
