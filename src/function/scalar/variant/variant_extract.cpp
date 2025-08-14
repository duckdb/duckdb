#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

enum class PathParsingState : uint8_t { BASE, KEY, INDEX };

struct BindData : public FunctionData {
public:
	explicit BindData(const string &constant_path);

public:
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;

public:
	string constant_path;
	//! NOTE: the keys in here reference data of the 'constant_path',
	//! the components can not be copied without reconstruction
	vector<VariantPathComponent> components;
};

} // namespace

using regexp_util::TryParseConstantPattern;

vector<VariantPathComponent> ParsePath(const string &path) {
	vector<VariantPathComponent> components;
	auto state = PathParsingState::BASE;

	idx_t i = 0;
	while (i < path.size()) {
		switch (state) {
		case PathParsingState::BASE: {
			if (path[i] == '.') {
				// Skip dot, move to key state
				i++;
				state = PathParsingState::KEY;
			} else if (path[i] == '[') {
				// Start of an index
				i++;
				state = PathParsingState::INDEX;
			} else {
				// Start of key at base
				state = PathParsingState::KEY;
			}
			break;
		}
		case PathParsingState::KEY: {
			// Parse key until next '.' or '['
			idx_t start = i;
			while (i < path.size() && path[i] != '.' && path[i] != '[') {
				i++;
			}
			auto key = string_t(path.c_str() + start, i - start);
			VariantPathComponent comp;
			comp.lookup_mode = VariantChildLookupMode::BY_KEY;
			comp.payload.key = std::move(key);
			components.push_back(std::move(comp));
			state = PathParsingState::BASE;
			break;
		}
		case PathParsingState::INDEX: {
			// Parse digits inside [ ]
			idx_t start = i;
			while (i < path.size() && isdigit(path[i])) {
				i++;
			}
			if (i == start || i >= path.size() || path[i] != ']') {
				throw BinderException("Invalid index in path: %s", path);
			}
			uint32_t index = std::stoul(path.substr(start, i - start));
			i++; // skip ']'
			VariantPathComponent comp;
			comp.lookup_mode = VariantChildLookupMode::BY_INDEX;
			comp.payload.index = index;
			components.push_back(std::move(comp));
			state = PathParsingState::BASE;
			break;
		}
		}
	}
	return components;
}

BindData::BindData(const string &constant_path_p) : FunctionData(), constant_path(constant_path_p) {
	components = ParsePath(constant_path);
}

unique_ptr<FunctionData> BindData::Copy() const {
	return make_uniq<BindData>(constant_path);
}
bool BindData::Equals(const FunctionData &other) const {
	auto &bind_data = other.Cast<BindData>();
	return bind_data.constant_path == constant_path;
}

static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
                                     vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("'variant_extract' expects two arguments, VARIANT column and VARCHAR path");
	}
	auto &path = *arguments[1];
	if (path.return_type.id() != LogicalTypeId::VARCHAR) {
		throw BinderException("'variant_extract' expects the second argument to be of type VARCHAR, not %s",
		                      path.return_type.ToString());
	}
	string constant_path;
	if (!TryParseConstantPattern(context, path, constant_path)) {
		throw BinderException("'variant_extract' expects the second argument (path) to be a constant expression");
	}
	return make_uniq<BindData>(constant_path);
}

//! FIXME: it could make sense to allow a third argument: 'default'
//! This can currently be achieved with COALESCE(TRY(<extract method>), 'default')
static void Func(DataChunk &input, ExpressionState &state, Vector &result) {
	auto count = input.size();

	D_ASSERT(input.ColumnCount() == 2);
	auto &variant = input.data[0];
	D_ASSERT(variant.GetType() == LogicalType::VARIANT());

	auto &path = input.data[1];
	D_ASSERT(path.GetType().id() == LogicalTypeId::VARCHAR);
	//! FIXME: do we want to assert that this is a constant, or allow different paths per row?
	D_ASSERT(path.GetVectorType() == VectorType::CONSTANT_VECTOR);

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<BindData>();
	auto &allocator = Allocator::DefaultAllocator();

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant, count, source_format);

	//! Path either contains array indices or object keys
	auto &validity = FlatVector::Validity(result);

	auto owned_value_indices = allocator.Allocate(sizeof(uint32_t) * count * 2);
	auto value_indices = reinterpret_cast<uint32_t *>(owned_value_indices.get());
	auto new_value_indices = &value_indices[count];
	::bzero(value_indices, sizeof(uint32_t) * count * 2);

	auto owned_nested_data = allocator.Allocate(sizeof(VariantNestedData) * count);
	auto nested_data = reinterpret_cast<VariantNestedData *>(owned_nested_data.get());

	string error;
	for (idx_t i = 0; i < info.components.size(); i++) {
		auto &component = info.components[i];
		auto input_indices = i % 2 == 0 ? value_indices : new_value_indices;
		auto output_indices = i % 2 == 0 ? new_value_indices : value_indices;

		auto expected_type = component.lookup_mode == VariantChildLookupMode::BY_INDEX ? VariantLogicalType::ARRAY
		                                                                               : VariantLogicalType::OBJECT;
		if (!VariantUtils::CollectNestedData(source_format, expected_type, input_indices, count, optional_idx(),
		                                     nested_data, error)) {
			throw InvalidInputException(error);
		}

		if (!VariantUtils::FindChildValues(source_format, component, optional_idx(), output_indices, nested_data,
		                                   count)) {
			switch (component.lookup_mode) {
			case VariantChildLookupMode::BY_INDEX: {
				throw InvalidInputException("ARRAY does not contain the index: %d", component.payload.index);
			}
			case VariantChildLookupMode::BY_KEY: {
				throw InvalidInputException("OBJECT does not contain the key: %s", component.payload.key.GetString());
			}
			}
		}
	}

	//! We have these indices left, the simplest way we can finalize this is:
	//! Leave the 'keys' alone, we'll just potentially have unused keys
	//! Leave the 'children' alone, we'll have less children, but the child indices are still correct
	//! We can also leave 'data' alone
	//! We just need to remap index 0 of the 'values' list (for all rows)

	auto result_indices = info.components.size() % 2 == 0 ? value_indices : new_value_indices;

	auto &values = UnifiedVariantVector::GetValues(source_format);
	auto values_data = values.GetData<list_entry_t>(values);
	auto &raw_values = VariantVector::GetValues(variant);
	auto values_list_size = ListVector::GetListSize(raw_values);

	//! Create a new Variant that references the existing data of the input Variant
	result.Initialize(false, count);
	VariantVector::GetKeys(result).Reference(VariantVector::GetKeys(variant));
	VariantVector::GetChildren(result).Reference(VariantVector::GetChildren(variant));
	VariantVector::GetData(result).Reference(VariantVector::GetData(variant));

	//! Copy the existing 'values'
	auto &result_values = VariantVector::GetValues(result);
	result_values.Initialize(false, count);
	ListVector::Reserve(result_values, values_list_size);
	ListVector::SetListSize(result_values, values_list_size);
	auto result_values_data = FlatVector::GetData<list_entry_t>(result_values);
	for (idx_t i = 0; i < count; i++) {
		result_values_data[i] = values_data[values.sel->get_index(i)];
	}

	//! Prepare the selection vector to remap index 0 of each row
	SelectionVector new_sel(0, values_list_size);
	for (idx_t i = 0; i < count; i++) {
		auto &list_entry = values_data[values.sel->get_index(i)];
		new_sel.set_index(list_entry.offset, list_entry.offset + result_indices[i]);
	}

	auto &result_type_id = VariantVector::GetValuesTypeId(result);
	auto &result_byte_offset = VariantVector::GetValuesByteOffset(result);
	result_type_id.Dictionary(VariantVector::GetValuesTypeId(variant), values_list_size, new_sel, values_list_size);
	result_byte_offset.Dictionary(VariantVector::GetValuesByteOffset(variant), values_list_size, new_sel,
	                              values_list_size);

	if (input.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction VariantExtractFun::GetFunction() {
	auto variant_type = LogicalType::VARIANT();
	return ScalarFunction("variant_extract", {variant_type, LogicalType::VARCHAR}, variant_type, Func, Bind);
}

} // namespace duckdb
