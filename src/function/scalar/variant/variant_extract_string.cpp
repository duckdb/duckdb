#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

class VariantStringAllocator {
public:
	explicit VariantStringAllocator(Allocator &allocator)
	    : heap(allocator), yyjson_allocator({Allocate, Reallocate, Free, this}) {
	}

	yyjson_alc *GetYYAlc() {
		return &yyjson_allocator;
	}

	StringHeap &GetStringHeap() {
		return heap;
	}

	void Reset() {
		heap.GetAllocator().Reset();
	}

private:
	static inline void *Allocate(void *ctx, size_t size) {
		auto alloc = (VariantStringAllocator *)ctx; // NOLINT
		return alloc->heap.GetAllocator().AllocateAligned(size);
	}

	static inline void *Reallocate(void *ctx, void *ptr, size_t old_size, size_t size) {
		auto alloc = (VariantStringAllocator *)ctx; // NOLINT
		return alloc->heap.GetAllocator().ReallocateAligned(data_ptr_cast(ptr), old_size, size);
	}

	static inline void Free(void *ctx, void *ptr) {
		// NOP because ArenaAllocator can't free
	}

private:
	StringHeap heap;
	yyjson_alc yyjson_allocator;
};

struct VariantStringExtractLocalState : public FunctionLocalState {
	explicit VariantStringExtractLocalState(ClientContext &context) : allocator(BufferAllocator::Get(context)) {
	}

	VariantStringAllocator allocator;
};

class VariantStringSerializer {
public:
	explicit VariantStringSerializer(VariantStringExtractLocalState &state) : allocator(state.allocator) {
	}

public:
	void operator()(const optional<VariantNode> &node, VectorWriter<string_t> &string_writer) const;

private:
	static bool SerializesAsString(VariantLogicalType type);
	string_t FormatString(const VariantNode &node, VariantLogicalType type) const;
	string_t FormatRaw(const VariantNode &node, VariantLogicalType type) const;
	yyjson_mut_val *SerializePrimitive(const VariantNode &node, VariantLogicalType type,
	                                   yyjson_mut_doc *document) const;
	yyjson_mut_val *SerializeVariant(const VariantNode &node, yyjson_mut_doc *document) const;
	void WriteJSON(const yyjson_mut_val *value, VectorWriter<string_t> &string_writer) const;

	string_t AddString(const string &value) const {
		return allocator.GetStringHeap().AddString(value);
	}

	static yyjson_mut_val *CreateString(yyjson_mut_doc *document, const string_t &value) {
		if (value.IsInlined()) {
			return yyjson_mut_strncpy(document, value.GetData(), value.GetSize());
		}
		return yyjson_mut_strn(document, value.GetData(), value.GetSize());
	}

	static yyjson_mut_val *CreateRaw(yyjson_mut_doc *document, const string_t &value) {
		if (value.IsInlined()) {
			return yyjson_mut_rawncpy(document, value.GetData(), value.GetSize());
		}
		return yyjson_mut_rawn(document, value.GetData(), value.GetSize());
	}

private:
	VariantStringAllocator &allocator;
};

bool VariantStringSerializer::SerializesAsString(const VariantLogicalType type) {
	switch (type) {
	case VariantLogicalType::VARCHAR:
	case VariantLogicalType::BLOB:
	case VariantLogicalType::UUID:
	case VariantLogicalType::DATE:
	case VariantLogicalType::TIME_MICROS:
	case VariantLogicalType::TIME_NANOS:
	case VariantLogicalType::TIME_MICROS_TZ:
	case VariantLogicalType::TIMESTAMP_SEC:
	case VariantLogicalType::TIMESTAMP_MILIS:
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_NANOS:
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
	case VariantLogicalType::INTERVAL:
	case VariantLogicalType::BITSTRING:
	case VariantLogicalType::GEOMETRY:
		return true;
	default:
		return false;
	}
}

string_t VariantStringSerializer::FormatString(const VariantNode &node, const VariantLogicalType type) const {
	auto &heap = allocator.GetStringHeap();

	switch (type) {
	case VariantLogicalType::VARCHAR:
		return node.GetString();
	case VariantLogicalType::BLOB: {
		const auto value = node.GetString();
		return AddString(Value::BLOB(const_data_ptr_cast(value.GetData()), value.GetSize()).ToString());
	}
	case VariantLogicalType::UUID:
		return AddString(Value::UUID(node.GetData<hugeint_t>()).ToString());
	case VariantLogicalType::DATE:
		return StringCast::Operation(node.GetData<date_t>(), heap);
	case VariantLogicalType::TIME_MICROS:
		return StringCast::Operation(node.GetData<dtime_t>(), heap);
	case VariantLogicalType::TIME_NANOS:
		return AddString(Value::TIME_NS(node.GetData<dtime_ns_t>()).ToString());
	case VariantLogicalType::TIME_MICROS_TZ:
		return AddString(Value::TIMETZ(node.GetData<dtime_tz_t>()).ToString());
	case VariantLogicalType::TIMESTAMP_SEC:
		return AddString(Value::TIMESTAMPSEC(node.GetData<timestamp_sec_t>()).ToString());
	case VariantLogicalType::TIMESTAMP_MILIS:
		return AddString(Value::TIMESTAMPMS(node.GetData<timestamp_ms_t>()).ToString());
	case VariantLogicalType::TIMESTAMP_MICROS:
		return StringCast::Operation(node.GetData<timestamp_t>(), heap);
	case VariantLogicalType::TIMESTAMP_NANOS:
		return AddString(Value::TIMESTAMPNS(node.GetData<timestamp_ns_t>()).ToString());
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return AddString(Value::TIMESTAMPTZ(node.GetData<timestamp_tz_t>()).ToString());
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		return AddString(Value::TIMESTAMPTZNS(node.GetData<timestamp_tz_ns_t>()).ToString());
	case VariantLogicalType::INTERVAL:
		return StringCast::Operation(node.GetData<interval_t>(), heap);
	case VariantLogicalType::BITSTRING: {
		const auto value = node.GetString();
		return AddString(Value::BIT(const_data_ptr_cast(value.GetData()), value.GetSize()).ToString());
	}
	case VariantLogicalType::GEOMETRY: {
		const auto value = node.GetString();
		return AddString(Value::GEOMETRY(const_data_ptr_cast(value.GetData()), value.GetSize()).ToString());
	}
	default:
		throw InternalException("Cannot format VARIANT type %s as a string", EnumUtil::ToString(type));
	}
}

string_t VariantStringSerializer::FormatRaw(const VariantNode &node, const VariantLogicalType type) const {
	auto &heap = allocator.GetStringHeap();

	switch (type) {
	case VariantLogicalType::BOOL_TRUE:
		return heap.AddString("true", 4);
	case VariantLogicalType::BOOL_FALSE:
		return heap.AddString("false", 5);
	case VariantLogicalType::INT8:
		return StringCast::Operation(node.GetData<int8_t>(), heap);
	case VariantLogicalType::INT16:
		return StringCast::Operation(node.GetData<int16_t>(), heap);
	case VariantLogicalType::INT32:
		return StringCast::Operation(node.GetData<int32_t>(), heap);
	case VariantLogicalType::INT64:
		return StringCast::Operation(node.GetData<int64_t>(), heap);
	case VariantLogicalType::INT128:
		return StringCast::Operation(node.GetData<hugeint_t>(), heap);
	case VariantLogicalType::UINT8:
		return StringCast::Operation(node.GetData<uint8_t>(), heap);
	case VariantLogicalType::UINT16:
		return StringCast::Operation(node.GetData<uint16_t>(), heap);
	case VariantLogicalType::UINT32:
		return StringCast::Operation(node.GetData<uint32_t>(), heap);
	case VariantLogicalType::UINT64:
		return StringCast::Operation(node.GetData<uint64_t>(), heap);
	case VariantLogicalType::UINT128:
		return StringCast::Operation(node.GetData<uhugeint_t>(), heap);
	case VariantLogicalType::DECIMAL: {
		const auto decimal = node.GetDecimal();
		switch (decimal.GetPhysicalType()) {
		case PhysicalType::INT16:
			return AddString(Decimal::ToString(node.GetData<int16_t>(), decimal.width, decimal.scale));
		case PhysicalType::INT32:
			return AddString(Decimal::ToString(node.GetData<int32_t>(), decimal.width, decimal.scale));
		case PhysicalType::INT64:
			return AddString(Decimal::ToString(node.GetData<int64_t>(), decimal.width, decimal.scale));
		case PhysicalType::INT128:
			return AddString(Decimal::ToString(node.GetData<hugeint_t>(), decimal.width, decimal.scale));
		default:
			throw InternalException("Unsupported VARIANT decimal physical type");
		}
	}
	case VariantLogicalType::BIGNUM: {
		const auto value = node.GetString();
		return AddString(Value::BIGNUM(const_data_ptr_cast(value.GetData()), value.GetSize()).ToString());
	}
	default:
		throw InternalException("Cannot format VARIANT type %s as raw JSON", EnumUtil::ToString(type));
	}
}

yyjson_mut_val *VariantStringSerializer::SerializePrimitive(const VariantNode &node, const VariantLogicalType type,
                                                            yyjson_mut_doc *document) const {
	if (SerializesAsString(type)) {
		return CreateString(document, FormatString(node, type));
	}

	switch (type) {
	case VariantLogicalType::VARIANT_NULL:
		return yyjson_mut_null(document);
	case VariantLogicalType::BOOL_TRUE:
		return yyjson_mut_true(document);
	case VariantLogicalType::BOOL_FALSE:
		return yyjson_mut_false(document);
	case VariantLogicalType::INT8:
		return yyjson_mut_sint(document, node.GetData<int8_t>());
	case VariantLogicalType::INT16:
		return yyjson_mut_sint(document, node.GetData<int16_t>());
	case VariantLogicalType::INT32:
		return yyjson_mut_sint(document, node.GetData<int32_t>());
	case VariantLogicalType::INT64:
		return yyjson_mut_sint(document, node.GetData<int64_t>());
	case VariantLogicalType::INT128:
		return CreateRaw(document, FormatRaw(node, type));
	case VariantLogicalType::UINT8:
		return yyjson_mut_uint(document, node.GetData<uint8_t>());
	case VariantLogicalType::UINT16:
		return yyjson_mut_uint(document, node.GetData<uint16_t>());
	case VariantLogicalType::UINT32:
		return yyjson_mut_uint(document, node.GetData<uint32_t>());
	case VariantLogicalType::UINT64:
		return yyjson_mut_uint(document, node.GetData<uint64_t>());
	case VariantLogicalType::UINT128:
	case VariantLogicalType::DECIMAL:
	case VariantLogicalType::BIGNUM:
		return CreateRaw(document, FormatRaw(node, type));
	case VariantLogicalType::FLOAT:
		return yyjson_mut_real(document, node.GetData<float>());
	case VariantLogicalType::DOUBLE:
		return yyjson_mut_real(document, node.GetData<double>());
	default:
		throw NotImplementedException("Cannot stringify VARIANT type %s", EnumUtil::ToString(type));
	}
}

yyjson_mut_val *VariantStringSerializer::SerializeVariant(const VariantNode &node, yyjson_mut_doc *document) const {
	const auto type = node.GetTypeId();
	switch (type) {
	case VariantLogicalType::ARRAY: {
		const auto result = yyjson_mut_arr(document);
		for (const auto &child : node.GetArrayChildren()) {
			if (!yyjson_mut_arr_append(result, SerializeVariant(child, document))) {
				throw InternalException("Failed to append VARIANT array child to yyjson value");
			}
		}
		return result;
	}
	case VariantLogicalType::OBJECT: {
		const auto result = yyjson_mut_obj(document);
		for (const auto &[key, value] : node.GetObjectChildren(VariantIterationOrder::LEXICOGRAPHIC)) {
			const auto key_value = CreateString(document, key);
			if (!yyjson_mut_obj_add(result, key_value, SerializeVariant(value, document))) {
				throw InternalException("Failed to append VARIANT object child to yyjson value");
			}
		}
		return result;
	}
	default:
		return SerializePrimitive(node, type, document);
	}
}

void VariantStringSerializer::WriteJSON(const yyjson_mut_val *value, VectorWriter<string_t> &string_writer) const {
	yyjson_write_err error;
	size_t size;
	const auto data =
	    yyjson_mut_val_write_opts(value, YYJSON_WRITE_ALLOW_INF_AND_NAN, allocator.GetYYAlc(), &size, &error);
	if (!data) {
		throw SerializationException("Failed to serialize VARIANT as JSON: %s", error.msg);
	}
	string_writer.WriteValue(string_t(data, NumericCast<uint32_t>(size)));
}

void VariantStringSerializer::operator()(const optional<VariantNode> &node,
                                         VectorWriter<string_t> &string_writer) const {
	allocator.Reset();

	if (!node) {
		string_writer.WriteNull();
		return;
	}
	const auto type = node->GetTypeId();
	if (type == VariantLogicalType::VARIANT_NULL) {
		string_writer.WriteNull();
		return;
	}

	// shortcut for root primitive types
	if (SerializesAsString(type)) {
		string_writer.WriteValue(FormatString(*node, type));
		return;
	}
	if (type != VariantLogicalType::ARRAY && type != VariantLogicalType::OBJECT && type != VariantLogicalType::FLOAT &&
	    type != VariantLogicalType::DOUBLE) {
		string_writer.WriteValue(FormatRaw(*node, type));
		return;
	}

	const auto document = yyjson_mut_doc_new(allocator.GetYYAlc());
	WriteJSON(SerializeVariant(*node, document), string_writer);
}

static unique_ptr<FunctionLocalState> VariantExtractStringInit(ExpressionState &state, const BoundFunctionExpression &,
                                                               FunctionData *) {
	return make_uniq<VariantStringExtractLocalState>(state.GetContext());
}

static void VariantExtractStringFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<VariantStringExtractLocalState>();
	VariantPathFunction::Execute<string_t>(input, state, result, VariantStringSerializer(local_state));
}

ScalarFunctionSet VariantExtractStringFun::GetFunctions() {
	return VariantPathFunction::CreateFunctionSet("variant_extract_string", VariantExtractStringFunction,
	                                              LogicalType::VARCHAR, false, VariantExtractStringInit);
}

} // namespace duckdb
