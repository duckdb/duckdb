#include "duckdb/execution/in_value_set.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

namespace {

template <class T>
class TemplatedInValueSet : public InValueSet {
	//! Integer values spanning at most this many slots (or 32 per value) are stored in a bitmap
	static constexpr uint64_t MAX_BITMAP_RANGE = 1ULL << 16;
	static constexpr bool SUPPORTS_BITMAP = std::is_integral<T>::value && !std::is_same<T, bool>::value;
	//! Hash table slots are kept sparse for up to this many values, so misses mostly hit an empty slot
	static constexpr idx_t MAX_SPARSE_TABLE_VALUES = 1ULL << 16;
	//! Bits per value of the filter that rejects most misses before probing the hash table
	static constexpr idx_t FILTER_BITS_PER_VALUE = 16;
	//! Rows that are hashed in one pass before probing
	static constexpr idx_t PROBE_BATCH_SIZE = 256;

protected:
	void Add(const Value &value) override {
		if (value.IsNull()) {
			has_null = true;
			return;
		}
		collected.push_back(StoreValue(value.GetValueUnsafe<T>()));
	}

	void Finalize() override {
		if (!TryBuildBitmap()) {
			BuildTable();
		}
		collected.clear();
	}

	void ProbeValues(const Vector &input, idx_t count, bool negate, Vector &result) const override {
		auto input_values = input.Values<T>();
		auto writer = FlatVector::Writer<bool>(result, count);
		if (!bitmap.empty()) {
			for (idx_t i = 0; i < count; i++) {
				auto entry = input_values[i];
				WriteResult(writer, entry.IsValid(), entry.IsValid() && BitmapContains(entry.GetValueUnsafe()), negate);
			}
			return;
		}
		hash_t hashes[PROBE_BATCH_SIZE];
		for (idx_t base = 0; base < count; base += PROBE_BATCH_SIZE) {
			auto batch_size = MinValue<idx_t>(count - base, PROBE_BATCH_SIZE);
			for (idx_t i = 0; i < batch_size; i++) {
				auto entry = input_values[base + i];
				hashes[i] = entry.IsValid() ? Hash<T>(entry.GetValueUnsafe()) : 0;
			}
			for (idx_t i = 0; i < batch_size; i++) {
				auto entry = input_values[base + i];
				auto found =
				    entry.IsValid() && FilterContains(hashes[i]) && TableContains(entry.GetValueUnsafe(), hashes[i]);
				WriteResult(writer, entry.IsValid(), found, negate);
			}
		}
	}

private:
	T StoreValue(const T &value) {
		return value;
	}

	void WriteResult(VectorWriter<bool> &writer, bool is_valid, bool found, bool negate) const {
		if (!is_valid || (!found && has_null)) {
			writer.WriteNull();
		} else {
			writer.WriteValue(found != negate);
		}
	}

	bool TryBuildBitmap() {
		if constexpr (SUPPORTS_BITMAP) {
			if (collected.empty()) {
				return false;
			}
			T min_value = collected[0];
			T max_value = collected[0];
			for (const T value : collected) {
				min_value = MinValue(min_value, value);
				max_value = MaxValue(max_value, value);
			}
			// two's complement subtraction yields the exact range, also for signed types
			auto range = static_cast<uint64_t>(max_value) - static_cast<uint64_t>(min_value);
			if (range >= MaxValue<uint64_t>(MAX_BITMAP_RANGE, collected.size() * 32)) {
				return false;
			}
			bitmap_min = static_cast<uint64_t>(min_value);
			bitmap_range = range;
			bitmap.resize(range / 64 + 1, 0);
			for (const T value : collected) {
				auto offset = static_cast<uint64_t>(value) - bitmap_min;
				bitmap[offset / 64] |= 1ULL << (offset % 64);
			}
			return true;
		} else {
			return false;
		}
	}

	bool BitmapContains(const T &value) const {
		if constexpr (SUPPORTS_BITMAP) {
			auto offset = static_cast<uint64_t>(value) - bitmap_min;
			return offset <= bitmap_range && (bitmap[offset / 64] >> (offset % 64)) & 1;
		} else {
			return false;
		}
	}

	//! The filter uses the upper hash bits, the hash table the lower ones
	idx_t FilterBit(hash_t hash) const {
		return (hash >> 32) & filter_mask;
	}

	bool FilterContains(hash_t hash) const {
		auto bit = FilterBit(hash);
		return (filter[bit / 64] >> (bit % 64)) & 1;
	}

	//! Slot tags are the hash with the lowest bit set, a zero tag marks an empty slot
	static hash_t SlotTag(hash_t hash) {
		return hash | 1;
	}

	void BuildTable() {
		auto load_factor = collected.size() <= MAX_SPARSE_TABLE_VALUES ? 4 : 2;
		auto capacity = NextPowerOfTwo(MaxValue<idx_t>(collected.size() * load_factor, 16));
		slot_tags.resize(capacity, 0);
		slot_values.resize(capacity);
		mask = capacity - 1;
		auto filter_size = NextPowerOfTwo(MaxValue<idx_t>(collected.size() * FILTER_BITS_PER_VALUE, 64));
		filter.resize(filter_size / 64, 0);
		filter_mask = filter_size - 1;
		for (const T value : collected) {
			auto hash = Hash<T>(value);
			if (TableContains(value, hash)) {
				continue;
			}
			auto idx = hash & mask;
			while (slot_tags[idx] != 0) {
				idx = (idx + 1) & mask;
			}
			slot_tags[idx] = SlotTag(hash);
			slot_values[idx] = value;
			auto bit = FilterBit(hash);
			filter[bit / 64] |= 1ULL << (bit % 64);
		}
	}

	bool TableContains(const T &value, hash_t hash) const {
		auto tag = SlotTag(hash);
		for (auto idx = hash & mask; slot_tags[idx] != 0; idx = (idx + 1) & mask) {
			if (slot_tags[idx] == tag && Equals::Operation<T>(slot_values[idx], value)) {
				return true;
			}
		}
		return false;
	}

private:
	vector<T> collected;
	vector<uint64_t> bitmap;
	uint64_t bitmap_min = 0;
	uint64_t bitmap_range = 0;
	vector<uint64_t> filter;
	idx_t filter_mask = 0;
	vector<hash_t> slot_tags;
	vector<T> slot_values;
	idx_t mask = 0;
	StringHeap heap;
};

template <>
string_t TemplatedInValueSet<string_t>::StoreValue(const string_t &value) {
	return heap.AddBlob(value);
}

//! Nested values are looked up by their sort key, which is equal exactly when the values compare equal
class SortKeyInValueSet : public InValueSet {
public:
	explicit SortKeyInValueSet(LogicalType type_p) : type(std::move(type_p)) {
	}

protected:
	void Add(const Value &value) override {
		if (value.IsNull()) {
			has_null = true;
			return;
		}
		collected.push_back(value);
	}

	void Finalize() override {
		Vector constants(type, collected.size());
		FlatVector::SetSize(constants, collected.size());
		for (idx_t i = 0; i < collected.size(); i++) {
			constants.SetValue(i, collected[i]);
		}
		Vector keys(LogicalType::BLOB, collected.size());
		CreateSortKeyHelpers::CreateSortKey(constants, collected.size(), Modifiers(), keys);
		for (auto entry : keys.Values<string_t>()) {
			if (keys_set.find(entry.GetValue()) == keys_set.end()) {
				keys_set.insert(heap.AddBlob(entry.GetValue()));
			}
		}
		collected.clear();
	}

	void ProbeValues(const Vector &input, idx_t count, bool negate, Vector &result) const override {
		Vector keys(LogicalType::BLOB, count);
		CreateSortKeyHelpers::CreateSortKeyWithValidity(input, keys, Modifiers(), count);
		auto writer = FlatVector::Writer<bool>(result, count);
		for (auto entry : keys.Values<string_t>()) {
			if (!entry.IsValid()) {
				writer.WriteNull();
				continue;
			}
			auto found = keys_set.find(entry.GetValue()) != keys_set.end();
			if (!found && has_null) {
				writer.WriteNull();
			} else {
				writer.WriteValue(found != negate);
			}
		}
	}

private:
	static OrderModifiers Modifiers() {
		return OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
	}

private:
	LogicalType type;
	vector<Value> collected;
	string_set_t keys_set;
	StringHeap heap;
};

unique_ptr<InValueSet> CreateTemplatedInValueSet(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return make_uniq<TemplatedInValueSet<bool>>();
	case PhysicalType::INT8:
		return make_uniq<TemplatedInValueSet<int8_t>>();
	case PhysicalType::INT16:
		return make_uniq<TemplatedInValueSet<int16_t>>();
	case PhysicalType::INT32:
		return make_uniq<TemplatedInValueSet<int32_t>>();
	case PhysicalType::INT64:
		return make_uniq<TemplatedInValueSet<int64_t>>();
	case PhysicalType::UINT8:
		return make_uniq<TemplatedInValueSet<uint8_t>>();
	case PhysicalType::UINT16:
		return make_uniq<TemplatedInValueSet<uint16_t>>();
	case PhysicalType::UINT32:
		return make_uniq<TemplatedInValueSet<uint32_t>>();
	case PhysicalType::UINT64:
		return make_uniq<TemplatedInValueSet<uint64_t>>();
	case PhysicalType::INT128:
		return make_uniq<TemplatedInValueSet<hugeint_t>>();
	case PhysicalType::UINT128:
		return make_uniq<TemplatedInValueSet<uhugeint_t>>();
	case PhysicalType::FLOAT:
		return make_uniq<TemplatedInValueSet<float>>();
	case PhysicalType::DOUBLE:
		return make_uniq<TemplatedInValueSet<double>>();
	case PhysicalType::INTERVAL:
		return make_uniq<TemplatedInValueSet<interval_t>>();
	case PhysicalType::VARCHAR:
		return make_uniq<TemplatedInValueSet<string_t>>();
	default:
		return nullptr;
	}
}

bool SupportsType(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::INTERVAL:
	case PhysicalType::VARCHAR:
		return true;
	default:
		return false;
	}
}

//! Nested types whose sort key equality matches regular equality
bool SupportsSortKeyLookup(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
		return SupportsSortKeyLookup(ListType::GetChildType(type));
	case LogicalTypeId::ARRAY:
		return SupportsSortKeyLookup(ArrayType::GetChildType(type));
	case LogicalTypeId::STRUCT:
		for (auto &child : StructType::GetChildTypes(type)) {
			if (!SupportsSortKeyLookup(child.second)) {
				return false;
			}
		}
		return true;
	default:
		break;
	}
	// sort keys do not normalize intervals or apply collations
	if (type.InternalType() == PhysicalType::INTERVAL ||
	    (type.InternalType() == PhysicalType::VARCHAR && !StringType::GetCollation(type).empty())) {
		return false;
	}
	return SupportsType(type);
}

bool IsNestedType(const LogicalType &type) {
	auto id = type.id();
	return id == LogicalTypeId::LIST || id == LogicalTypeId::ARRAY || id == LogicalTypeId::STRUCT;
}

} // namespace

bool InValueSet::IsSupported(const BoundOperatorExpression &expr) {
	auto &children = expr.GetChildren();
	if (children.size() <= MIN_VALUE_COUNT) {
		return false;
	}
	auto &type = children[0]->GetReturnType();
	if (IsNestedType(type) ? !SupportsSortKeyLookup(type) : !SupportsType(type)) {
		return false;
	}
	for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];
		if (child.GetExpressionType() != ExpressionType::VALUE_CONSTANT || child.GetReturnType() != type) {
			return false;
		}
	}
	return true;
}

unique_ptr<InValueSet> InValueSet::Create(const BoundOperatorExpression &expr) {
	D_ASSERT(IsSupported(expr));
	auto &children = expr.GetChildren();
	auto &type = children[0]->GetReturnType();
	unique_ptr<InValueSet> result;
	if (IsNestedType(type)) {
		result = make_uniq<SortKeyInValueSet>(type);
	} else {
		result = CreateTemplatedInValueSet(type);
	}
	for (idx_t child_idx = 1; child_idx < children.size(); child_idx++) {
		result->Add(children[child_idx]->Cast<BoundConstantExpression>().GetValue());
	}
	result->Finalize();
	return result;
}

void InValueSet::Probe(const Vector &input, idx_t count, bool negate, Vector &result) const {
	if (input.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// probe each dictionary entry only once
		auto dictionary_size = DictionaryVector::DictionarySize(input);
		if (dictionary_size.IsValid() && dictionary_size.GetIndex() < count) {
			Vector dictionary_result(LogicalType::BOOLEAN, dictionary_size.GetIndex());
			ProbeValues(DictionaryVector::Child(input), dictionary_size.GetIndex(), negate, dictionary_result);
			result.Slice(dictionary_result, DictionaryVector::SelVector(input), count);
			return;
		}
	}
	ProbeValues(input, count, negate, result);
}

} // namespace duckdb
