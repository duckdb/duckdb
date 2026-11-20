#include "duckdb/function/variant/variant_structure.hpp"

#include "duckdb/common/array.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/storage/arena_allocator.hpp"

#include <algorithm>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Numeric domains
//===--------------------------------------------------------------------===//
namespace {

template <class T>
void Widen(T &current, T value) {
	current = MaxValue(current, value);
}

//! The number of decimal digits of the largest magnitude of a signed integer type
uint32_t SignedDigits(idx_t bits) {
	switch (bits) {
	case 0:
		return 0;
	case 8:
		return 3;
	case 16:
		return 5;
	case 32:
		return 10;
	case 64:
		return 19;
	default:
		return 39;
	}
}

//! The number of decimal digits of the largest magnitude of an unsigned integer type
uint32_t UnsignedDigits(idx_t bits) {
	return bits == 64 ? 20 : SignedDigits(bits);
}

VariantLogicalType SignedType(idx_t bits) {
	switch (bits) {
	case 8:
		return VariantLogicalType::INT8;
	case 16:
		return VariantLogicalType::INT16;
	case 32:
		return VariantLogicalType::INT32;
	case 64:
		return VariantLogicalType::INT64;
	default:
		D_ASSERT(bits == 128);
		return VariantLogicalType::INT128;
	}
}

VariantLogicalType UnsignedType(idx_t bits) {
	switch (bits) {
	case 8:
		return VariantLogicalType::UINT8;
	case 16:
		return VariantLogicalType::UINT16;
	case 32:
		return VariantLogicalType::UINT32;
	case 64:
		return VariantLogicalType::UINT64;
	default:
		D_ASSERT(bits == 128);
		return VariantLogicalType::UINT128;
	}
}

} // namespace

bool VariantNumericDomain::TryAdd(VariantLogicalType type, const VariantNode &value) {
	switch (type) {
	case VariantLogicalType::INT8:
		Widen<uint8_t>(signed_bits, 8);
		return true;
	case VariantLogicalType::INT16:
		Widen<uint8_t>(signed_bits, 16);
		return true;
	case VariantLogicalType::INT32:
		Widen<uint8_t>(signed_bits, 32);
		return true;
	case VariantLogicalType::INT64:
		Widen<uint8_t>(signed_bits, 64);
		return true;
	case VariantLogicalType::INT128:
		Widen<uint8_t>(signed_bits, 128);
		return true;
	case VariantLogicalType::UINT8:
		Widen<uint8_t>(unsigned_bits, 8);
		return true;
	case VariantLogicalType::UINT16:
		Widen<uint8_t>(unsigned_bits, 16);
		return true;
	case VariantLogicalType::UINT32:
		Widen<uint8_t>(unsigned_bits, 32);
		return true;
	case VariantLogicalType::UINT64:
		Widen<uint8_t>(unsigned_bits, 64);
		return true;
	case VariantLogicalType::UINT128:
		Widen<uint8_t>(unsigned_bits, 128);
		return true;
	case VariantLogicalType::FLOAT:
		has_float = true;
		return true;
	case VariantLogicalType::DOUBLE:
		has_double = true;
		return true;
	case VariantLogicalType::DECIMAL: {
		const auto properties = value.GetDecimalProperties();
		D_ASSERT(properties.scale <= properties.width);
		has_decimal = true;
		Widen(decimal_digits, properties.width - properties.scale);
		Widen(decimal_scale, properties.scale);
		return true;
	}
	default:
		return false;
	}
}

void VariantNumericDomain::Merge(const VariantNumericDomain &other) {
	Widen(signed_bits, other.signed_bits);
	Widen(unsigned_bits, other.unsigned_bits);
	Widen(decimal_digits, other.decimal_digits);
	Widen(decimal_scale, other.decimal_scale);
	has_decimal = has_decimal || other.has_decimal;
	has_float = has_float || other.has_float;
	has_double = has_double || other.has_double;
}

bool VariantNumericDomain::TryGetType(VariantLogicalType &type, uint32_t &width, uint32_t &scale) const {
	if (has_float || has_double) {
		// floating point types represent integers exactly up to 2^24 (FLOAT) and 2^53 (DOUBLE), but no fractions
		if (decimal_scale > 0) {
			return false;
		}
		if (!has_double && signed_bits <= 16 && unsigned_bits <= 16 && decimal_digits <= 7) {
			type = VariantLogicalType::FLOAT;
			return true;
		}
		if (signed_bits <= 32 && unsigned_bits <= 32 && decimal_digits <= 15) {
			type = VariantLogicalType::DOUBLE;
			return true;
		}
		return false;
	}
	const auto integer_digits = MaxValue(SignedDigits(signed_bits), UnsignedDigits(unsigned_bits));
	if (has_decimal) {
		const auto decimal_width = idx_t(MaxValue(decimal_digits, integer_digits)) + decimal_scale;
		if (decimal_width > NumericLimits<uint32_t>::Maximum()) {
			return false;
		}
		type = VariantLogicalType::DECIMAL;
		width = NumericCast<uint32_t>(decimal_width);
		scale = decimal_scale;
		return true;
	}
	if (signed_bits == 0) {
		D_ASSERT(unsigned_bits > 0);
		type = UnsignedType(unsigned_bits);
		return true;
	}
	// a signed type has to be twice as wide as an unsigned type to hold all of its values
	const auto bits = MaxValue<idx_t>(signed_bits, 2 * idx_t(unsigned_bits));
	if (bits > 128) {
		// a UINT128 and a signed integer, which only a DECIMAL holds
		type = VariantLogicalType::DECIMAL;
		width = integer_digits;
		scale = 0;
		return true;
	}
	type = SignedType(bits);
	return true;
}

//===--------------------------------------------------------------------===//
// Structure nodes
//===--------------------------------------------------------------------===//
void VariantStructure::StructureNode::Reset() {
	kind = VariantStructureKind::NULL_VALUE;
	type = VariantLogicalType::VARIANT_NULL;
	numeric = VariantNumericDomain();
	ResetChildren();
}

void VariantStructure::StructureNode::ResetChildren() {
	array_child = optional_idx();
	object_children.clear();
}

void VariantStructure::MergeFrame::Initialize(const VariantNode &value, idx_t node_idx_p) {
	node_idx = node_idx_p;
	child_idx = 0;
	cursor = 0;
	object_children.clear();
	if (value.GetTypeId() == VariantLogicalType::OBJECT) {
		array_children.reset();
		for (const auto &entry : value.GetObjectChildren()) {
			object_children.push_back(entry);
		}
	} else {
		array_children.emplace(value.GetArrayChildren());
	}
}

//===--------------------------------------------------------------------===//
// Type names
//===--------------------------------------------------------------------===//
namespace {

const string_t &GetTypeName(VariantLogicalType type) {
	static const auto names = [] {
		array<string_t, static_cast<idx_t>(VariantLogicalType::ENUM_SIZE)> result;
		for (idx_t i = 0; i < result.size(); i++) {
			result[i] = string_t(EnumUtil::ToChars(static_cast<VariantLogicalType>(i)));
		}
		result[static_cast<idx_t>(VariantLogicalType::BOOL_TRUE)] = string_t("BOOLEAN");
		return result;
	}();
	return names[static_cast<idx_t>(type)];
}

const string_t &NullTypeName() {
	static const string_t name("NULL");
	return name;
}

const string_t &VariantTypeName() {
	static const string_t name("VARIANT");
	return name;
}

void EmitTypeName(VariantBuilder &builder, const string_t &name) {
	builder.type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::VARCHAR));
	builder.byte_offsets.push_back(static_cast<uint32_t>(builder.blob.size()));
	VariantBuilderAppendVarint(builder.blob, NumericCast<uint32_t>(name.GetSize()));
	VariantBuilderAppendBytes(builder.blob, const_data_ptr_cast(name.GetData()), name.GetSize());
}

void AppendDigits(char *buffer, idx_t &length, uint32_t value) {
	char digits[10];
	idx_t count = 0;
	do {
		digits[count++] = static_cast<char>('0' + value % 10);
		value /= 10;
	} while (value > 0);
	while (count > 0) {
		buffer[length++] = digits[--count];
	}
}

void EmitDecimalTypeName(VariantBuilder &builder, uint32_t width, uint32_t scale) {
	static constexpr char PREFIX[] = "DECIMAL(";
	char buffer[sizeof("DECIMAL(4294967295,4294967295)")];
	idx_t length = sizeof(PREFIX) - 1;
	memcpy(buffer, PREFIX, length);
	AppendDigits(buffer, length, width);
	buffer[length++] = ',';
	AppendDigits(buffer, length, scale);
	buffer[length++] = ')';
	EmitTypeName(builder, string_t(buffer, NumericCast<uint32_t>(length)));
}

} // namespace

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//
void VariantStructure::Reset() {
	node_count = 0;
	merge_depth = 0;
}

idx_t VariantStructure::CreateNode() {
	if (node_count == nodes.size()) {
		nodes.emplace_back();
	}
	nodes[node_count].Reset();
	return node_count++;
}

idx_t VariantStructure::GetOrCreateRoot() {
	if (IsEmpty()) {
		return CreateNode();
	}
	return 0;
}

void VariantStructure::PushMergeFrame(const VariantNode &value, idx_t node_idx) {
	if (merge_depth == merge_stack.size()) {
		merge_stack.emplace_back();
	}
	merge_stack[merge_depth++].Initialize(value, node_idx);
}

void VariantStructure::MarkIncompatible(StructureNode &node) {
	node.kind = VariantStructureKind::INCOMPATIBLE;
	node.ResetChildren();
}

bool VariantStructure::MergeContainer(StructureNode &node, VariantStructureKind kind) {
	if (node.kind == VariantStructureKind::NULL_VALUE) {
		node.kind = kind;
		return true;
	}
	if (node.kind == kind) {
		return true;
	}
	MarkIncompatible(node);
	return false;
}

void VariantStructure::MergePrimitive(StructureNode &node, VariantLogicalType type) {
	if (node.kind == VariantStructureKind::NULL_VALUE) {
		node.kind = VariantStructureKind::PRIMITIVE;
		node.type = type;
		return;
	}
	if (node.kind != VariantStructureKind::PRIMITIVE || node.type != type) {
		MarkIncompatible(node);
	}
}

void VariantStructure::MergeNumeric(StructureNode &node, const VariantNumericDomain &domain) {
	if (node.kind == VariantStructureKind::NULL_VALUE) {
		node.kind = VariantStructureKind::NUMERIC;
		node.numeric = domain;
		return;
	}
	if (node.kind != VariantStructureKind::NUMERIC) {
		MarkIncompatible(node);
		return;
	}
	node.numeric.Merge(domain);
}

idx_t VariantStructure::GetOrCreateArrayChild(idx_t node_idx) {
	// all elements share one structure node, which is "NULL" for an empty array
	if (!nodes[node_idx].array_child.IsValid()) {
		const auto child_idx = CreateNode();
		nodes[node_idx].array_child = child_idx;
	}
	return nodes[node_idx].array_child.GetIndex();
}

//! Merges 'value' into the structure node, returns whether 'value' is a container that must be traversed
bool VariantStructure::MergeValue(idx_t node_idx, const VariantNode &value) {
	if (nodes[node_idx].kind == VariantStructureKind::INCOMPATIBLE || value.IsNull() || value.IsMissing()) {
		return false;
	}
	const auto type = value.GetTypeId();
	switch (type) {
	case VariantLogicalType::VARIANT_NULL:
		return false;
	case VariantLogicalType::OBJECT:
		return MergeContainer(nodes[node_idx], VariantStructureKind::OBJECT);
	case VariantLogicalType::ARRAY:
		if (!MergeContainer(nodes[node_idx], VariantStructureKind::ARRAY)) {
			return false;
		}
		GetOrCreateArrayChild(node_idx);
		return true;
	case VariantLogicalType::BOOL_FALSE:
		MergePrimitive(nodes[node_idx], VariantLogicalType::BOOL_TRUE);
		return false;
	default: {
		VariantNumericDomain domain;
		if (domain.TryAdd(type, value)) {
			MergeNumeric(nodes[node_idx], domain);
		} else {
			MergePrimitive(nodes[node_idx], type);
		}
		return false;
	}
	}
}

//! Merges a node of another structure into the structure node, returns whether the children must be merged
bool VariantStructure::MergeNode(idx_t node_idx, const StructureNode &other) {
	auto &node = nodes[node_idx];
	if (node.kind == VariantStructureKind::INCOMPATIBLE) {
		return false;
	}
	switch (other.kind) {
	case VariantStructureKind::NULL_VALUE:
		return false;
	case VariantStructureKind::PRIMITIVE:
		MergePrimitive(node, other.type);
		return false;
	case VariantStructureKind::NUMERIC:
		MergeNumeric(node, other.numeric);
		return false;
	case VariantStructureKind::INCOMPATIBLE:
		MarkIncompatible(node);
		return false;
	case VariantStructureKind::OBJECT:
	case VariantStructureKind::ARRAY:
		return MergeContainer(node, other.kind);
	}
	throw InternalException("Unexpected structure kind in VariantStructure::MergeNode");
}

//! Returns the structure node of 'key' in the object node, 'cursor' is the position after the last merged key
idx_t VariantStructure::GetOrCreateObjectChild(idx_t node_idx, idx_t &cursor, const string_t &key,
                                               optional_ptr<ArenaAllocator> key_allocator) {
	auto &children = nodes[node_idx].object_children;
	// the keys of an object are usually merged in the order they are stored in
	if (cursor < children.size() && children[cursor].key == key) {
		return children[cursor++].node_idx;
	}
	idx_t pos;
	if ((cursor == children.size() || key < children[cursor].key) && (cursor == 0 || children[cursor - 1].key < key)) {
		pos = cursor;
	} else {
		auto entry = std::lower_bound(children.begin(), children.end(), key,
		                              [](const StructureChild &child, const string_t &k) { return child.key < k; });
		pos = NumericCast<idx_t>(entry - children.begin());
		if (entry != children.end() && entry->key == key) {
			cursor = pos + 1;
			return entry->node_idx;
		}
	}
	auto stored_key = key;
	if (key_allocator && !key.IsInlined()) {
		auto data = key_allocator->Allocate(key.GetSize());
		memcpy(data, key.GetData(), key.GetSize());
		stored_key = string_t(char_ptr_cast(data), NumericCast<uint32_t>(key.GetSize()));
	}
	// may reallocate 'nodes', invalidating 'children'
	const auto child_idx = CreateNode();
	auto &new_children = nodes[node_idx].object_children;
	new_children.insert(new_children.begin() + NumericCast<int64_t>(pos), StructureChild {stored_key, child_idx});
	cursor = pos + 1;
	return child_idx;
}

//! Merges the children of the frame until one is found that must be traversed
bool VariantStructure::NextChild(MergeFrame &frame, VariantNode &child_value, idx_t &child_idx,
                                 optional_ptr<ArenaAllocator> key_allocator) {
	if (frame.array_children) {
		child_idx = nodes[frame.node_idx].array_child.GetIndex();
		while (frame.child_idx < frame.array_children->size()) {
			if (nodes[child_idx].kind == VariantStructureKind::INCOMPATIBLE) {
				return false;
			}
			child_value = (*frame.array_children)[frame.child_idx++];
			if (MergeValue(child_idx, child_value)) {
				return true;
			}
		}
		return false;
	}
	while (frame.child_idx < frame.object_children.size()) {
		const auto &entry = frame.object_children[frame.child_idx++];
		child_idx = GetOrCreateObjectChild(frame.node_idx, frame.cursor, entry.key, key_allocator);
		child_value = entry.value;
		if (MergeValue(child_idx, child_value)) {
			return true;
		}
	}
	return false;
}

void VariantStructure::Merge(const VariantNode &value, optional_ptr<ArenaAllocator> key_allocator) {
	merge_depth = 0;
	const auto root_idx = GetOrCreateRoot();
	if (MergeValue(root_idx, value)) {
		PushMergeFrame(value, root_idx);
	}
	while (merge_depth > 0) {
		VariantNode child_value;
		idx_t child_idx = 0;
		if (!NextChild(merge_stack[merge_depth - 1], child_value, child_idx, key_allocator)) {
			merge_depth--;
			continue;
		}
		PushMergeFrame(child_value, child_idx);
	}
}

void VariantStructure::Merge(const VariantStructure &other, optional_ptr<ArenaAllocator> key_allocator) {
	D_ASSERT(&other != this);
	if (other.IsEmpty()) {
		return;
	}
	merge_pairs.clear();
	merge_pairs.emplace_back(GetOrCreateRoot(), 0);
	while (!merge_pairs.empty()) {
		const auto node_idx = merge_pairs.back().first;
		const auto &other_node = other.nodes[merge_pairs.back().second];
		merge_pairs.pop_back();
		if (!MergeNode(node_idx, other_node)) {
			continue;
		}
		if (other_node.kind == VariantStructureKind::ARRAY) {
			const auto child_idx = GetOrCreateArrayChild(node_idx);
			merge_pairs.emplace_back(child_idx, other_node.array_child.GetIndex());
			continue;
		}
		// the children of the other node are sorted by key, so the cursor usually finds the key right away
		idx_t cursor = 0;
		for (const auto &other_child : other_node.object_children) {
			const auto child_idx = GetOrCreateObjectChild(node_idx, cursor, other_child.key, key_allocator);
			merge_pairs.emplace_back(child_idx, other_child.node_idx);
		}
	}
}

//===--------------------------------------------------------------------===//
// Emitting
//===--------------------------------------------------------------------===//
void VariantStructure::Emit(VariantBuilder &builder) {
	D_ASSERT(!IsEmpty());
	emit_stack.clear();
	EmitNode(0, builder);
	while (!emit_stack.empty()) {
		auto &frame = emit_stack.back();
		const auto &node = nodes[frame.node_idx];
		const auto child_count = frame.is_array ? 1 : node.object_children.size();
		if (frame.child_idx >= child_count) {
			emit_stack.pop_back();
			continue;
		}
		const auto block = frame.block;
		const auto i = frame.child_idx++;
		idx_t child_idx;
		if (frame.is_array) {
			child_idx = node.array_child.GetIndex();
			builder.AssignArrayChild(block, i);
		} else {
			const auto &child = node.object_children[i];
			child_idx = child.node_idx;
			builder.AssignObjectChild(block, i, child.key);
		}
		// may push a frame, invalidating 'frame'
		EmitNode(child_idx, builder);
	}
}

void VariantStructure::EmitNode(idx_t node_idx, VariantBuilder &builder) {
	const auto &node = nodes[node_idx];
	switch (node.kind) {
	case VariantStructureKind::NULL_VALUE:
		EmitTypeName(builder, NullTypeName());
		return;
	case VariantStructureKind::PRIMITIVE:
		EmitTypeName(builder, GetTypeName(node.type));
		return;
	case VariantStructureKind::NUMERIC: {
		VariantLogicalType type;
		uint32_t width = 0;
		uint32_t scale = 0;
		if (!node.numeric.TryGetType(type, width, scale)) {
			EmitTypeName(builder, VariantTypeName());
		} else if (type == VariantLogicalType::DECIMAL) {
			EmitDecimalTypeName(builder, width, scale);
		} else {
			EmitTypeName(builder, GetTypeName(type));
		}
		return;
	}
	case VariantStructureKind::INCOMPATIBLE:
		EmitTypeName(builder, VariantTypeName());
		return;
	case VariantStructureKind::OBJECT: {
		if (node.object_children.empty()) {
			EmitTypeName(builder, VariantTypeName());
			return;
		}
		const auto block = builder.BeginContainer(VariantLogicalType::OBJECT, node.object_children.size());
		emit_stack.push_back(EmitFrame {block.GetIndex(), 0, node_idx, false});
		return;
	}
	case VariantStructureKind::ARRAY: {
		const auto block = builder.BeginContainer(VariantLogicalType::ARRAY, 1);
		emit_stack.push_back(EmitFrame {block.GetIndex(), 0, node_idx, true});
		return;
	}
	}
}

//===--------------------------------------------------------------------===//
// variant_structure
//===--------------------------------------------------------------------===//
namespace {

class VariantStructureSource {
public:
	explicit VariantStructureSource(const Vector &input) : variants(input) {
	}

	//! Returns whether the row is a SQL NULL
	bool Emit(idx_t row, VariantBuilder &builder) {
		const auto root = variants[row];
		if (root.IsNull() || root.GetTypeId() == VariantLogicalType::VARIANT_NULL) {
			return true;
		}
		structure.Reset();
		structure.Merge(root);
		structure.Emit(builder);
		return false;
	}

private:
	const VectorIterator<VectorVariantType> variants;
	VariantStructure structure;
};

void VariantStructureFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);
	VariantStructureSource source(input.data[0]);
	BuildVariant(source, input.size(), result);
}

} // namespace

ScalarFunction VariantStructureFun::GetFunction() {
	ScalarFunction function("variant_structure", {}, LogicalType::VARIANT(), VariantStructureFunction);
	function.GetSignature().AddParameter("input_variant", LogicalType::VARIANT());
	return function;
}

} // namespace duckdb
