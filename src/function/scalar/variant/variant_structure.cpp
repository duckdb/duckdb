#include "duckdb/common/array.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Structure nodes
//===--------------------------------------------------------------------===//
enum class StructureKind : uint8_t { NULL_VALUE, PRIMITIVE, OBJECT, ARRAY, INCOMPATIBLE };

//! A field of an OBJECT structure node, the key points into the input VARIANT
struct StructureChild {
	string_t key;
	idx_t node_idx;
};

//! A structure node represents a subtree of the output structure.
struct StructureNode {
	StructureKind kind = StructureKind::NULL_VALUE;
	//! PRIMITIVE: the merged type, BOOL_TRUE for booleans
	VariantLogicalType type = VariantLogicalType::VARIANT_NULL;
	//! PRIMITIVE: the first value merged, its numeric type is only needed when another type is merged
	VariantNode first_value;
	//! PRIMITIVE: the merged numeric type, set once a value of another type (or a DECIMAL) is merged
	LogicalType numeric_type;
	//! ARRAY: the structure shared by all elements
	optional_idx array_child;
	//! OBJECT: the structure of each field, sorted by key
	vector<StructureChild> object_children;

	void Reset() {
		kind = StructureKind::NULL_VALUE;
		type = VariantLogicalType::VARIANT_NULL;
		ResetChildren();
	}

	void ResetChildren() {
		if (numeric_type.id() != LogicalTypeId::INVALID) {
			numeric_type = LogicalType::INVALID;
		}
		array_child = optional_idx();
		object_children.clear();
	}
};

//! Tracks one input container (OBJECT or ARRAY) whose children are merged into a structure node.
struct ExtractFrame {
	void Initialize(const VariantNode &value, idx_t node_idx_p) {
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

	idx_t node_idx = 0;
	idx_t child_idx = 0;
	//! OBJECT: the position in the children of the structure node after the last merged key
	idx_t cursor = 0;
	vector<VariantObjectEntry> object_children;
	optional<VariantArrayIterator> array_children;
};

//! Tracks one output container whose children are being emitted.
struct EmitFrame {
	idx_t block;
	idx_t child_idx;
	idx_t node_idx;
	bool is_array;
};

//===--------------------------------------------------------------------===//
// Type names
//===--------------------------------------------------------------------===//
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

//===--------------------------------------------------------------------===//
// Structure extraction
//===--------------------------------------------------------------------===//
//! Infers the structure of a VARIANT value and emits it as a VARIANT. Both passes use an explicit stack, so the
//! nesting depth is not bounded by the thread stack. Nodes and frames are reused across rows.
class VariantStructureExtractor {
public:
	void Extract(const VariantNode &root) {
		node_count = 0;
		extract_depth = 0;
		const auto root_idx = CreateNode();
		if (MergeValue(root_idx, root)) {
			PushFrame(root, root_idx);
		}
		while (extract_depth > 0) {
			VariantNode child_value;
			idx_t child_idx = 0;
			if (!NextChild(extract_stack[extract_depth - 1], child_value, child_idx)) {
				extract_depth--;
				continue;
			}
			PushFrame(child_value, child_idx);
		}
	}

	void Emit(VariantBuilder &builder) {
		D_ASSERT(node_count > 0);
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

private:
	//! May reallocate 'nodes', invalidating references into it
	idx_t CreateNode() {
		if (node_count == nodes.size()) {
			nodes.emplace_back();
		}
		nodes[node_count].Reset();
		return node_count++;
	}

	void PushFrame(const VariantNode &value, idx_t node_idx) {
		if (extract_depth == extract_stack.size()) {
			extract_stack.emplace_back();
		}
		extract_stack[extract_depth++].Initialize(value, node_idx);
	}

	static void MarkIncompatible(StructureNode &node) {
		node.kind = StructureKind::INCOMPATIBLE;
		node.ResetChildren();
	}

	static bool MergeContainer(StructureNode &node, StructureKind kind) {
		if (node.kind == StructureKind::NULL_VALUE) {
			node.kind = kind;
			return true;
		}
		if (node.kind == kind) {
			return true;
		}
		MarkIncompatible(node);
		return false;
	}

	static void MergePrimitive(StructureNode &node, const VariantNode &value, VariantLogicalType type) {
		if (type == VariantLogicalType::BOOL_FALSE) {
			type = VariantLogicalType::BOOL_TRUE;
		}
		if (node.kind == StructureKind::NULL_VALUE) {
			node.kind = StructureKind::PRIMITIVE;
			node.type = type;
			node.first_value = value;
			return;
		}
		if (node.kind != StructureKind::PRIMITIVE) {
			MarkIncompatible(node);
			return;
		}
		if (node.type == type && type != VariantLogicalType::DECIMAL) {
			return;
		}
		if (node.numeric_type.id() == LogicalTypeId::INVALID) {
			node.numeric_type = VariantUtils::GetNumericType(node.first_value);
		}
		auto numeric_type = VariantUtils::GetNumericType(value);
		if (node.numeric_type.id() == LogicalTypeId::INVALID || numeric_type.id() == LogicalTypeId::INVALID) {
			MarkIncompatible(node);
			return;
		}
		node.numeric_type = LogicalType::DefaultForceMaxLogicalType(node.numeric_type, numeric_type);
		node.type = VariantUtils::GetNumericVariantType(node.numeric_type);
	}

	//! Merges 'value' into the structure node, returns whether 'value' is a container that must be traversed
	bool MergeValue(idx_t node_idx, const VariantNode &value) {
		if (nodes[node_idx].kind == StructureKind::INCOMPATIBLE || value.IsNull() || value.IsMissing()) {
			return false;
		}
		const auto type = value.GetTypeId();
		switch (type) {
		case VariantLogicalType::VARIANT_NULL:
			return false;
		case VariantLogicalType::OBJECT:
			return MergeContainer(nodes[node_idx], StructureKind::OBJECT);
		case VariantLogicalType::ARRAY: {
			if (!MergeContainer(nodes[node_idx], StructureKind::ARRAY)) {
				return false;
			}
			// all elements share one structure node, which is "NULL" for an empty array
			if (!nodes[node_idx].array_child.IsValid()) {
				const auto child_idx = CreateNode();
				nodes[node_idx].array_child = child_idx;
			}
			return true;
		}
		default:
			MergePrimitive(nodes[node_idx], value, type);
			return false;
		}
	}

	//! Returns the structure node of 'key' in the object node, 'cursor' is the position after the last merged key
	idx_t GetOrCreateObjectChild(idx_t node_idx, idx_t &cursor, const string_t &key) {
		auto &children = nodes[node_idx].object_children;
		// the keys of an object are usually merged in the order they are stored in
		if (cursor < children.size() && children[cursor].key == key) {
			return children[cursor++].node_idx;
		}
		idx_t pos;
		if ((cursor == children.size() || key < children[cursor].key) &&
		    (cursor == 0 || children[cursor - 1].key < key)) {
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
		const auto child_idx = CreateNode();
		auto &new_children = nodes[node_idx].object_children;
		new_children.insert(new_children.begin() + NumericCast<int64_t>(pos), StructureChild {key, child_idx});
		cursor = pos + 1;
		return child_idx;
	}

	//! Merges the children of the frame until one is found that must be traversed
	bool NextChild(ExtractFrame &frame, VariantNode &child_value, idx_t &child_idx) {
		if (frame.array_children) {
			child_idx = nodes[frame.node_idx].array_child.GetIndex();
			while (frame.child_idx < frame.array_children->size()) {
				if (nodes[child_idx].kind == StructureKind::INCOMPATIBLE) {
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
			child_idx = GetOrCreateObjectChild(frame.node_idx, frame.cursor, entry.key);
			child_value = entry.value;
			if (MergeValue(child_idx, child_value)) {
				return true;
			}
		}
		return false;
	}

	static void EmitTypeName(VariantBuilder &builder, const string_t &name) {
		builder.type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::VARCHAR));
		builder.byte_offsets.push_back(static_cast<uint32_t>(builder.blob.size()));
		VariantBuilderAppendVarint(builder.blob, NumericCast<uint32_t>(name.GetSize()));
		VariantBuilderAppendBytes(builder.blob, const_data_ptr_cast(name.GetData()), name.GetSize());
	}

	void EmitNode(idx_t node_idx, VariantBuilder &builder) {
		const auto &node = nodes[node_idx];
		switch (node.kind) {
		case StructureKind::NULL_VALUE:
			EmitTypeName(builder, NullTypeName());
			return;
		case StructureKind::PRIMITIVE:
			EmitTypeName(builder, GetTypeName(node.type));
			return;
		case StructureKind::INCOMPATIBLE:
			EmitTypeName(builder, VariantTypeName());
			return;
		case StructureKind::OBJECT: {
			if (node.object_children.empty()) {
				EmitTypeName(builder, VariantTypeName());
				return;
			}
			const auto block = builder.BeginContainer(VariantLogicalType::OBJECT, node.object_children.size());
			emit_stack.push_back(EmitFrame {block.GetIndex(), 0, node_idx, false});
			return;
		}
		case StructureKind::ARRAY: {
			const auto block = builder.BeginContainer(VariantLogicalType::ARRAY, 1);
			emit_stack.push_back(EmitFrame {block.GetIndex(), 0, node_idx, true});
			return;
		}
		}
	}

private:
	//! The nodes [0, node_count) hold the structure of the current row
	vector<StructureNode> nodes;
	idx_t node_count = 0;
	//! The frames [0, extract_depth) are the containers being traversed
	vector<ExtractFrame> extract_stack;
	idx_t extract_depth = 0;
	vector<EmitFrame> emit_stack;
};

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
		extractor.Extract(root);
		extractor.Emit(builder);
		return false;
	}

private:
	const VectorIterator<VectorVariantType> variants;
	VariantStructureExtractor extractor;
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
