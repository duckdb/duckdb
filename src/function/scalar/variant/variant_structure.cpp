#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Structure nodes
//===--------------------------------------------------------------------===//
enum class StructureKind : uint8_t { NULL_VALUE, PRIMITIVE, OBJECT, ARRAY, INCOMPATIBLE };

//! A structure node represents a subtree of the output structure.
struct StructureNode {
	StructureKind kind = StructureKind::NULL_VALUE;
	//! PRIMITIVE: the merged type, BOOL_TRUE for booleans
	VariantLogicalType type = VariantLogicalType::VARIANT_NULL;
	//! PRIMITIVE: the merged numeric type, used to widen numerics (INVALID if not numeric)
	LogicalType numeric_type;
	//! ARRAY: the structure shared by all elements
	optional_idx array_child;
	//! OBJECT: the structure of each field, keys point into the input VARIANT
	string_map_t<idx_t> object_children;
};

//! Tracks one input container (OBJECT or ARRAY) whose children are merged into a structure node.
struct ExtractFrame {
	ExtractFrame(const VariantNode &value, idx_t node_idx) : node_idx(node_idx) {
		if (value.GetTypeId() == VariantLogicalType::OBJECT) {
			for (const auto &entry : value.GetObjectChildren()) {
				object_children.push_back(entry);
			}
		} else {
			array_children.emplace(value.GetArrayChildren());
		}
	}

	idx_t node_idx;
	idx_t child_idx = 0;
	vector<VariantObjectEntry> object_children;
	optional<VariantArrayIterator> array_children;
};

//! Tracks one output container whose children are being emitted.
struct EmitFrame {
	idx_t block = 0;
	idx_t child_idx = 0;
	//! OBJECT: the children sorted by key
	vector<pair<string_t, idx_t>> object_children;
	//! ARRAY: the single element structure
	optional_idx array_child;

	idx_t ChildCount() const {
		return array_child.IsValid() ? 1 : object_children.size();
	}
};

//===--------------------------------------------------------------------===//
// Structure extraction
//===--------------------------------------------------------------------===//
//! Infers the structure of a VARIANT value and emits it as a VARIANT. Both passes use an explicit stack, so the
//! nesting depth is not bounded by the thread stack.
class VariantStructureExtractor {
public:
	void Extract(const VariantNode &root) {
		nodes.clear();
		extract_stack.clear();
		const auto root_idx = CreateNode();
		if (MergeValue(root_idx, root)) {
			extract_stack.emplace_back(root, root_idx);
		}
		while (!extract_stack.empty()) {
			VariantNode child_value;
			idx_t child_idx = 0;
			if (!NextChild(extract_stack.back(), child_value, child_idx)) {
				extract_stack.pop_back();
				continue;
			}
			extract_stack.emplace_back(child_value, child_idx);
		}
	}

	void Emit(VariantBuilder &builder) {
		D_ASSERT(!nodes.empty());
		emit_stack.clear();
		EmitNode(0, builder);
		while (!emit_stack.empty()) {
			auto &frame = emit_stack.back();
			if (frame.child_idx >= frame.ChildCount()) {
				emit_stack.pop_back();
				continue;
			}
			const auto block = frame.block;
			const auto i = frame.child_idx++;
			idx_t child_idx;
			if (frame.array_child.IsValid()) {
				child_idx = frame.array_child.GetIndex();
				builder.AssignArrayChild(block, i);
			} else {
				child_idx = frame.object_children[i].second;
				builder.AssignObjectChild(block, i, frame.object_children[i].first);
			}
			// may push a frame, invalidating 'frame'
			EmitNode(child_idx, builder);
		}
	}

private:
	idx_t CreateNode() {
		nodes.emplace_back();
		return nodes.size() - 1;
	}

	static void MarkIncompatible(StructureNode &node) {
		node.kind = StructureKind::INCOMPATIBLE;
		node.numeric_type = LogicalType::INVALID;
		node.array_child = optional_idx();
		node.object_children.clear();
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
		if (node.kind == StructureKind::PRIMITIVE && node.type == type && type != VariantLogicalType::DECIMAL) {
			return;
		}
		auto numeric_type = VariantUtils::GetNumericType(value);
		if (node.kind == StructureKind::NULL_VALUE) {
			node.kind = StructureKind::PRIMITIVE;
			node.type = type;
			node.numeric_type = std::move(numeric_type);
			return;
		}
		if (node.kind != StructureKind::PRIMITIVE) {
			MarkIncompatible(node);
			return;
		}
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

	idx_t GetOrCreateObjectChild(idx_t node_idx, const string_t &key) {
		auto entry = nodes[node_idx].object_children.find(key);
		if (entry != nodes[node_idx].object_children.end()) {
			return entry->second;
		}
		const auto child_idx = CreateNode();
		nodes[node_idx].object_children.emplace(key, child_idx);
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
			child_idx = GetOrCreateObjectChild(frame.node_idx, entry.key);
			child_value = entry.value;
			if (MergeValue(child_idx, child_value)) {
				return true;
			}
		}
		return false;
	}

	static void EmitTypeName(VariantBuilder &builder, const char *name) {
		builder.EmitPrimitive(Value(name), NumericCast<uint32_t>(builder.blob.size()));
	}

	void EmitNode(idx_t node_idx, VariantBuilder &builder) {
		const auto &node = nodes[node_idx];
		switch (node.kind) {
		case StructureKind::NULL_VALUE:
			EmitTypeName(builder, "NULL");
			return;
		case StructureKind::PRIMITIVE:
			EmitTypeName(builder,
			             node.type == VariantLogicalType::BOOL_TRUE ? "BOOLEAN" : EnumUtil::ToChars(node.type));
			return;
		case StructureKind::INCOMPATIBLE:
			EmitTypeName(builder, "VARIANT");
			return;
		case StructureKind::OBJECT: {
			if (node.object_children.empty()) {
				EmitTypeName(builder, "VARIANT");
				return;
			}
			EmitFrame frame;
			frame.object_children.assign(node.object_children.begin(), node.object_children.end());
			std::sort(frame.object_children.begin(), frame.object_children.end(),
			          [](const pair<string_t, idx_t> &a, const pair<string_t, idx_t> &b) { return a.first < b.first; });
			frame.block = builder.BeginContainer(VariantLogicalType::OBJECT, frame.object_children.size()).GetIndex();
			emit_stack.push_back(std::move(frame));
			return;
		}
		case StructureKind::ARRAY: {
			EmitFrame frame;
			frame.array_child = node.array_child;
			frame.block = builder.BeginContainer(VariantLogicalType::ARRAY, 1).GetIndex();
			emit_stack.push_back(std::move(frame));
			return;
		}
		}
	}

private:
	vector<StructureNode> nodes;
	vector<ExtractFrame> extract_stack;
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
