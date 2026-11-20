//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/variant/variant_structure.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ArenaAllocator;
struct VariantBuilder;

//! The union of the domains of the numeric types merged at one position of a structure
struct VariantNumericDomain {
	//! The widest signed and unsigned integer types, in bits (0 if there are none)
	uint8_t signed_bits = 0;
	uint8_t unsigned_bits = 0;
	//! The widest integral part (width - scale) and scale of the DECIMALs
	uint32_t decimal_digits = 0;
	uint32_t decimal_scale = 0;
	bool has_decimal = false;
	bool has_float = false;
	bool has_double = false;

	//! Adds the domain of a numeric value, returns false if the value is not numeric
	bool TryAdd(VariantLogicalType type, const VariantNode &value);
	void Merge(const VariantNumericDomain &other);
	//! Gets the narrowest type that represents the domain exactly, returns false if there is none
	bool TryGetType(VariantLogicalType &type, uint32_t &width, uint32_t &scale) const;
};

//! The kind of a position in a VariantStructure
enum class VariantStructureKind : uint8_t { NULL_VALUE, PRIMITIVE, NUMERIC, OBJECT, ARRAY, INCOMPATIBLE };

//! Accumulates the structure of VARIANT values, as returned by variant_structure and variant_group_structure.
//! Merging is commutative and associative, so the result does not depend on the order of the merged values. Both
//! merging and emitting use an explicit stack, so the nesting depth is not bounded by the thread stack.
class VariantStructure {
public:
	//! Clears the structure, keeping its allocations
	DUCKDB_API void Reset();
	//! Whether nothing has been merged since the last reset
	bool IsEmpty() const {
		return node_count == 0;
	}
	//! Merges the structure of 'value'. Object keys are copied into 'key_allocator' if it is set, otherwise they
	//! must outlive the structure.
	DUCKDB_API void Merge(const VariantNode &value, optional_ptr<ArenaAllocator> key_allocator = nullptr);
	//! Merges another structure, see Merge(const VariantNode &, ...) for 'key_allocator'
	DUCKDB_API void Merge(const VariantStructure &other, optional_ptr<ArenaAllocator> key_allocator = nullptr);
	//! Emits the structure as a VARIANT value, the structure must not be empty
	DUCKDB_API void Emit(VariantBuilder &builder);

private:
	//! A field of an OBJECT structure node
	struct StructureChild {
		string_t key;
		idx_t node_idx;
	};

	//! A structure node represents a subtree of the structure
	struct StructureNode {
		VariantStructureKind kind = VariantStructureKind::NULL_VALUE;
		//! PRIMITIVE: the type, BOOL_TRUE for booleans
		VariantLogicalType type = VariantLogicalType::VARIANT_NULL;
		//! NUMERIC: the union of the merged numeric domains
		VariantNumericDomain numeric;
		//! ARRAY: the structure shared by all elements
		optional_idx array_child;
		//! OBJECT: the structure of each field, sorted by key
		vector<StructureChild> object_children;

		void Reset();
		void ResetChildren();
	};

	//! Tracks one input container (OBJECT or ARRAY) whose children are merged into a structure node
	struct MergeFrame {
		void Initialize(const VariantNode &value, idx_t node_idx_p);

		idx_t node_idx = 0;
		idx_t child_idx = 0;
		//! OBJECT: the position in the children of the structure node after the last merged key
		idx_t cursor = 0;
		vector<VariantObjectEntry> object_children;
		optional<VariantArrayIterator> array_children;
	};
	//! Tracks one output container whose children are being emitted
	struct EmitFrame {
		idx_t block;
		idx_t child_idx;
		idx_t node_idx;
		bool is_array;
	};

private:
	idx_t CreateNode();
	idx_t GetOrCreateRoot();
	void PushMergeFrame(const VariantNode &value, idx_t node_idx);
	static void MarkIncompatible(StructureNode &node);
	static bool MergeContainer(StructureNode &node, VariantStructureKind kind);
	static void MergePrimitive(StructureNode &node, VariantLogicalType type);
	static void MergeNumeric(StructureNode &node, const VariantNumericDomain &domain);
	bool MergeValue(idx_t node_idx, const VariantNode &value);
	bool MergeNode(idx_t node_idx, const StructureNode &other);
	idx_t GetOrCreateArrayChild(idx_t node_idx);
	idx_t GetOrCreateObjectChild(idx_t node_idx, idx_t &cursor, const string_t &key,
	                             optional_ptr<ArenaAllocator> key_allocator);
	bool NextChild(MergeFrame &frame, VariantNode &child_value, idx_t &child_idx,
	               optional_ptr<ArenaAllocator> key_allocator);
	void EmitNode(idx_t node_idx, VariantBuilder &builder);

private:
	//! The nodes [0, node_count) hold the structure, the root is node 0
	vector<StructureNode> nodes;
	idx_t node_count = 0;
	//! The frames [0, merge_depth) are the containers being traversed
	vector<MergeFrame> merge_stack;
	idx_t merge_depth = 0;
	//! The (node, other node) pairs still to be merged by Merge(const VariantStructure &, ...)
	vector<pair<idx_t, idx_t>> merge_pairs;
	vector<EmitFrame> emit_stack;
};

} // namespace duckdb
