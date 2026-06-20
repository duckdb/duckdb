//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/variant/parquet_variant_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/variant_value.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/helper.hpp"
#include "reader/variant/variant_binary_decoder.hpp"

namespace duckdb {

struct VariantBuilder;

//! A "group" in the Parquet shredded VARIANT layout: STRUCT(value BLOB, [typed_value <T>]).
//! The recursive view normalizes (flattens) the regular typed vectors of the group tree once, so each
//! layer can be navigated by index during the (per-row) single-pass conversion.
enum class ParquetGroupKind : uint8_t { LEAF, OBJECT, ARRAY };

struct ShreddedGroupView {
	//! 'value' (the binary-encoded fallback / overlay)
	UnifiedVectorFormat value_format;
	const string_t *value_data = nullptr;

	//! Whether the group has a 'typed_value' (a shredded representation)
	bool has_typed_value = false;
	ParquetGroupKind kind = ParquetGroupKind::LEAF;
	//! 'typed_value' (validity + data: the primitive for LEAF, list_entry_t for ARRAY, struct for OBJECT)
	UnifiedVectorFormat typed_format;
	LogicalType typed_type;

	//! OBJECT: the field names + their (group) sub-views
	vector<string> field_names;
	vector<unique_ptr<ShreddedGroupView>> fields;

	//! ARRAY: the list entries + the element (group) sub-view
	const list_entry_t *list_data = nullptr;
	unique_ptr<ShreddedGroupView> element;

	//! Build the view recursively from a group Vector
	void Build(Vector &group);
};

class ParquetVariantIterator;
class ParquetObjectIterator;
class ParquetArrayIterator;

//! A lightweight cursor pointing at a single logical Parquet VARIANT value. A node is either a position
//! in the shredded tree (OBJECT / ARRAY), or a materialized VariantValue (VALUE) for binary-encoded
//! fallbacks and shredded leaves - those are emitted through VariantBuilder::EmitVariantValue.
class ParquetVariantNode {
public:
	enum class Kind : uint8_t { NULL_VALUE, MISSING, OBJECT, ARRAY, VALUE };

public:
	ParquetVariantNode() : kind(Kind::NULL_VALUE) {
	}

public:
	static ParquetVariantNode MakeNull() {
		return ParquetVariantNode(Kind::NULL_VALUE);
	}
	static ParquetVariantNode MakeMissing() {
		return ParquetVariantNode(Kind::MISSING);
	}
	static ParquetVariantNode MakeValue(const VariantValue *value) {
		ParquetVariantNode result(Kind::VALUE);
		result.value = value;
		return result;
	}
	static ParquetVariantNode MakeObject(const ParquetVariantIterator &state, const ShreddedGroupView &view,
	                                     idx_t index, const VariantValue *overlay) {
		ParquetVariantNode result(Kind::OBJECT);
		result.state = &state;
		result.view = &view;
		result.index = index;
		result.overlay = overlay;
		return result;
	}
	static ParquetVariantNode MakeArray(const ParquetVariantIterator &state, const ShreddedGroupView &view,
	                                    idx_t index) {
		ParquetVariantNode result(Kind::ARRAY);
		result.state = &state;
		result.view = &view;
		result.index = index;
		return result;
	}

public:
	bool IsNull() const {
		return kind == Kind::NULL_VALUE;
	}
	bool IsMissing() const {
		return kind == Kind::MISSING;
	}
	//! Non-null for VALUE nodes: the whole subtree is emitted via VariantBuilder::EmitVariantValue
	optional_ptr<const VariantValue> AsValue() const {
		return kind == Kind::VALUE ? value : nullptr;
	}
	VariantLogicalType GetTypeId() const {
		switch (kind) {
		case Kind::OBJECT:
			return VariantLogicalType::OBJECT;
		case Kind::ARRAY:
			return VariantLogicalType::ARRAY;
		case Kind::NULL_VALUE:
			return VariantLogicalType::VARIANT_NULL;
		default:
			throw InternalException("ParquetVariantNode::GetTypeId on a VALUE/MISSING node");
		}
	}

	//! Primitive accessors are unreachable: every primitive/leaf is resolved to a VALUE node
	string_t GetString() const {
		throw InternalException("ParquetVariantNode::GetString - primitives are emitted as VALUE nodes");
	}
	VariantDecimalData GetDecimal() const {
		throw InternalException("ParquetVariantNode::GetDecimal - primitives are emitted as VALUE nodes");
	}
	const_data_ptr_t GetDataPointer() const {
		throw InternalException("ParquetVariantNode::GetDataPointer - primitives are emitted as VALUE nodes");
	}

	ParquetObjectIterator GetObjectChildren(VariantIterationOrder order) const;
	ParquetArrayIterator GetArrayChildren() const;

private:
	explicit ParquetVariantNode(Kind kind) : kind(kind) {
	}

private:
	Kind kind;
	//! VALUE: the materialized value
	const VariantValue *value = nullptr;
	//! OBJECT / ARRAY: the shredded position
	optional_ptr<const ParquetVariantIterator> state;
	optional_ptr<const ShreddedGroupView> view;
	idx_t index = 0;
	//! OBJECT: the (decoded) partial-shredding overlay object (or null when fully shredded)
	const VariantValue *overlay = nullptr;
};

//! A single (key, value) entry of an OBJECT
struct ParquetObjectEntry {
	string_t key;
	ParquetVariantNode value;
};

//! Iterates the (key, value) children of a shredded OBJECT: the typed struct fields merged with the
//! leftover (overlay) object's fields, deduplicated (typed fields win) and sorted lexicographically.
class ParquetObjectIterator {
public:
	ParquetObjectIterator(const ParquetVariantIterator &state, const ShreddedGroupView &view, idx_t index,
	                      const VariantValue *overlay);

public:
	const ParquetObjectEntry *begin() const { // NOLINT: match stl API
		return ordered_entries.data();
	}
	const ParquetObjectEntry *end() const { // NOLINT: match stl API
		return ordered_entries.data() + ordered_entries.size();
	}

private:
	vector<ParquetObjectEntry> ordered_entries;
};

//! Iterates the element values of a shredded ARRAY. Random-access: a child cursor is resolved on demand.
class ParquetArrayIterator {
public:
	ParquetArrayIterator(const ParquetVariantIterator &state, const ShreddedGroupView &view, idx_t index);

public:
	idx_t size() const {
		return length;
	}
	ParquetVariantNode operator[](idx_t i) const;

private:
	reference<const ParquetVariantIterator> state;
	//! the element (group) sub-view
	reference<const ShreddedGroupView> element;
	idx_t base;
	idx_t length;
};

//! Iterates a shredded Parquet VARIANT column (metadata + group), handing out ParquetVariantNode cursors
//! per row. The decoded (binary) VariantValues a row needs are owned by a per-row arena so the cursors
//! that point into them stay valid for the duration of that row's emit.
class ParquetVariantIterator {
public:
	ParquetVariantIterator(Vector &metadata, Vector &group);

public:
	//! Reset the per-row state (arena + lazily-decoded metadata) for a new row
	void BeginRow(idx_t row);
	//! Resolve the root value of 'row' (a missing root is promoted to a SQL NULL)
	ParquetVariantNode Root(idx_t row) const;
	//! Resolve the value of the group 'view' at logical position 'index'
	ParquetVariantNode ResolveGroup(const ShreddedGroupView &view, idx_t index) const;

	//! The (lazily-decoded) Variant metadata of the current row
	const VariantMetadata &GetMetadata() const;
	//! Decode the binary 'value' of 'view' at flat position 'value_index' into a VariantValue
	VariantValue DecodeBinary(const ShreddedGroupView &view, idx_t value_index) const;
	//! Own a decoded VariantValue for the duration of the current row, returning a stable pointer to it
	const VariantValue *ArenaAdd(VariantValue value) const;

private:
	ShreddedGroupView root_view;

	UnifiedVectorFormat metadata_format;
	const string_t *metadata_data;

	idx_t current_row = 0;
	mutable unique_ptr<VariantMetadata> current_metadata;
	mutable vector<unique_ptr<VariantValue>> arena;
};

//! BuildVariant source wrapping a ParquetVariantIterator (mirrors VariantIteratorSource in core)
struct ParquetVariantIteratorSource {
	explicit ParquetVariantIteratorSource(ParquetVariantIterator &iterator) : iterator(iterator) {
	}
	bool Emit(idx_t row, VariantBuilder &builder);

	ParquetVariantIterator &iterator;
};

//! Convert a shredded Parquet VARIANT (metadata + group) into the canonical VARIANT 'result' in a single
//! pass through the shared VariantBuilder
class ParquetVariantConversion {
public:
	static void Convert(Vector &metadata, Vector &group, Vector &result, idx_t count);
};

} // namespace duckdb
