//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/variant/parquet_variant_iterator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/variant.hpp"
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

//! A lightweight cursor pointing at a single logical Parquet VARIANT value. A value is either SHREDDED
//! (a position in a typed vector tree) or BINARY (a Spark variant-encoded value at a byte offset in a
//! 'value' blob). Both expose the same node concept, so the shared EmitIterator traverses them uniformly
//! and emits the correct VariantLogicalType - no value is ever materialized as a whole VariantValue.
class ParquetVariantNode {
public:
	enum class Kind : uint8_t { NULL_VALUE, MISSING, SHREDDED, BINARY };

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
	//! A position in the shredded (typed) tree. 'overlay' is the binary OBJECT holding the leftover fields
	//! of a partially-shredded object (or null when fully shredded / not an object).
	static ParquetVariantNode MakeShredded(const ParquetVariantIterator &state, const ShreddedGroupView &view,
	                                       idx_t index, const_data_ptr_t overlay = nullptr) {
		ParquetVariantNode result(Kind::SHREDDED);
		result.state = &state;
		result.view = &view;
		result.index = index;
		result.binary = overlay;
		return result;
	}
	//! A Spark variant-encoded value starting at 'data' (its header byte)
	static ParquetVariantNode MakeBinary(const ParquetVariantIterator &state, const_data_ptr_t data) {
		ParquetVariantNode result(Kind::BINARY);
		result.state = &state;
		result.binary = data;
		return result;
	}

public:
	bool IsNull() const {
		return kind == Kind::NULL_VALUE;
	}
	bool IsMissing() const {
		return kind == Kind::MISSING;
	}

	VariantLogicalType GetTypeId() const;
	string_t GetString() const;
	VariantDecimalData GetDecimal() const;
	const_data_ptr_t GetDataPointer() const;
	ParquetObjectIterator GetObjectChildren(VariantIterationOrder order) const;
	ParquetArrayIterator GetArrayChildren() const;

private:
	explicit ParquetVariantNode(Kind kind) : kind(kind) {
	}

private:
	Kind kind;
	optional_ptr<const ParquetVariantIterator> state;
	//! SHREDDED: the shredded position
	optional_ptr<const ShreddedGroupView> view;
	idx_t index = 0;
	//! SHREDDED OBJECT: the (binary) overlay object holding leftover fields, or null.
	//! BINARY: the value's start (header byte).
	const_data_ptr_t binary = nullptr;
};

//! A single (key, value) entry of an OBJECT
struct ParquetObjectEntry {
	string_t key;
	ParquetVariantNode value;
};

//! Iterates the (key, value) children of an OBJECT: for a shredded object, the typed struct fields merged
//! with the leftover (binary overlay) fields - deduplicated (typed fields win) and sorted lexicographically;
//! for a binary object, the encoded fields.
class ParquetObjectIterator {
public:
	//! Shredded object (typed fields + optional binary overlay)
	ParquetObjectIterator(const ParquetVariantIterator &state, const ShreddedGroupView &view, idx_t index,
	                      const_data_ptr_t overlay);
	//! Binary object
	ParquetObjectIterator(const ParquetVariantIterator &state, const VariantMetadata &metadata, const_data_ptr_t data);

public:
	const ParquetObjectEntry *begin() const { // NOLINT: match stl API
		return ordered_entries.data();
	}
	const ParquetObjectEntry *end() const { // NOLINT: match stl API
		return ordered_entries.data() + ordered_entries.size();
	}

private:
	void Finalize();

private:
	vector<ParquetObjectEntry> ordered_entries;
};

//! Iterates the element values of an ARRAY (shredded list or binary array). Random-access.
class ParquetArrayIterator {
public:
	//! Shredded array
	ParquetArrayIterator(const ParquetVariantIterator &state, const ShreddedGroupView &view, idx_t index);
	//! Binary array
	ParquetArrayIterator(const ParquetVariantIterator &state, const VariantMetadata &metadata, const_data_ptr_t data);

public:
	idx_t size() const {
		return length;
	}
	ParquetVariantNode operator[](idx_t i) const;

private:
	reference<const ParquetVariantIterator> state;
	bool shredded;
	idx_t length;

	//! SHREDDED: the element (group) sub-view + the list offset of the elements
	optional_ptr<const ShreddedGroupView> element;
	idx_t base = 0;

	//! BINARY: where to read each element's value offset, and the values base
	const_data_ptr_t field_offsets = nullptr;
	const_data_ptr_t values = nullptr;
	uint32_t field_offset_size = 0;
};

//! Iterates a shredded Parquet VARIANT column (metadata + group), handing out ParquetVariantNode cursors
//! per row. Binary (Spark-encoded) values are read directly from the 'value' blobs; the only per-row
//! materialization is a small byte arena for the handful of leaf payloads that need a representation
//! change (BINARY->base64, UUID byte order, binary DECIMAL re-encoding).
class ParquetVariantIterator {
public:
	ParquetVariantIterator(Vector &metadata, Vector &group);

public:
	//! Reset the per-row state (byte arena + lazily-decoded metadata) for a new row
	void BeginRow(idx_t row);
	//! Resolve the root value of 'row' (a missing root is promoted to a SQL NULL)
	ParquetVariantNode Root(idx_t row) const;
	//! Resolve the value of the group 'view' at logical position 'index'
	ParquetVariantNode ResolveGroup(const ShreddedGroupView &view, idx_t index) const;

	//! The (lazily-decoded) Variant metadata of the current row
	const VariantMetadata &GetMetadata() const;
	//! Own 'size' bytes for the duration of the current row, returning a stable pointer to a copy
	const_data_ptr_t StoreBytes(const_data_ptr_t data, idx_t size) const;
	//! Own a string for the duration of the current row, returning a stable string_t view of it
	string_t StoreString(string str) const;

private:
	ShreddedGroupView root_view;

	UnifiedVectorFormat metadata_format;
	const string_t *metadata_data;

	idx_t current_row = 0;
	mutable unique_ptr<VariantMetadata> current_metadata;
	mutable vector<unique_ptr<string>> byte_arena;
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
