//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {

class BufferHandle;
class VectorBuffer;
class Vector;

enum class VectorBufferType : uint8_t {
	STANDARD_BUFFER,   // standard buffer, holds a single array of data
	DICTIONARY_BUFFER, // dictionary buffer, holds a selection vector and child vector
	STRING_BUFFER,     // string buffer, holds a string heap
	FSST_BUFFER,       // fsst compressed string buffer, holds a string heap, fsst symbol table and a string count
	STRUCT_BUFFER,     // struct buffer, holds a ordered mapping from name to child vector
	LIST_BUFFER,       // list buffer, holds a single flatvector child
	MANAGED_BUFFER,    // managed buffer, holds a buffer managed by the buffermanager
	OPAQUE_BUFFER,     // opaque buffer, can be created for example by the parquet reader
	ARRAY_BUFFER,      // array buffer, holds a single flatvector child
	SHREDDED_BUFFER,   // holds data for a shredded variant vector
	SEQUENCE_BUFFER    // holds a linear numeric sequence (start, increment)
};

struct AuxiliaryDataHolder {
	virtual ~AuxiliaryDataHolder() = default;
};

class PinnedBufferHolder : public AuxiliaryDataHolder {
public:
	explicit PinnedBufferHolder(BufferHandle handle);
	~PinnedBufferHolder() override;

private:
	BufferHandle handle;
};

class VectorBufferHolder : public AuxiliaryDataHolder {
public:
	explicit VectorBufferHolder(buffer_ptr<VectorBuffer> buffer) : vector_buffer(std::move(buffer)) {
	}

private:
	buffer_ptr<VectorBuffer> vector_buffer;
};

//! The VectorBuffer is a class used by the vector to hold its data
class VectorBuffer {
public:
	explicit VectorBuffer(VectorBufferType type) : buffer_type(type) {
	}
	virtual ~VectorBuffer() {
	}

public:
	virtual data_ptr_t GetData() {
		return nullptr;
	}

	void AddAuxiliaryData(unique_ptr<AuxiliaryDataHolder> aux_data_p) {
		auxiliary_data.push_back(std::move(aux_data_p));
	}
	vector<unique_ptr<AuxiliaryDataHolder>> &GetAuxiliaryDataMutable() {
		return auxiliary_data;
	}
	virtual void ClearAuxiliaryData() {
		auxiliary_data.clear();
	}

	virtual optional_ptr<Allocator> GetAllocator() const {
		return nullptr;
	}

	static buffer_ptr<VectorBuffer> CreateStandardVector(PhysicalType type, idx_t capacity = STANDARD_VECTOR_SIZE);
	static buffer_ptr<VectorBuffer> CreateConstantVector(PhysicalType type);
	static buffer_ptr<VectorBuffer> CreateConstantVector(const LogicalType &logical_type);
	static buffer_ptr<VectorBuffer> CreateStandardVector(const LogicalType &logical_type,
	                                                     idx_t capacity = STANDARD_VECTOR_SIZE);

	inline VectorBufferType GetBufferType() const {
		return buffer_type;
	}

protected:
	VectorBufferType buffer_type;
	vector<unique_ptr<AuxiliaryDataHolder>> auxiliary_data;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
