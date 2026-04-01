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

struct AuxiliaryData {
	vector<unique_ptr<AuxiliaryDataHolder>> data;
};

class AuxiliaryDataSetHolder : public AuxiliaryDataHolder {
public:
	explicit AuxiliaryDataSetHolder(buffer_ptr<AuxiliaryData> data) : auxiliary_data(std::move(data)) {
	}

private:
	buffer_ptr<AuxiliaryData> auxiliary_data;
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
		if (!auxiliary_data) {
			auxiliary_data = make_buffer<AuxiliaryData>();
		}
		auxiliary_data->data.push_back(std::move(aux_data_p));
	}
	buffer_ptr<AuxiliaryData> GetAuxiliaryData() {
		return auxiliary_data;
	}
	virtual void ClearAuxiliaryData() {
		auxiliary_data.reset();
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
	buffer_ptr<AuxiliaryData> auxiliary_data;

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

class StandardVectorBuffer : public VectorBuffer {
public:
	StandardVectorBuffer(Allocator &allocator, idx_t data_size)
	    : VectorBuffer(VectorBufferType::STANDARD_BUFFER), data_ptr(nullptr) {
		if (data_size > 0) {
			allocated_data = allocator.Allocate(data_size);
			data_ptr = allocated_data.get();
		}
	}
	explicit StandardVectorBuffer(idx_t data_size) : StandardVectorBuffer(Allocator::DefaultAllocator(), data_size) {
	}
	explicit StandardVectorBuffer(data_ptr_t data_ptr_p)
	    : VectorBuffer(VectorBufferType::STANDARD_BUFFER), data_ptr(data_ptr_p) {
	}
	explicit StandardVectorBuffer(AllocatedData &&data_p)
	    : VectorBuffer(VectorBufferType::STANDARD_BUFFER), allocated_data(std::move(data_p)) {
		data_ptr = allocated_data.get();
	}

public:
	data_ptr_t GetData() override {
		return data_ptr;
	}

	optional_ptr<Allocator> GetAllocator() const override {
		return allocated_data.GetAllocator();
	}

protected:
	data_ptr_t data_ptr;
	AllocatedData allocated_data;
};

} // namespace duckdb
