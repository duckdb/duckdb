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
	STANDARD_BUFFER,     // standard buffer, holds a single array of data
	DICTIONARY_BUFFER,   // dictionary buffer, holds a selection vector
	VECTOR_CHILD_BUFFER, // vector child buffer: holds another vector
	STRING_BUFFER,       // string buffer, holds a string heap
	FSST_BUFFER,         // fsst compressed string buffer, holds a string heap, fsst symbol table and a string count
	STRUCT_BUFFER,       // struct buffer, holds a ordered mapping from name to child vector
	LIST_BUFFER,         // list buffer, holds a single flatvector child
	MANAGED_BUFFER,      // managed buffer, holds a buffer managed by the buffermanager
	OPAQUE_BUFFER,       // opaque buffer, can be created for example by the parquet reader
	ARRAY_BUFFER,        // array buffer, holds a single flatvector child
	SHREDDED_BUFFER,     // holds data for a shredded variant vector
	SEQUENCE_BUFFER      // holds a linear numeric sequence (start, increment)
};

enum class VectorAuxiliaryDataType : uint8_t {
	ARROW_AUXILIARY // Holds Arrow Chunks that this vector depends on
};

struct VectorAuxiliaryData {
	explicit VectorAuxiliaryData(VectorAuxiliaryDataType type_p)
	    : type(type_p) {

	      };
	VectorAuxiliaryDataType type;

	virtual ~VectorAuxiliaryData() {
	}

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast vector auxiliary data to type - type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast vector auxiliary data to type - type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

//! The VectorBuffer is a class used by the vector to hold its data
class VectorBuffer {
public:
	explicit VectorBuffer(VectorBufferType type) : buffer_type(type), data_ptr(nullptr) {
	}
	explicit VectorBuffer(idx_t data_size) : buffer_type(VectorBufferType::STANDARD_BUFFER), data_ptr(nullptr) {
		if (data_size > 0) {
			allocated_data = Allocator::DefaultAllocator().Allocate(data_size);
			data_ptr = allocated_data.get();
		}
	}
	explicit VectorBuffer(data_ptr_t data_ptr_p)
	    : buffer_type(VectorBufferType::STANDARD_BUFFER), data_ptr(data_ptr_p) {
	}
	explicit VectorBuffer(AllocatedData &&data_p)
	    : buffer_type(VectorBufferType::STANDARD_BUFFER), allocated_data(std::move(data_p)) {
		data_ptr = allocated_data.get();
	}
	virtual ~VectorBuffer() {
	}
	VectorBuffer() {
	}

public:
	data_ptr_t GetData() {
		return data_ptr;
	}

	void SetData(AllocatedData &&new_data) {
		allocated_data = std::move(new_data);
		data_ptr = allocated_data.get();
	}

	void SetData(data_ptr_t data) {
		data_ptr = data;
		allocated_data.Reset();
	}

	VectorAuxiliaryData *GetAuxiliaryData() {
		return aux_data.get();
	}

	void SetAuxiliaryData(unique_ptr<VectorAuxiliaryData> aux_data_p) {
		aux_data = std::move(aux_data_p);
	}

	void MoveAuxiliaryData(VectorBuffer &source_buffer) {
		SetAuxiliaryData(std::move(source_buffer.aux_data));
	}

	virtual optional_ptr<Allocator> GetAllocator() const {
		return allocated_data.GetAllocator();
	}

	static buffer_ptr<VectorBuffer> CreateStandardVector(PhysicalType type, idx_t capacity = STANDARD_VECTOR_SIZE);
	static buffer_ptr<VectorBuffer> CreateConstantVector(PhysicalType type);
	static buffer_ptr<VectorBuffer> CreateConstantVector(const LogicalType &logical_type);
	static buffer_ptr<VectorBuffer> CreateStandardVector(const LogicalType &logical_type,
	                                                     idx_t capacity = STANDARD_VECTOR_SIZE);

	inline VectorBufferType GetBufferType() const {
		return buffer_type;
	}

	inline VectorAuxiliaryDataType GetAuxiliaryDataType() const {
		return aux_data->type;
	}

protected:
	VectorBufferType buffer_type;
	unique_ptr<VectorAuxiliaryData> aux_data;
	data_ptr_t data_ptr;
	AllocatedData allocated_data;

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

//! The ManagedVectorBuffer holds a buffer handle
class ManagedVectorBuffer : public VectorBuffer {
public:
	explicit ManagedVectorBuffer(BufferHandle handle);
	~ManagedVectorBuffer() override;

private:
	BufferHandle handle;
};

//! The DictionaryBuffer holds a selection vector
class ShreddedVectorBuffer : public VectorBuffer {
public:
	explicit ShreddedVectorBuffer(Vector &shredded_data);
	~ShreddedVectorBuffer() override;

public:
	Vector &GetChild() {
		return *shredded_data;
	}

private:
	unique_ptr<Vector> shredded_data;
};
} // namespace duckdb
