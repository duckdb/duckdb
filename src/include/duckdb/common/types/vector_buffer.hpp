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
	ARRAY_BUFFER         // array buffer, holds a single flatvector child
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
	explicit VectorBuffer(VectorBufferType type) : buffer_type(type) {
	}
	explicit VectorBuffer(idx_t data_size) : buffer_type(VectorBufferType::STANDARD_BUFFER) {
		if (data_size > 0) {
			data = make_unsafe_uniq_array<data_t>(data_size);
		}
	}
	explicit VectorBuffer(unsafe_unique_array<data_t> data_p)
	    : buffer_type(VectorBufferType::STANDARD_BUFFER), data(std::move(data_p)) {
	}
	virtual ~VectorBuffer() {
	}
	VectorBuffer() {
	}

public:
	data_ptr_t GetData() {
		return data.get();
	}

	void SetData(unsafe_unique_array<data_t> new_data) {
		data = std::move(new_data);
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
	unsafe_unique_array<data_t> data;

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

//! The DictionaryBuffer holds a selection vector
class DictionaryBuffer : public VectorBuffer {
public:
	explicit DictionaryBuffer(const SelectionVector &sel)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(sel) {
	}
	explicit DictionaryBuffer(buffer_ptr<SelectionData> data)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(std::move(data)) {
	}
	explicit DictionaryBuffer(idx_t count = STANDARD_VECTOR_SIZE)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(count) {
	}

public:
	const SelectionVector &GetSelVector() const {
		return sel_vector;
	}
	SelectionVector &GetSelVector() {
		return sel_vector;
	}
	void SetSelVector(const SelectionVector &vector) {
		this->sel_vector.Initialize(vector);
	}

private:
	SelectionVector sel_vector;
};

class VectorStringBuffer : public VectorBuffer {
public:
	VectorStringBuffer();
	explicit VectorStringBuffer(VectorBufferType type);

public:
	string_t AddString(const char *data, idx_t len) {
		return heap.AddString(data, len);
	}
	string_t AddString(string_t data) {
		return heap.AddString(data);
	}
	string_t AddBlob(string_t data) {
		return heap.AddBlob(data.GetData(), data.GetSize());
	}
	string_t EmptyString(idx_t len) {
		return heap.EmptyString(len);
	}

	void AddHeapReference(buffer_ptr<VectorBuffer> heap) {
		references.push_back(std::move(heap));
	}

private:
	//! The string heap of this buffer
	StringHeap heap;
	// References to additional vector buffers referenced by this string buffer
	vector<buffer_ptr<VectorBuffer>> references;
};

class VectorFSSTStringBuffer : public VectorStringBuffer {
public:
	VectorFSSTStringBuffer();

public:
	void AddDecoder(buffer_ptr<void> &duckdb_fsst_decoder_p, const idx_t string_block_limit) {
		duckdb_fsst_decoder = duckdb_fsst_decoder_p;
		decompress_buffer.resize(string_block_limit + 1);
	}
	void *GetDecoder() {
		return duckdb_fsst_decoder.get();
	}
	vector<unsigned char> &GetDecompressBuffer() {
		return decompress_buffer;
	}
	void SetCount(idx_t count) {
		total_string_count = count;
	}
	idx_t GetCount() {
		return total_string_count;
	}

private:
	buffer_ptr<void> duckdb_fsst_decoder;
	idx_t total_string_count = 0;
	vector<unsigned char> decompress_buffer;
};

class VectorStructBuffer : public VectorBuffer {
public:
	VectorStructBuffer();
	explicit VectorStructBuffer(const LogicalType &struct_type, idx_t capacity = STANDARD_VECTOR_SIZE);
	VectorStructBuffer(Vector &other, const SelectionVector &sel, idx_t count);
	~VectorStructBuffer() override;

public:
	const vector<unique_ptr<Vector>> &GetChildren() const {
		return children;
	}
	vector<unique_ptr<Vector>> &GetChildren() {
		return children;
	}

private:
	//! child vectors used for nested data
	vector<unique_ptr<Vector>> children;
};

class VectorListBuffer : public VectorBuffer {
public:
	explicit VectorListBuffer(unique_ptr<Vector> vector, idx_t initial_capacity = STANDARD_VECTOR_SIZE);
	explicit VectorListBuffer(const LogicalType &list_type, idx_t initial_capacity = STANDARD_VECTOR_SIZE);
	~VectorListBuffer() override;

public:
	Vector &GetChild() {
		return *child;
	}
	void Reserve(idx_t to_reserve);

	void Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset = 0);
	void Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size, idx_t source_offset = 0);

	void PushBack(const Value &insert);

	idx_t GetSize() {
		return size;
	}

	idx_t GetCapacity() {
		return capacity;
	}

	void SetCapacity(idx_t new_capacity);
	void SetSize(idx_t new_size);

private:
	//! child vectors used for nested data
	unique_ptr<Vector> child;
	idx_t capacity = 0;
	idx_t size = 0;
};

class VectorArrayBuffer : public VectorBuffer {
public:
	explicit VectorArrayBuffer(unique_ptr<Vector> child_vector, idx_t array_size, idx_t initial_capacity);
	explicit VectorArrayBuffer(const LogicalType &array, idx_t initial = STANDARD_VECTOR_SIZE);
	~VectorArrayBuffer() override;

public:
	Vector &GetChild();
	idx_t GetArraySize();
	idx_t GetChildSize();

private:
	unique_ptr<Vector> child;
	// The size of each array in this buffer
	idx_t array_size = 0;
	// How many arrays are currently stored in this buffer
	// The child vector has size (array_size * size)
	idx_t size = 0;
};

//! The ManagedVectorBuffer holds a buffer handle
class ManagedVectorBuffer : public VectorBuffer {
public:
	explicit ManagedVectorBuffer(BufferHandle handle);
	~ManagedVectorBuffer() override;

private:
	BufferHandle handle;
};

} // namespace duckdb
