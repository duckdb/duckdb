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
#include "duckdb/common/enums/vector_type.hpp"

namespace duckdb {

class BufferHandle;
struct LogicalType;
struct ResizeInfo;
struct UnifiedVectorFormat;
class VectorBuffer;
class Vector;
struct ValidityMask;

enum class VectorBufferType : uint8_t {
	STANDARD_BUFFER,   // VectorType::FLAT/CONSTANT - Fixed-Size Type - Holds a single array of data
	STRING_BUFFER,     // VectorType::FLAT/CONSTANT - String          - Holds string_t array and StringHeap
	STRUCT_BUFFER,     // VectorType::FLAT/CONSTANT - Struct          - Holds struct child vectors
	LIST_BUFFER,       // VectorType::FLAT/CONSTANT - List            - Holds list_entry_t array and list child vector
	ARRAY_BUFFER,      // VectorType::FLAT/CONSTANT - Array           - Holds array child vector
	DICTIONARY_BUFFER, // VectorType::DICTIONARY    - Any             - Holds SelectionVector and dict child vector
	FSST_BUFFER,       // VectorType::FSST          - String          - Holds string_t array, StringHeap and FSST table
	SHREDDED_BUFFER,   // VectorType::SHREDDED      - Variant         - Holds shredded variant
	SEQUENCE_BUFFER    // VectorType::SEQUENCE      - Any             - Holds linear numeric sequence (start, increment)
};

struct AuxiliaryDataHolder {
	virtual ~AuxiliaryDataHolder() = default;

	virtual idx_t GetAllocationSize() const {
		return 0;
	}
};

struct AuxiliaryDataSet {
	vector<unique_ptr<AuxiliaryDataHolder>> data;
};

class PinnedBufferHolder : public AuxiliaryDataHolder {
public:
	explicit PinnedBufferHolder(BufferHandle handle);
	~PinnedBufferHolder() override;

private:
	BufferHandle handle;
};

class AuxiliaryDataSetHolder : public AuxiliaryDataHolder {
public:
	explicit AuxiliaryDataSetHolder(buffer_ptr<AuxiliaryDataSet> buffer) : auxiliary_data(std::move(buffer)) {
	}

private:
	buffer_ptr<AuxiliaryDataSet> auxiliary_data;
};

//! The VectorBuffer is a class used by the vector to hold its data
class VectorBuffer {
public:
	explicit VectorBuffer(VectorType vector_type, VectorBufferType type) : vector_type(vector_type), buffer_type(type) {
	}
	virtual ~VectorBuffer() {
	}

public:
	virtual data_ptr_t GetData() {
		return nullptr;
	}
	virtual ValidityMask &GetValidityMask() {
		throw InternalException("VectorBuffer does not have a ValidityMask");
	}

	void AddAuxiliaryData(unique_ptr<AuxiliaryDataHolder> aux_data_p) {
		if (!auxiliary_data) {
			auxiliary_data = make_buffer<AuxiliaryDataSet>();
		}
		auxiliary_data->data.push_back(std::move(aux_data_p));
	}
	buffer_ptr<AuxiliaryDataSet> &GetAuxiliaryData() {
		return auxiliary_data;
	}
	virtual void ClearAuxiliaryData() {
		auxiliary_data.reset();
	}

	virtual optional_ptr<Allocator> GetAllocator() const {
		return nullptr;
	}

	//! Flatten the vector buffer, converting it to a FLAT_VECTOR
	//! The selection vector maps output indices to source indices in this buffer
	//! Returns a new buffer, or nullptr if already flat with an unset selection vector
	virtual buffer_ptr<VectorBuffer> Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count);

	static buffer_ptr<VectorBuffer> CreateStandardVector(PhysicalType type, idx_t capacity = STANDARD_VECTOR_SIZE);
	static buffer_ptr<VectorBuffer> CreateConstantVector(PhysicalType type);
	static buffer_ptr<VectorBuffer> CreateConstantVector(const LogicalType &logical_type);
	static buffer_ptr<VectorBuffer> CreateStandardVector(const LogicalType &logical_type,
	                                                     idx_t capacity = STANDARD_VECTOR_SIZE);

	inline VectorType GetVectorType() const {
		return vector_type;
	}
	virtual void SetVectorType(VectorType vector_type);
	//! Set only this buffer's vector type without propagating to children (for struct/array buffers)
	void SetVectorTypeOnly(VectorType new_vector_type) {
		vector_type = new_vector_type;
	}

	inline VectorBufferType GetBufferType() const {
		return buffer_type;
	}

public:
	virtual idx_t GetAllocationSize() const;
	virtual void Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const;
	//! Get the value at the given index directly from the buffer's data
	virtual Value GetValue(const LogicalType &type, idx_t index) const;
	//! Set the value at the given index (flat/constant vectors only)
	virtual void SetValue(const LogicalType &type, idx_t index, const Value &val);
	//! Produce a string representation of buffer contents (debug only)
	virtual string ToString(const LogicalType &type, idx_t count) const;
	virtual string ToString(const LogicalType &type) const;
	//! Slice the buffer with a selection vector, returning a new buffer
	virtual buffer_ptr<VectorBuffer> Slice(const LogicalType &type, const SelectionVector &sel, idx_t count);
	//! Slice the buffer with an offset range, returning a new buffer
	virtual buffer_ptr<VectorBuffer> Slice(const LogicalType &type, const VectorBuffer &source, idx_t offset,
	                                       idx_t end);
	//! Create a UnifiedVectorFormat from the buffer's data
	virtual void ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const;
	//! Collect resize information for this buffer and children
	virtual void FindResizeInfos(Vector &vector, duckdb::vector<ResizeInfo> &resize_infos, idx_t multiplier);
	//! Resize the buffer's data allocation
	virtual void Resize(Vector &vector, idx_t current_size, idx_t new_size);

protected:
	VectorType vector_type;
	VectorBufferType buffer_type;
	buffer_ptr<AuxiliaryDataSet> auxiliary_data;

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
