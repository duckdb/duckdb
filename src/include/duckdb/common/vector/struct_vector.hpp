//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/struct_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

class VectorStructBuffer : public VectorBuffer {
public:
	VectorStructBuffer();
	explicit VectorStructBuffer(const LogicalType &struct_type, idx_t capacity = STANDARD_VECTOR_SIZE);
	VectorStructBuffer(Vector &other, const SelectionVector &sel, idx_t count);
	~VectorStructBuffer() override;

public:
	ValidityMask &GetValidityMask() override {
		return validity;
	}
	const vector<Vector> &GetChildren() const {
		return children;
	}
	vector<Vector> &GetChildren() {
		return children;
	}
	void SetVectorType(VectorType vector_type) override;
	buffer_ptr<VectorBuffer> Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) override;

public:
	idx_t GetAllocationSize() const override;
	buffer_ptr<VectorBuffer> Slice(const LogicalType &type, const VectorBuffer &source, idx_t offset, idx_t end) override;
	buffer_ptr<VectorBuffer> Slice(const SelectionVector &sel, idx_t count) override;
	void FindResizeInfos(Vector &vector, duckdb::vector<ResizeInfo> &resize_infos, idx_t multiplier) override;
	void ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const override;
	Value GetValue(const LogicalType &type, idx_t index) const override;
	void SetValue(const LogicalType &type, idx_t index, const Value &val) override;
	void Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;

private:
	ValidityMask validity;
	//! child vectors used for nested data
	vector<Vector> children;
};

struct StructVector {
	DUCKDB_API static const vector<Vector> &GetEntries(const Vector &vector);
	DUCKDB_API static vector<Vector> &GetEntries(Vector &vector);
};

} // namespace duckdb
