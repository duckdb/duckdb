#pragma once

#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

struct VariantShredding {
public:
	VariantShredding() {
	}
	virtual ~VariantShredding() = default;

public:
	virtual void WriteVariantValues(UnifiedVariantVectorData &variant, Vector &result,
	                                optional_ptr<const SelectionVector> sel,
	                                optional_ptr<const SelectionVector> value_index_sel,
	                                optional_ptr<const SelectionVector> result_sel, idx_t count) = 0;

protected:
	void WriteTypedValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
	                      const SelectionVector &value_index_sel, const SelectionVector &result_sel, idx_t count);

private:
	void WriteTypedObjectValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
	                            const SelectionVector &value_index_sel, const SelectionVector &result_sel, idx_t count);
	void WriteTypedArrayValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
	                           const SelectionVector &value_index_sel, const SelectionVector &result_sel, idx_t count);
	void WriteTypedPrimitiveValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
	                               const SelectionVector &value_index_sel, const SelectionVector &result_sel,
	                               idx_t count);
};

struct VariantShreddingState {
public:
	explicit VariantShreddingState(const LogicalType &type, idx_t total_count);

public:
	bool ValueIsShredded(UnifiedVariantVectorData &variant, idx_t row, idx_t values_index);
	void SetShredded(idx_t row, idx_t values_index, idx_t result_idx);
	case_insensitive_string_set_t ObjectFields();
	virtual const unordered_set<VariantLogicalType> &GetVariantTypes() = 0;

public:
	//! The type the field is shredded on
	const LogicalType &type;
	//! row that is shredded
	SelectionVector shredded_sel;
	//! 'values_index' of the shredded value
	SelectionVector values_index_sel;
	//! result row of the shredded value
	SelectionVector result_sel;
	//! The amount of rows that are shredded on
	idx_t count = 0;
};

} // namespace duckdb
