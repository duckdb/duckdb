#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

void UnifiedVectorFormat::ThrowVectorTypeMismatch(PhysicalType expected) const {
	throw InternalException("Expected unified vector format of type %s, but found type %s", expected, physical_type);
}

UnifiedVectorFormat::UnifiedVectorFormat() : sel(nullptr), data(nullptr), physical_type(PhysicalType::INVALID) {
}

void UnifiedVectorFormat::Borrow(const Vector &input, const Vector &result) {
	const auto is_dictionary = input.GetVectorType() == VectorType::DICTIONARY_VECTOR;
	const auto &source = is_dictionary ? DictionaryVector::Child(input) : input;
	if (source.GetVectorType() != VectorType::FLAT_VECTOR ||
	    input.GetBufferRef().get() == result.GetBufferRef().get() ||
	    source.GetBufferRef().get() == result.GetBufferRef().get()) {
		input.ToUnifiedFormat(*this);
		return;
	}
	physical_type = input.GetType().InternalType();
	data = FlatVector::GetData(source);
	const auto &source_validity = FlatVector::Validity(source);
	validity.Initialize(source_validity.GetData(), source_validity.Capacity());
	owned_sel = SelectionVector();
	sel = is_dictionary ? &DictionaryVector::SelVector(input) : FlatVector::IncrementalSelectionVector();
}

UnifiedVectorFormat::UnifiedVectorFormat(UnifiedVectorFormat &&other) noexcept
    : sel(nullptr), data(nullptr), physical_type(PhysicalType::INVALID) {
	bool refers_to_self = other.sel == &other.owned_sel;
	std::swap(sel, other.sel);
	std::swap(data, other.data);
	std::swap(validity, other.validity);
	std::swap(owned_sel, other.owned_sel);
	std::swap(physical_type, other.physical_type);
	if (refers_to_self) {
		sel = &owned_sel;
	}
}

UnifiedVectorFormat &UnifiedVectorFormat::operator=(UnifiedVectorFormat &&other) noexcept {
	bool refers_to_self = other.sel == &other.owned_sel;
	std::swap(sel, other.sel);
	std::swap(data, other.data);
	std::swap(validity, other.validity);
	std::swap(owned_sel, other.owned_sel);
	std::swap(physical_type, other.physical_type);
	if (refers_to_self) {
		sel = &owned_sel;
	}
	return *this;
}

} // namespace duckdb
