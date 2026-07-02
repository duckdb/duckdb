#include "duckdb/common/types/selection_vector.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/types/bitmap_selection_vector.hpp"

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace duckdb {

idx_t SelectionVector::IntersectBitmap(const SelectionVector &other) {
	D_ASSERT(IsBitmap() && other.IsBitmap());
	D_ASSERT(RowSpan() == other.RowSpan());
	auto a = reinterpret_cast<validity_t *>(selection_data->bitmap_data.get());
	auto b = reinterpret_cast<const validity_t *>(other.selection_data->bitmap_data.get());
	const idx_t nwords = (selection_data->row_span + 63) / 64;
	idx_t total = 0;
	for (idx_t w = 0; w < nwords; w++) {
		a[w] &= b[w];
#if defined(_MSC_VER)
		total += idx_t(__popcnt64(a[w]));
#else
		total += idx_t(__builtin_popcountll(a[w]));
#endif
	}
	return total;
}

void SelectionVector::Flatten() const {
	if (sel_vector || !selection_data || !selection_data->is_bitmap) {
		return; // already materialized, or not bitmap-backed
	}
	// Keep the bitmap alive: BitmapToSelectionVector re-Initializes *this with a fresh index buffer,
	// replacing selection_data; `keep` holds the bitmap data referenced by `bm` during conversion.
	auto keep = selection_data;
	auto bm = reinterpret_cast<const validity_t *>(keep->bitmap_data.get());
	BitmapToSelectionVector(bm, keep->row_span, const_cast<SelectionVector &>(*this));
}

SelectionData::SelectionData(idx_t count) {
	owned_data = Allocator::DefaultAllocator().Allocate(MaxValue<idx_t>(count, 1) * sizeof(sel_t));
#ifdef DEBUG
	auto data_ptr = reinterpret_cast<sel_t *>(owned_data.get());
	for (idx_t i = 0; i < count; i++) {
		data_ptr[i] = std::numeric_limits<sel_t>::max();
	}
#endif
}

// LCOV_EXCL_START
string SelectionVector::ToString(idx_t count) const {
	string result = "Selection Vector (" + to_string(count) + ") [";
	for (idx_t i = 0; i < count; i++) {
		if (i != 0) {
			result += ", ";
		}
		result += to_string(get_index(i));
	}
	result += "]";
	return result;
}

void SelectionVector::Sort(idx_t count) {
	std::sort(data(), data() + count);
}

void SelectionVector::Print(idx_t count) const {
	Printer::Print(ToString(count));
}
// LCOV_EXCL_STOP

buffer_ptr<SelectionData> SelectionVector::Slice(const SelectionVector &sel, idx_t count) const {
	auto data = make_buffer<SelectionData>(count);
	auto result_ptr = reinterpret_cast<sel_t *>(data->owned_data.get());
	// for every element, we perform result[i] = target[new[i]]
	for (idx_t i = 0; i < count; i++) {
		auto new_idx = sel.get_index(i);
		auto idx = this->get_index(new_idx);
		result_ptr[i] = UnsafeNumericCast<sel_t>(idx);
	}
	return data;
}

idx_t SelectionVector::SliceInPlace(const SelectionVector &source, idx_t count) {
	for (idx_t i = 0; i < count; ++i) {
		set_index(i, get_index(source.get_index(i)));
	}

	return count;
}

void SelectionVector::Verify(idx_t count, idx_t vector_size) const {
#ifdef DEBUG
	D_ASSERT(vector_size >= 1);
	for (idx_t i = 0; i < count; i++) {
		auto index = get_index(i);
		if (index >= vector_size) {
			throw InternalException(
			    "Provided SelectionVector is invalid, index %d points to %d, which is out of range. "
			    "the valid range (0-%d)",
			    i, index, vector_size - 1);
		}
	}
#endif
}

idx_t SelectionVector::GetAllocationSize() const {
	if (!selection_data) {
		return 0;
	}
	return selection_data->owned_data.GetSize();
}

} // namespace duckdb
