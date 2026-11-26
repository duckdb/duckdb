#include "duckdb/common/vector_operations/string_comparison.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"

namespace duckdb {

namespace {

struct StringComparisonSample {
	idx_t inlined = 0;
	idx_t shared_prefix = 0;
	idx_t different_suffix = 0;

	void Add(const string_t &left, const string_t &right) {
#ifndef DUCKDB_DEBUG_NO_INLINE
		inlined += left.IsInlined() && right.IsInlined();
		if (left.GetPrefixIntegerComparable() == right.GetPrefixIntegerComparable()) {
			shared_prefix++;
			different_suffix += Load<uint64_t>(const_data_ptr_cast(left.GetData()) + string_t::PREFIX_BYTES) !=
			                    Load<uint64_t>(const_data_ptr_cast(right.GetData()) + string_t::PREFIX_BYTES);
		}
#endif
	}
};

template <class SAMPLE>
StringComparisonMode SampleStringComparison(idx_t count, SAMPLE &&sample) {
#ifdef DUCKDB_DEBUG_NO_INLINE
	return StringComparisonMode::GENERIC;
#else
	if (count < 128) {
		return StringComparisonMode::INLINED;
	}
	StringComparisonSample stats;
	for (idx_t i = 0; i < 8; i++) {
		// Scatter the samples to avoid aliasing with power-of-two input periods.
		const auto row = ((i * 257 + 37) % 2048) * count / 2048;
		sample(row, stats);
	}
	// Samples predict profitability; every kernel must handle arbitrary unsampled values.
	if (stats.inlined >= 7) {
		return StringComparisonMode::INLINED;
	}
	if (stats.shared_prefix && stats.different_suffix * 2 >= stats.shared_prefix) {
		return StringComparisonMode::PREFIX;
	}
	return StringComparisonMode::GENERIC;
#endif
}

} // namespace

bool StringComparison::HasInlinedConstant(const Vector &left, const Vector &right) {
	for (auto &input : {std::cref(left), std::cref(right)}) {
		auto &constant = input.get();
		if (constant.GetVectorType() == VectorType::CONSTANT_VECTOR && !ConstantVector::IsNull(constant) &&
		    ConstantVector::GetData<string_t>(constant)->IsInlined()) {
			return true;
		}
	}
	return false;
}

StringComparisonMode StringComparison::SelectMode(const Vector &left, const Vector &right, idx_t count) {
#ifdef DUCKDB_DEBUG_NO_INLINE
	return StringComparisonMode::GENERIC;
#else
	if (count < 128 ||
	    (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR)) {
		return StringComparisonMode::INLINED;
	}
	auto left_values = left.Values<string_t>();
	auto right_values = right.Values<string_t>();
	return SampleStringComparison(count, [&](idx_t row, StringComparisonSample &stats) {
		auto l = left_values[row];
		auto r = right_values[row];
		if (l.IsValid() && r.IsValid()) {
			stats.Add(l.GetValue(), r.GetValue());
		}
	});
#endif
}

StringComparisonMode StringComparison::SelectMode(const UnifiedVectorFormat &input, const string_t &constant,
                                                  const SelectionVector &sel, idx_t count) {
	if (input.sel->data() == ConstantVector::ZERO_VECTOR) {
		return StringComparisonMode::INLINED;
	}
	auto data = UnifiedVectorFormat::GetData<string_t>(input);
	return SampleStringComparison(count, [&](idx_t row, StringComparisonSample &stats) {
		const auto idx = input.sel->get_index(sel.get_index(row));
		if (input.validity.RowIsValid(idx)) {
			stats.Add(data[idx], constant);
		}
	});
}

} // namespace duckdb
