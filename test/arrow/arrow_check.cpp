#include "arrow_check.hpp"
#include "arrow/util/bitmap_ops.h"
#include "arrow/visitor_inline.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/bit_run_reader.h"
using arrow::internal::BitmapEquals;
using arrow::internal::BitmapUInt64Reader;
using arrow::internal::OptionalBitmapEquals;

using namespace arrow;
bool CompareArrayRanges(const ArrayData &left, const ArrayData &right, int64_t left_start_idx, int64_t left_end_idx,
                        int64_t right_start_idx, const EqualOptions &options, bool floating_approximate);
enum FloatingEqualityFlags : int8_t { Approximate = 1, NansEqual = 2 };

template <typename T, int8_t Flags>
struct FloatingEquality {
	bool operator()(T x, T y) {
		return x == y;
	}
};

template <typename T>
struct FloatingEquality<T, NansEqual> {
	bool operator()(T x, T y) {
		return (x == y) || (std::isnan(x) && std::isnan(y));
	}
};

template <typename T>
struct FloatingEquality<T, Approximate> {
	explicit FloatingEquality(const EqualOptions &options) : epsilon(static_cast<T>(options.atol())) {
	}

	bool operator()(T x, T y) {
		return (fabs(x - y) <= epsilon) || (x == y);
	}

	const T epsilon;
};

template <typename T>
struct FloatingEquality<T, Approximate | NansEqual> {
	explicit FloatingEquality(const EqualOptions &options) : epsilon(static_cast<T>(options.atol())) {
	}

	bool operator()(T x, T y) {
		return (fabs(x - y) <= epsilon) || (x == y) || (std::isnan(x) && std::isnan(y));
	}

	const T epsilon;
};

template <typename T, typename Visitor>
void VisitFloatingEquality(const EqualOptions &options, bool floating_approximate, Visitor &&visit) {
	if (options.nans_equal()) {
		if (floating_approximate) {
			visit(FloatingEquality<T, NansEqual | Approximate> {options});
		} else {
			visit(FloatingEquality<T, NansEqual> {});
		}
	} else {
		if (floating_approximate) {
			visit(FloatingEquality<T, Approximate> {options});
		} else {
			visit(FloatingEquality<T, 0> {});
		}
	}
}

class RangeDataEqualsImpl {
public:
	// PRE-CONDITIONS:
	// - the types are equal
	// - the ranges are in bounds
	RangeDataEqualsImpl(const EqualOptions &options, bool floating_approximate, const ArrayData &left,
	                    const ArrayData &right, int64_t left_start_idx, int64_t right_start_idx, int64_t range_length)
	    : options_(options), floating_approximate_(floating_approximate), left_(left), right_(right),
	      left_start_idx_(left_start_idx), right_start_idx_(right_start_idx), range_length_(range_length),
	      result_(false) {
	}

	bool Compare() {
		// Compare null bitmaps
		if (left_start_idx_ == 0 && right_start_idx_ == 0 && range_length_ == left_.length &&
		    range_length_ == right_.length) {
			// If we're comparing entire arrays, we can first compare the cached null counts
			if (left_.GetNullCount() != right_.GetNullCount()) {
				return false;
			}
		}
		if (!OptionalBitmapEquals(left_.buffers[0], left_.offset + left_start_idx_, right_.buffers[0],
		                          right_.offset + right_start_idx_, range_length_)) {
			return false;
		}
		// Compare values
		return CompareWithType(*left_.type);
	}

	bool CompareWithType(const DataType &type) {
		result_ = true;
		if (range_length_ != 0) {
			VisitTypeInline(type, this);
		}
		return result_;
	}

	Status Visit(const NullType &) {
		return Status::OK();
	}

	template <typename TypeClass>
	enable_if_primitive_ctype<TypeClass, Status> Visit(const TypeClass &type) {
		return ComparePrimitive(type);
	}

	template <typename TypeClass>
	enable_if_t<is_temporal_type<TypeClass>::value, Status> Visit(const TypeClass &type) {
		return ComparePrimitive(type);
	}

	Status Visit(const BooleanType &) {
		const uint8_t *left_bits = left_.GetValues<uint8_t>(1, 0);
		const uint8_t *right_bits = right_.GetValues<uint8_t>(1, 0);
		auto compare_runs = [&](int64_t i, int64_t length) -> bool {
			if (length <= 8) {
				// Avoid the BitmapUInt64Reader overhead for very small runs
				for (int64_t j = i; j < i + length; ++j) {
					if (BitUtil::GetBit(left_bits, left_start_idx_ + left_.offset + j) !=
					    BitUtil::GetBit(right_bits, right_start_idx_ + right_.offset + j)) {
						return false;
					}
				}
				return true;
			} else if (length <= 1024) {
				BitmapUInt64Reader left_reader(left_bits, left_start_idx_ + left_.offset + i, length);
				BitmapUInt64Reader right_reader(right_bits, right_start_idx_ + right_.offset + i, length);
				while (left_reader.position() < length) {
					if (left_reader.NextWord() != right_reader.NextWord()) {
						return false;
					}
				}
				//        DCHECK_EQ(right_reader.position(), length);
			} else {
				// BitmapEquals is the fastest method on large runs
				return BitmapEquals(left_bits, left_start_idx_ + left_.offset + i, right_bits,
				                    right_start_idx_ + right_.offset + i, length);
			}
			return true;
		};
		VisitValidRuns(compare_runs);
		return Status::OK();
	}

	Status Visit(const FloatType &type) {
		return CompareFloating(type);
	}

	Status Visit(const DoubleType &type) {
		return CompareFloating(type);
	}

	// Also matches StringType
	Status Visit(const BinaryType &type) {
		return CompareBinary(type);
	}

	// Also matches LargeStringType
	Status Visit(const LargeBinaryType &type) {
		return CompareBinary(type);
	}

	Status Visit(const FixedSizeBinaryType &type) {
		const auto byte_width = type.byte_width();
		const uint8_t *left_data = left_.GetValues<uint8_t>(1, 0);
		const uint8_t *right_data = right_.GetValues<uint8_t>(1, 0);

		if (left_data != nullptr && right_data != nullptr) {
			auto compare_runs = [&](int64_t i, int64_t length) -> bool {
				return memcmp(left_data + (left_start_idx_ + left_.offset + i) * byte_width,
				              right_data + (right_start_idx_ + right_.offset + i) * byte_width,
				              length * byte_width) == 0;
			};
			VisitValidRuns(compare_runs);
		} else {
			auto compare_runs = [&](int64_t i, int64_t length) -> bool {
				return true;
			};
			VisitValidRuns(compare_runs);
		}
		return Status::OK();
	}

	// Also matches MapType
	Status Visit(const ListType &type) {
		return CompareList(type);
	}

	Status Visit(const LargeListType &type) {
		return CompareList(type);
	}

	Status Visit(const FixedSizeListType &type) {
		const auto list_size = type.list_size();
		const ArrayData &left_data = *left_.child_data[0];
		const ArrayData &right_data = *right_.child_data[0];

		auto compare_runs = [&](int64_t i, int64_t length) -> bool {
			RangeDataEqualsImpl impl(options_, floating_approximate_, left_data, right_data,
			                         (left_start_idx_ + left_.offset + i) * list_size,
			                         (right_start_idx_ + right_.offset + i) * list_size, length * list_size);
			return impl.Compare();
		};
		VisitValidRuns(compare_runs);
		return Status::OK();
	}

	Status Visit(const StructType &type) {
		const int32_t num_fields = type.num_fields();

		auto compare_runs = [&](int64_t i, int64_t length) -> bool {
			for (int32_t f = 0; f < num_fields; ++f) {
				RangeDataEqualsImpl impl(options_, floating_approximate_, *left_.child_data[f], *right_.child_data[f],
				                         left_start_idx_ + left_.offset + i, right_start_idx_ + right_.offset + i,
				                         length);
				if (!impl.Compare()) {
					return false;
				}
			}
			return true;
		};
		VisitValidRuns(compare_runs);
		return Status::OK();
	}

	Status Visit(const SparseUnionType &type) {
		const auto &child_ids = type.child_ids();
		const int8_t *left_codes = left_.GetValues<int8_t>(1);
		const int8_t *right_codes = right_.GetValues<int8_t>(1);

		// Unions don't have a null bitmap
		for (int64_t i = 0; i < range_length_; ++i) {
			const auto type_id = left_codes[left_start_idx_ + i];
			if (type_id != right_codes[right_start_idx_ + i]) {
				result_ = false;
				break;
			}
			const auto child_num = child_ids[type_id];
			// XXX can we instead detect runs of same-child union values?
			RangeDataEqualsImpl impl(options_, floating_approximate_, *left_.child_data[child_num],
			                         *right_.child_data[child_num], left_start_idx_ + left_.offset + i,
			                         right_start_idx_ + right_.offset + i, 1);
			if (!impl.Compare()) {
				result_ = false;
				break;
			}
		}
		return Status::OK();
	}

	Status Visit(const DenseUnionType &type) {
		const auto &child_ids = type.child_ids();
		const int8_t *left_codes = left_.GetValues<int8_t>(1);
		const int8_t *right_codes = right_.GetValues<int8_t>(1);
		const int32_t *left_offsets = left_.GetValues<int32_t>(2);
		const int32_t *right_offsets = right_.GetValues<int32_t>(2);

		for (int64_t i = 0; i < range_length_; ++i) {
			const auto type_id = left_codes[left_start_idx_ + i];
			if (type_id != right_codes[right_start_idx_ + i]) {
				result_ = false;
				break;
			}
			const auto child_num = child_ids[type_id];
			RangeDataEqualsImpl impl(options_, floating_approximate_, *left_.child_data[child_num],
			                         *right_.child_data[child_num], left_offsets[left_start_idx_ + i],
			                         right_offsets[right_start_idx_ + i], 1);
			if (!impl.Compare()) {
				result_ = false;
				break;
			}
		}
		return Status::OK();
	}

	Status Visit(const DictionaryType &type) {
		// Compare dictionaries
		result_ &= CompareArrayRanges(*left_.dictionary, *right_.dictionary,
		                              /*left_start_idx=*/0,
		                              /*left_end_idx=*/std::max(left_.dictionary->length, right_.dictionary->length),
		                              /*right_start_idx=*/0, options_, floating_approximate_);
		if (result_) {
			// Compare indices
			result_ &= CompareWithType(*type.index_type());
		}
		return Status::OK();
	}

	Status Visit(const ExtensionType &type) {
		// Compare storages
		result_ &= CompareWithType(*type.storage_type());
		return Status::OK();
	}

protected:
	// For CompareFloating (templated local classes or lambdas not supported in C++11)
	template <typename CType>
	struct ComparatorVisitor {
		RangeDataEqualsImpl *impl;
		const CType *left_values;
		const CType *right_values;

		template <typename CompareFunction>
		void operator()(CompareFunction &&compare) {
			impl->VisitValues([&](int64_t i) {
				const CType x = left_values[i + impl->left_start_idx_];
				const CType y = right_values[i + impl->right_start_idx_];
				return compare(x, y);
			});
		}
	};

	template <typename CType>
	friend struct ComparatorVisitor;

	template <typename TypeClass, typename CType = typename TypeClass::c_type>
	Status ComparePrimitive(const TypeClass &) {
		const CType *left_values = left_.GetValues<CType>(1);
		const CType *right_values = right_.GetValues<CType>(1);
		VisitValidRuns([&](int64_t i, int64_t length) {
			return memcmp(left_values + left_start_idx_ + i, right_values + right_start_idx_ + i,
			              length * sizeof(CType)) == 0;
		});
		return Status::OK();
	}

	template <typename TypeClass>
	Status CompareFloating(const TypeClass &) {
		using CType = typename TypeClass::c_type;
		const CType *left_values = left_.GetValues<CType>(1);
		const CType *right_values = right_.GetValues<CType>(1);

		ComparatorVisitor<CType> visitor {this, left_values, right_values};
		VisitFloatingEquality<CType>(options_, floating_approximate_, visitor);
		return Status::OK();
	}

	template <typename TypeClass>
	Status CompareBinary(const TypeClass &) {
		const uint8_t *left_data = left_.GetValues<uint8_t>(2, 0);
		const uint8_t *right_data = right_.GetValues<uint8_t>(2, 0);

		if (left_data != nullptr && right_data != nullptr) {
			const auto compare_ranges = [&](int64_t left_offset, int64_t right_offset, int64_t length) -> bool {
				return memcmp(left_data + left_offset, right_data + right_offset, length) == 0;
			};
			CompareWithOffsets<typename TypeClass::offset_type>(1, compare_ranges);
		} else {
			// One of the arrays is an array of empty strings and nulls.
			// We just need to compare the offsets.
			// (note we must not call memcmp() with null data pointers)
			CompareWithOffsets<typename TypeClass::offset_type>(1, [](...) { return true; });
		}
		return Status::OK();
	}

	template <typename TypeClass>
	Status CompareList(const TypeClass &) {
		const ArrayData &left_data = *left_.child_data[0];
		const ArrayData &right_data = *right_.child_data[0];

		const auto compare_ranges = [&](int64_t left_offset, int64_t right_offset, int64_t length) -> bool {
			RangeDataEqualsImpl impl(options_, floating_approximate_, left_data, right_data, left_offset, right_offset,
			                         length);
			return impl.Compare();
		};

		CompareWithOffsets<typename TypeClass::offset_type>(1, compare_ranges);
		return Status::OK();
	}

	template <typename offset_type, typename CompareRanges>
	void CompareWithOffsets(int offsets_buffer_index, CompareRanges &&compare_ranges) {
		const offset_type *left_offsets = left_.GetValues<offset_type>(offsets_buffer_index) + left_start_idx_;
		const offset_type *right_offsets = right_.GetValues<offset_type>(offsets_buffer_index) + right_start_idx_;

		const auto compare_runs = [&](int64_t i, int64_t length) {
			for (int64_t j = i; j < i + length; ++j) {
				if (left_offsets[j + 1] - left_offsets[j] != right_offsets[j + 1] - right_offsets[j]) {
					return false;
				}
			}
			if (!compare_ranges(left_offsets[i], right_offsets[i], left_offsets[i + length] - left_offsets[i])) {
				return false;
			}
			return true;
		};

		VisitValidRuns(compare_runs);
	}

	template <typename CompareValues>
	void VisitValues(CompareValues &&compare_values) {
		internal::VisitSetBitRunsVoid(left_.buffers[0], left_.offset + left_start_idx_, range_length_,
		                              [&](int64_t position, int64_t length) {
			                              for (int64_t i = 0; i < length; ++i) {
				                              result_ &= compare_values(position + i);
			                              }
		                              });
	}

	// Visit and compare runs of non-null values
	template <typename CompareRuns>
	void VisitValidRuns(CompareRuns &&compare_runs) {
		const uint8_t *left_null_bitmap = left_.GetValues<uint8_t>(0, 0);
		if (left_null_bitmap == nullptr) {
			result_ = compare_runs(0, range_length_);
			return;
		}
		internal::SetBitRunReader reader(left_null_bitmap, left_.offset + left_start_idx_, range_length_);
		while (true) {
			const auto run = reader.NextRun();
			if (run.length == 0) {
				return;
			}
			if (!compare_runs(run.position, run.length)) {
				result_ = false;
				return;
			}
		}
	}

	const EqualOptions &options_;
	const bool floating_approximate_;
	const ArrayData &left_;
	const ArrayData &right_;
	const int64_t left_start_idx_;
	const int64_t right_start_idx_;
	const int64_t range_length_;

	bool result_;
};

bool CompareArrayRanges(const ArrayData &left, const ArrayData &right, int64_t left_start_idx, int64_t left_end_idx,
                        int64_t right_start_idx, const EqualOptions &options, bool floating_approximate) {

	const int64_t range_length = left_end_idx - left_start_idx;
	if (left_start_idx + range_length > left.length) {
		// Left range too small
		return false;
	}
	if (right_start_idx + range_length > right.length) {
		// Right range too small
		return false;
	}
	// Compare values
	RangeDataEqualsImpl impl(options, floating_approximate, left, right, left_start_idx, right_start_idx, range_length);
	return impl.Compare();
}

bool CompareArrays(const Array &left, const Array &right) {
	auto ops = EqualOptions::Defaults();
	return CompareArrayRanges(*left.data(), *right.data(), 0, left.length(), 0, ops, false);
}

bool ArrowChunkEquals(const arrow::ChunkedArray &left, const arrow::ChunkedArray &right) {
	if (left.length() != right.length()) {
		return false;
	}
	if (left.null_count() != right.null_count()) {
		return false;
	}
	arrow::internal::MultipleChunkIterator iterator(left, right);
	std::shared_ptr<arrow::Array> left_piece, right_piece;
	while (iterator.Next(&left_piece, &right_piece)) {
		if (!CompareArrays(*left_piece, *right_piece)) {
			return false;
		}
	}
	return true;
}

bool ArrowTableEquals(const arrow::Table &left, const arrow::Table &right) {
	if (&left == &right) {
		return true;
	}
	if (!(left.schema()->Equals(*right.schema()), true)) {
		return false;
	}
	if (left.num_columns() != right.num_columns()) {
		return false;
	}

	for (int i = 0; i < left.num_columns(); i++) {
		if (!ArrowChunkEquals(*left.column(i), *right.column(i))) {
			return false;
		}
	}
	return true;
}
