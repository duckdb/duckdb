//===--------------------------------------------------------------------===//
// copy.cpp
// Description: This file contains the implementation of the different copy
// functions
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

namespace {

template <class T>
void TemplatedCopy(const Vector &source, const SelectionVector &sel, Vector &target, idx_t source_offset,
                   idx_t target_offset, idx_t copy_count) {
	auto ldata = FlatVector::GetData<T>(source);
	auto tdata = FlatVector::GetData<T>(target);
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel.get_index(source_offset + i);
		tdata[target_offset + i] = ldata[source_idx];
	}
}
} // namespace

static const ValidityMask &ExtractValidityMask(const Vector &v) {
	switch (v.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		return FlatVector::Validity(v);
	case VectorType::FSST_VECTOR:
		return FSSTVector::Validity(v);
	default:
		throw InternalException("Unsupported vector type in vector copy");
	}
}

void VectorOperations::Copy(const Vector &source_p, Vector &target, const SelectionVector &sel_p, idx_t source_count,
                            idx_t source_offset, idx_t target_offset, idx_t copy_count) {
	SelectionVector owned_sel;
	const SelectionVector *sel = &sel_p;

	const Vector *source = &source_p;
	bool finished = false;
	while (!finished) {
		switch (source->GetVectorType()) {
		case VectorType::DICTIONARY_VECTOR: {
			// dictionary vector: merge selection vectors
			auto &child = DictionaryVector::Child(*source);
			auto &dict_sel = DictionaryVector::SelVector(*source);
			// merge the selection vectors and verify the child
			auto new_buffer = dict_sel.Slice(*sel, source_count);
			owned_sel.Initialize(new_buffer);
			sel = &owned_sel;
			source = &child;
			break;
		}
		case VectorType::SEQUENCE_VECTOR: {
			int64_t start, increment;
			Vector seq(source->GetType());
			SequenceVector::GetSequence(*source, start, increment);
			VectorOperations::GenerateSequence(seq, source_count, *sel, start, increment);
			VectorOperations::Copy(seq, target, *sel, source_count, source_offset, target_offset);
			return;
		}
		case VectorType::CONSTANT_VECTOR:
			sel = ConstantVector::ZeroSelectionVector(copy_count, owned_sel);
			finished = true;
			break;
		case VectorType::FSST_VECTOR:
			finished = true;
			break;
		case VectorType::FLAT_VECTOR:
			finished = true;
			break;
		default:
			throw NotImplementedException("FIXME unimplemented vector type for VectorOperations::Copy");
		}
	}

	if (copy_count == 0) {
		return;
	}

	// Allow copying of a single value to constant vectors
	const auto target_vector_type = target.GetVectorType();
	if (copy_count == 1 && target_vector_type == VectorType::CONSTANT_VECTOR) {
		target_offset = 0;
		target.SetVectorType(VectorType::FLAT_VECTOR);
	}
	D_ASSERT(target.GetVectorType() == VectorType::FLAT_VECTOR);

	// first copy the nullmask
	auto &tmask = FlatVector::Validity(target);
	if (source->GetVectorType() == VectorType::CONSTANT_VECTOR) {
		const bool valid = !ConstantVector::IsNull(*source);
		for (idx_t i = 0; i < copy_count; i++) {
			tmask.Set(target_offset + i, valid);
		}
	} else {
		auto &smask = ExtractValidityMask(*source);
		tmask.CopySel(smask, *sel, source_offset, target_offset, copy_count);
	}

	D_ASSERT(sel);

	// For FSST Vectors we decompress instead of copying.
	if (source->GetVectorType() == VectorType::FSST_VECTOR) {
		FSSTVector::DecompressVector(*source, target, source_offset, target_offset, copy_count, sel);
		return;
	}

	// now copy over the data
	switch (source->GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedCopy<int8_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT16:
		TemplatedCopy<int16_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT32:
		TemplatedCopy<int32_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT64:
		TemplatedCopy<int64_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT8:
		TemplatedCopy<uint8_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT16:
		TemplatedCopy<uint16_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT32:
		TemplatedCopy<uint32_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT64:
		TemplatedCopy<uint64_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT128:
		TemplatedCopy<hugeint_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT128:
		TemplatedCopy<uhugeint_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::FLOAT:
		TemplatedCopy<float>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedCopy<double>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedCopy<interval_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::VARCHAR: {
		auto ldata = FlatVector::GetData<string_t>(*source);
		auto tdata = FlatVector::GetData<string_t>(target);
		for (idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sel->get_index(source_offset + i);
			auto target_idx = target_offset + i;
			if (tmask.RowIsValid(target_idx)) {
				tdata[target_idx] = StringVector::AddStringOrBlob(target, ldata[source_idx]);
			}
		}
		break;
	}
	case PhysicalType::STRUCT: {
		auto &source_children = StructVector::GetEntries(*source);
		auto &target_children = StructVector::GetEntries(target);
		D_ASSERT(source_children.size() == target_children.size());
		for (idx_t i = 0; i < source_children.size(); i++) {
			VectorOperations::Copy(*source_children[i], *target_children[i], sel_p, source_count, source_offset,
			                       target_offset, copy_count);
		}
		break;
	}
	case PhysicalType::ARRAY: {
		D_ASSERT(target.GetType().InternalType() == PhysicalType::ARRAY);
		D_ASSERT(ArrayType::GetSize(source->GetType()) == ArrayType::GetSize(target.GetType()));

		auto &source_child = ArrayVector::GetEntry(*source);
		auto &target_child = ArrayVector::GetEntry(target);
		auto array_size = ArrayType::GetSize(source->GetType());

		// Create a selection vector for the child elements
		SelectionVector child_sel(source_count * array_size);
		for (idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sel->get_index(source_offset + i);
			for (idx_t j = 0; j < array_size; j++) {
				child_sel.set_index((source_offset * array_size) + (i * array_size + j), source_idx * array_size + j);
			}
		}
		VectorOperations::Copy(source_child, target_child, child_sel, source_count * array_size,
		                       source_offset * array_size, target_offset * array_size);
		break;
	}
	case PhysicalType::LIST: {
		D_ASSERT(target.GetType().InternalType() == PhysicalType::LIST);

		auto &source_child = ListVector::GetEntry(*source);
		auto sdata = FlatVector::GetData<list_entry_t>(*source);
		auto tdata = FlatVector::GetData<list_entry_t>(target);

		if (target_vector_type == VectorType::CONSTANT_VECTOR) {
			// If we are only writing one value, then the copied values (if any) are contiguous
			// and we can just Append from the offset position
			if (!tmask.RowIsValid(target_offset)) {
				break;
			}
			auto source_idx = sel->get_index(source_offset);
			auto &source_entry = sdata[source_idx];
			const idx_t source_child_size = source_entry.length + source_entry.offset;

			//! overwrite constant target vectors.
			ListVector::SetListSize(target, 0);
			ListVector::Append(target, source_child, source_child_size, source_entry.offset);

			auto &target_entry = tdata[target_offset];
			target_entry.length = source_entry.length;
			target_entry.offset = 0;
		} else {
			//! if the source has list offsets, we need to append them to the target
			//! build a selection vector for the copied child elements
			vector<sel_t> child_rows;
			for (idx_t i = 0; i < copy_count; ++i) {
				if (tmask.RowIsValid(target_offset + i)) {
					auto source_idx = sel->get_index(source_offset + i);
					auto &source_entry = sdata[source_idx];
					for (idx_t j = 0; j < source_entry.length; ++j) {
						child_rows.emplace_back(source_entry.offset + j);
					}
				}
			}
			idx_t source_child_size = child_rows.size();
			SelectionVector child_sel(child_rows.data());

			idx_t old_target_child_len = ListVector::GetListSize(target);

			//! append to list itself
			ListVector::Append(target, source_child, child_sel, source_child_size);

			//! now write the list offsets
			for (idx_t i = 0; i < copy_count; i++) {
				auto source_idx = sel->get_index(source_offset + i);
				auto &source_entry = sdata[source_idx];
				auto &target_entry = tdata[target_offset + i];

				target_entry.length = source_entry.length;
				target_entry.offset = old_target_child_len;
				if (tmask.RowIsValid(target_offset + i)) {
					old_target_child_len += target_entry.length;
				}
			}
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type '%s' for copy!",
		                              TypeIdToString(source->GetType().InternalType()));
	}

	if (target_vector_type != VectorType::FLAT_VECTOR) {
		target.SetVectorType(target_vector_type);
	}
}

void VectorOperations::Copy(const Vector &source_p, Vector &target, const SelectionVector &sel_p, idx_t source_count,
                            idx_t source_offset, idx_t target_offset) {
	D_ASSERT(source_offset <= source_count);
	D_ASSERT(source_p.GetType() == target.GetType());
	idx_t copy_count = source_count - source_offset;
	VectorOperations::Copy(source_p, target, sel_p, source_count, source_offset, target_offset, copy_count);
}

void VectorOperations::Copy(const Vector &source, Vector &target, idx_t source_count, idx_t source_offset,
                            idx_t target_offset) {
	VectorOperations::Copy(source, target, *FlatVector::IncrementalSelectionVector(), source_count, source_offset,
	                       target_offset);
}

} // namespace duckdb
