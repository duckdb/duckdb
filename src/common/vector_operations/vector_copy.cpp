//===--------------------------------------------------------------------===//
// copy.cpp
// Description: This file contains the implementation of the different copy
// functions
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template<class T>
static void TemplatedCopy(Vector &source, const SelectionVector &sel, Vector &target, idx_t source_offset, idx_t copy_count) {
	auto ldata = FlatVector::GetData<T>(source);
	auto tdata = FlatVector::GetData<T>(target);
	for(idx_t i = 0; i < copy_count; i++) {
		auto idx = sel.get_index(source_offset + i);
		tdata[i] = ldata[idx];
	}
}

void VectorOperations::Copy(Vector &source, Vector &target, const SelectionVector &sel, idx_t source_count, idx_t offset) {
	assert(offset <= source_count);
	assert(target.vector_type == VectorType::FLAT_VECTOR);
	assert(source.type == target.type);
	if (source.vector_type == VectorType::DICTIONARY_VECTOR) {
		// dictionary vector: merge selection vectors
		auto &child = DictionaryVector::Child(source);
		auto &dict_sel = DictionaryVector::SelVector(source);
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(sel, source_count);
		SelectionVector merged_sel(new_buffer);

		VectorOperations::Copy(child, target, merged_sel, source_count, offset);
		return;
	}
	if (source.vector_type != VectorType::FLAT_VECTOR && source.vector_type != VectorType::CONSTANT_VECTOR) {
		throw NotImplementedException("FIXME unimplemented vector type for copy");
	}

	idx_t copy_count = source_count - offset;
	if (copy_count == 0) {
		return;
	}

	// first copy the nullmask
	auto &tmask = FlatVector::Nullmask(target);
	if (source.vector_type == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(source)) {
			for(idx_t i = 0; i < copy_count; i++) {
				tmask[i] = true;
			}
		}
	} else {
		auto &smask = FlatVector::Nullmask(source);
		for(idx_t i = 0; i < copy_count; i++) {
			auto idx = sel.get_index(offset + i);
			tmask[i] = smask[idx];
		}
	}

	// now copy over the data
	switch(source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		TemplatedCopy<int8_t>(source, sel, target, offset, copy_count);
		break;
	case TypeId::INT16:
		TemplatedCopy<int16_t>(source, sel, target, offset, copy_count);
		break;
	case TypeId::INT32:
		TemplatedCopy<int32_t>(source, sel, target, offset, copy_count);
		break;
	case TypeId::INT64:
		TemplatedCopy<int64_t>(source, sel, target, offset, copy_count);
		break;
	case TypeId::POINTER:
		TemplatedCopy<uint64_t>(source, sel, target, offset, copy_count);
		break;
	case TypeId::FLOAT:
		TemplatedCopy<float>(source, sel, target, offset, copy_count);
		break;
	case TypeId::DOUBLE:
		TemplatedCopy<double>(source, sel, target, offset, copy_count);
		break;
	case TypeId::VARCHAR: {
		auto ldata = FlatVector::GetData<string_t>(source);
		auto tdata = FlatVector::GetData<string_t>(target);
		for(idx_t i = 0; i < copy_count; i++) {
			if (!tmask[i]) {
				auto idx = sel.get_index(offset + i);
				tdata[i] = StringVector::AddString(target, ldata[idx]);
			}
		}
		break;
	}
	case TypeId::STRUCT: {
		// for the rest we copy the children of the vector with the specified offset
		auto &source_children = StructVector::GetEntries(source);
		for (auto &child : source_children) {
			auto child_copy = make_unique<Vector>(child.second->type);

			VectorOperations::Copy(*child.second, *child_copy, sel, source_count, offset);
			StructVector::AddEntry(target, child.first, move(child_copy));
		}
		break;
	}
	case TypeId::LIST: {
		// // copy main vector
		// // TODO implement non-zero offsets
		assert(offset == 0 || !ListVector::HasEntry(target));
		assert(target.type == TypeId::LIST);

		TemplatedCopy<list_entry_t>(source, sel, target, offset, copy_count);
		if (ListVector::HasEntry(source)) {
			auto &child = ListVector::GetEntry(source);
			auto child_copy = make_unique<ChunkCollection>();
			child_copy->Append(child);
			// TODO optimization: if offset != 0 we can skip some of the child list and adjustd offsets accordingly
			ListVector::SetEntry(target, move(child_copy));
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for copy!");
	}
}

void VectorOperations::Copy(Vector &source, Vector &target, idx_t source_count, idx_t offset) {
	switch(source.vector_type) {
	case VectorType::DICTIONARY_VECTOR: {
		// dictionary: continue into child with selection vector
		auto &child = DictionaryVector::Child(source);
		auto &dict_sel = DictionaryVector::SelVector(source);
		VectorOperations::Copy(child, target, dict_sel, source_count, offset);
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		VectorOperations::Copy(source, target, ConstantVector::ZeroSelectionVector, source_count, offset);
		break;
	default:
		source.Normalify(source_count);
		VectorOperations::Copy(source, target, FlatVector::IncrementalSelectionVector, source_count, offset);
		break;
	}
}

template<class T>
static void TemplatedAppend(Vector &source, const SelectionVector &sel, Vector &target, idx_t target_offset, idx_t copy_count) {
	auto ldata = FlatVector::GetData<T>(source);
	auto tdata = FlatVector::GetData<T>(target);
	for(idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel.get_index(i);
		auto target_idx = target_offset + i;

		tdata[target_idx] = ldata[source_idx];
	}
}

void VectorOperations::Append(Vector &source, Vector &target, const SelectionVector &sel, idx_t copy_count, idx_t target_offset) {
	assert(target.vector_type == VectorType::FLAT_VECTOR);
	assert(source.type == target.type);
	if (source.vector_type == VectorType::DICTIONARY_VECTOR) {
		// dictionary vector: merge selection vectors
		auto &child = DictionaryVector::Child(source);
		auto &dict_sel = DictionaryVector::SelVector(source);
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(sel, copy_count);
		SelectionVector merged_sel(new_buffer);

		VectorOperations::Append(child, target, merged_sel, copy_count, target_offset);
		return;
	}

	assert(target_offset + copy_count <= STANDARD_VECTOR_SIZE);

	// first copy the nullmask
	auto &tmask = FlatVector::Nullmask(target);
	if (source.vector_type == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(source)) {
			for(idx_t i = 0; i < copy_count; i++) {
				tmask[target_offset + i] = true;
			}
		}
	} else {
		auto &smask = FlatVector::Nullmask(source);
		for(idx_t i = 0; i < copy_count; i++) {
			auto idx = sel.get_index(i);
			tmask[target_offset + i] = smask[idx];
		}
	}

	switch(source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		TemplatedAppend<int8_t>(source, sel, target, target_offset, copy_count);
		break;
	case TypeId::INT16:
		TemplatedAppend<int16_t>(source, sel, target, target_offset, copy_count);
		break;
	case TypeId::INT32:
		TemplatedAppend<int32_t>(source, sel, target, target_offset, copy_count);
		break;
	case TypeId::INT64:
		TemplatedAppend<int64_t>(source, sel, target, target_offset, copy_count);
		break;
	case TypeId::POINTER:
		TemplatedAppend<uint64_t>(source, sel, target, target_offset, copy_count);
		break;
	case TypeId::FLOAT:
		TemplatedAppend<float>(source, sel, target, target_offset, copy_count);
		break;
	case TypeId::DOUBLE:
		TemplatedAppend<double>(source, sel, target, target_offset, copy_count);
		break;
	case TypeId::VARCHAR: {
		auto ldata = FlatVector::GetData<string_t>(source);
		auto tdata = FlatVector::GetData<string_t>(target);
		for(idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sel.get_index(i);
			auto target_idx = target_offset + i;

			if (!tmask[target_idx]) {
				tdata[target_idx] = StringVector::AddString(target, ldata[source_idx]);
			}
		}
		break;
	}
	case TypeId::STRUCT: {
		// recursively apply to children
		auto &source_children = StructVector::GetEntries(source);
		auto &target_children = StructVector::GetEntries(target);
		assert(source_children.size() == target_children.size());
		for (idx_t i = 0; i < source_children.size(); i++) {
			assert(target_children[i].first == target_children[i].first);
			VectorOperations::Append(*source_children[i].second, *target_children[i].second, sel, copy_count, target_offset);
		}
		break;
	}
	case TypeId::LIST: {
		if (!ListVector::HasEntry(source)) {
			assert(!VectorOperations::HasNotNull(source, copy_count));
			auto new_source_child = make_unique<ChunkCollection>();
			ListVector::SetEntry(source, move(new_source_child));
		}

		if (!ListVector::HasEntry(target)) {
			assert(!VectorOperations::HasNotNull(target, target_offset));
			auto new_target_child = make_unique<ChunkCollection>();
			ListVector::SetEntry(target, move(new_target_child));
		}

		auto &source_child = ListVector::GetEntry(source);
		auto &target_child = ListVector::GetEntry(target);
		// append to list index
		auto old_target_child_len = target_child.count;
		target_child.Append(source_child);

		auto source_data = FlatVector::GetData<list_entry_t>(source);
		auto target_data = FlatVector::GetData<list_entry_t>(target);
		for(idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sel.get_index(i);
			auto target_idx = target_offset + i;

			if (!tmask[target_idx]) {
				target_data[target_idx].length = source_data[source_idx].length;
				target_data[target_idx].offset = source_data[source_idx].offset + old_target_child_len;
			}
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for append!");
	}
}

void VectorOperations::Append(Vector &source, Vector &target, idx_t copy_count, idx_t target_offset) {
	switch(source.vector_type) {
	case VectorType::DICTIONARY_VECTOR: {
		// dictionary: continue into child with selection vector
		auto &child = DictionaryVector::Child(source);
		auto &dict_sel = DictionaryVector::SelVector(source);
		VectorOperations::Append(child, target, dict_sel, copy_count, target_offset);
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		VectorOperations::Append(source, target, ConstantVector::ZeroSelectionVector, copy_count, target_offset);
		break;
	default:
		source.Normalify(copy_count);
		VectorOperations::Append(source, target, FlatVector::IncrementalSelectionVector, copy_count, target_offset);
		break;
	}
}
