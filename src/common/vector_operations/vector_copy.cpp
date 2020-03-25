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
static void TemplatedCopy(Vector &source, VectorData &sdata, Vector &target, idx_t source_offset, idx_t copy_count) {
	auto ldata = (T*) sdata.data;
	auto tdata = FlatVector::GetData<T>(target);
	for(idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sdata.sel->get_index(source_offset + i);
		tdata[i] = ldata[source_idx];
	}
}

void VectorOperations::Copy(Vector &source, Vector &target, idx_t source_count, idx_t offset) {
	assert(offset <= source_count);
	assert(target.vector_type == VectorType::FLAT_VECTOR);
	if (source.type != target.type) {
		throw TypeMismatchException(source.type, target.type, "Copy types don't match!");
	}
	if (offset == source_count) {
		return;
	}
	VectorData sdata;
	source.Orrify(source_count, sdata);

	idx_t copy_count = source_count - offset;

	// first copy the nullmask
	auto &lmask = *sdata.nullmask;
	auto &tmask = FlatVector::Nullmask(target);
	for(idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sdata.sel->get_index(offset + i);
		tmask[i] = lmask[source_idx];
	}
	// now copy over the data
	switch(source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		TemplatedCopy<int8_t>(source, sdata, target, offset, copy_count);
		break;
	case TypeId::INT16:
		TemplatedCopy<int16_t>(source, sdata, target, offset, copy_count);
		break;
	case TypeId::INT32:
		TemplatedCopy<int32_t>(source, sdata, target, offset, copy_count);
		break;
	case TypeId::INT64:
		TemplatedCopy<int64_t>(source, sdata, target, offset, copy_count);
		break;
	case TypeId::POINTER:
		TemplatedCopy<uint64_t>(source, sdata, target, offset, copy_count);
		break;
	case TypeId::FLOAT:
		TemplatedCopy<float>(source, sdata, target, offset, copy_count);
		break;
	case TypeId::DOUBLE:
		TemplatedCopy<double>(source, sdata, target, offset, copy_count);
		break;
	case TypeId::VARCHAR: {
		auto ldata = (string_t*) sdata.data;
		auto tdata = FlatVector::GetData<string_t>(target);
		for(idx_t i = 0; i < copy_count; i++) {
			if (!tmask[i]) {
				auto source_idx = sdata.sel->get_index(offset + i);
				tdata[i] = StringVector::AddString(target, ldata[source_idx]);
			}
		}
		break;
	}
	case TypeId::STRUCT: {
		// for the rest we copy the children of the vector with the specified offset
		auto &source_children = StructVector::GetEntries(source);
		for (auto &child : source_children) {
			auto child_copy = make_unique<Vector>(child.second->type);

			VectorOperations::Copy(*child.second, *child_copy, source_count, offset);
			StructVector::AddEntry(target, child.first, move(child_copy));
		}
		break;
	}
	case TypeId::LIST: {
		// // copy main vector
		// // TODO implement non-zero offsets
		assert(offset == 0 || !ListVector::HasEntry(target));
		assert(target.type == TypeId::LIST);

		TemplatedCopy<list_entry_t>(source, sdata, target, offset, copy_count);
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

template<class T>
static void TemplatedAppend(Vector &source, VectorData &sdata, Vector &target, idx_t target_offset, idx_t copy_count) {
	auto ldata = (T*) sdata.data;
	auto tdata = FlatVector::GetData<T>(target);
	for(idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sdata.sel->get_index(i);
		auto target_idx = target_offset + i;

		tdata[target_idx] = ldata[source_idx];
	}
}

void VectorOperations::Append(Vector &source, Vector &target, idx_t copy_count, idx_t target_offset) {
	assert(target.vector_type == VectorType::FLAT_VECTOR);
	if (source.type != target.type) {
		throw TypeMismatchException(source.type, target.type, "Append types don't match!");
	}
	VectorData sdata;
	source.Orrify(copy_count, sdata);

	assert(target_offset + copy_count <= STANDARD_VECTOR_SIZE);

	auto &smask = *sdata.nullmask;
	auto &tmask = FlatVector::Nullmask(target);
	// merge null masks
	for(idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sdata.sel->get_index(i);
		auto target_idx = target_offset + i;

		tmask[target_idx] = smask[source_idx];
	}

	switch(source.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		TemplatedAppend<int8_t>(source, sdata, target, target_offset, copy_count);
		break;
	case TypeId::INT16:
		TemplatedAppend<int16_t>(source, sdata, target, target_offset, copy_count);
		break;
	case TypeId::INT32:
		TemplatedAppend<int32_t>(source, sdata, target, target_offset, copy_count);
		break;
	case TypeId::INT64:
		TemplatedAppend<int64_t>(source, sdata, target, target_offset, copy_count);
		break;
	case TypeId::POINTER:
		TemplatedAppend<uint64_t>(source, sdata, target, target_offset, copy_count);
		break;
	case TypeId::FLOAT:
		TemplatedAppend<float>(source, sdata, target, target_offset, copy_count);
		break;
	case TypeId::DOUBLE:
		TemplatedAppend<double>(source, sdata, target, target_offset, copy_count);
		break;
	case TypeId::VARCHAR: {
		auto ldata = (string_t *) sdata.data;
		auto tdata = FlatVector::GetData<string_t>(target);
		for(idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sdata.sel->get_index(i);
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
			VectorOperations::Append(*source_children[i].second, *target_children[i].second, copy_count, target_offset);
		}
		break;
	}
	case TypeId::LIST: {
		// recursively apply to children
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

		auto source_data = (list_entry_t *)sdata.data;
		auto target_data = FlatVector::GetData<list_entry_t>(target);
		for(idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sdata.sel->get_index(i);
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
