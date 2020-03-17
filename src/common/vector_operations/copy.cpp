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

// template <class T>
// static void copy_function(T *__restrict source, T *__restrict target, idx_t offset, idx_t count,
//                           sel_t *__restrict sel_vector) {
// 	VectorOperations::Exec(
// 	    sel_vector, count + offset, [&](idx_t i, idx_t k) { target[k - offset] = source[i]; }, offset);
// }

// template <class T>
// static void copy_function_set_null(T *__restrict source, T *__restrict target, idx_t offset, idx_t count,
//                                    sel_t *__restrict sel_vector, nullmask_t &nullmask) {
// 	if (nullmask.any()) {
// 		// null values, have to check the NULL values in the mask
// 		VectorOperations::Exec(
// 		    sel_vector, count + offset,
// 		    [&](idx_t i, idx_t k) {
// 			    if (nullmask[i]) {
// 				    target[k - offset] = NullValue<T>();
// 			    } else {
// 				    target[k - offset] = source[i];
// 			    }
// 		    },
// 		    offset);
// 	} else {
// 		// no NULL values, use normal copy
// 		copy_function(source, target, offset, count, sel_vector);
// 	}
// }

// template <class T, bool SET_NULL>
// static void copy_loop(Vector &input, void *target, idx_t offset, idx_t element_count) {
// 	auto ldata = (T *)input.GetData();
// 	auto result_data = (T *)target;
// 	if (SET_NULL) {
// 		copy_function_set_null(ldata, result_data, offset, element_count, input.sel_vector(), input.nullmask);
// 	} else {
// 		copy_function(ldata, result_data, offset, element_count, input.sel_vector());
// 	}
// }

// template <bool SET_NULL> void generic_copy_loop(Vector &source, void *target, idx_t offset, idx_t element_count) {
// 	if (source.size() == 0)
// 		return;
// 	if (element_count == 0) {
// 		element_count = source.size();
// 	}
// 	assert(offset + element_count <= source.size());

// 	switch (source.type) {
// 	case TypeId::BOOL:
// 	case TypeId::INT8:
// 		copy_loop<int8_t, SET_NULL>(source, target, offset, element_count);
// 		break;
// 	case TypeId::INT16:
// 		copy_loop<int16_t, SET_NULL>(source, target, offset, element_count);
// 		break;
// 	case TypeId::INT32:
// 		copy_loop<int32_t, SET_NULL>(source, target, offset, element_count);
// 		break;
// 	case TypeId::INT64:
// 		copy_loop<int64_t, SET_NULL>(source, target, offset, element_count);
// 		break;
// 	case TypeId::HASH:
// 		copy_loop<uint64_t, SET_NULL>(source, target, offset, element_count);
// 		break;
// 	case TypeId::POINTER:
// 		copy_loop<uintptr_t, SET_NULL>(source, target, offset, element_count);
// 		break;
// 	case TypeId::FLOAT:
// 		copy_loop<float, SET_NULL>(source, target, offset, element_count);
// 		break;
// 	case TypeId::DOUBLE:
// 		copy_loop<double, SET_NULL>(source, target, offset, element_count);
// 		break;
// 	case TypeId::VARCHAR:
// 		copy_loop<string_t, SET_NULL>(source, target, offset, element_count);
// 		break;
// 	default:
// 		throw NotImplementedException("Unimplemented type for copy");
// 	}
// }

//===--------------------------------------------------------------------===//
// Copy data from vector
//===--------------------------------------------------------------------===//
void VectorOperations::Copy(Vector &source, void *target, idx_t offset, idx_t element_count) {
	if (!TypeIsConstantSize(source.type)) {
		throw InvalidTypeException(source.type, "Cannot copy non-constant size types using this method!");
	}
	throw NotImplementedException("FIXME: copy");
	// generic_copy_loop<false>(source, target, offset, element_count);
}

void VectorOperations::CopyToStorage(Vector &source, void *target, idx_t offset, idx_t element_count) {
	throw NotImplementedException("FIXME: copy");
	// generic_copy_loop<true>(source, target, offset, element_count);
}

void VectorOperations::Copy(Vector &source, Vector &target, idx_t offset) {
	throw NotImplementedException("FIXME: copy");
	// if (source.type != target.type) {
	// 	throw TypeMismatchException(source.type, target.type, "Copy types don't match!");
	// }
	// source.Normalify();

	// assert(target.vector_type == VectorType::FLAT_VECTOR);
	// assert(!target.sel_vector());
	// assert(offset <= source.size());
	// assert(source.size() - offset <= STANDARD_VECTOR_SIZE);
	// idx_t copy_count = source.size() - offset;

	// // merge null masks
	// VectorOperations::Exec(
	//     source, [&](idx_t i, idx_t k) { target.nullmask[k - offset] = source.nullmask[i]; }, offset);

	// if (source.nullmask.all()) {
	// 	return;
	// }

	// if (!TypeIsConstantSize(source.type)) {
	// 	switch (source.type) {
	// 	case TypeId::VARCHAR: {
	// 		auto source_data = (string_t *)source.GetData();
	// 		auto target_data = (string_t *)target.GetData();
	// 		VectorOperations::Exec(
	// 		    source,
	// 		    [&](idx_t i, idx_t k) {
	// 			    if (!target.nullmask[k - offset]) {
	// 				    target_data[k - offset] = target.AddString(source_data[i]);
	// 			    }
	// 		    },
	// 		    offset);
	// 	} break;
	// 	case TypeId::STRUCT: {
	// 		// the main vector only has a nullmask, so set that with offset
	// 		// recursively apply to children
	// 		auto &source_children = source.GetStructEntries();
	// 		for (auto &child : source_children) {
	// 			auto child_copy = make_unique<Vector>(target.cardinality(), child.second->type);

	// 			VectorOperations::Copy(*child.second, *child_copy, offset);
	// 			target.AddStructEntry(child.first, move(child_copy));
	// 		}
	// 	} break;
	// 	case TypeId::LIST: {
	// 		// // copy main vector
	// 		// // TODO implement non-zero offsets
	// 		assert(offset == 0 || !target.HasListEntry());
	// 		assert(target.type == TypeId::LIST);

	// 		copy_loop<list_entry_t, false>(source, target.GetData(), offset, copy_count);
	// 		auto &child = source.GetListEntry();
	// 		auto child_copy = make_unique<ChunkCollection>();
	// 		child_copy->Append(child);
	// 		// TODO optimization: if offset != 0 we can skip some of the child list and adjustd offsets accordingly
	// 		target.SetListEntry(move(child_copy));
	// 	} break;
	// 	default:
	// 		throw NotImplementedException("Unimplemented type for copy");
	// 	}
	// } else {
	// 	VectorOperations::Copy(source, target.GetData(), offset, copy_count);
	// }
}

void VectorOperations::Append(Vector &source, Vector &target) {
	throw NotImplementedException("FIXME: copy");
	// if (source.type != target.type) {
	// 	throw TypeMismatchException(source.type, target.type, "Append types don't match!");
	// }
	// if (target.size() == 0) {
	// 	VectorOperations::Copy(source, target);
	// 	return;
	// }
	// source.Normalify();

	// assert(target.vector_type == VectorType::FLAT_VECTOR);
	// assert(!target.sel_vector());
	// idx_t copy_count = source.size();
	// idx_t old_count = target.size();
	// assert(old_count + copy_count <= STANDARD_VECTOR_SIZE);

	// // merge null masks
	// VectorOperations::Exec(source, [&](idx_t i, idx_t k) { target.nullmask[old_count + k] = source.nullmask[i]; });

	// if (!TypeIsConstantSize(source.type)) {
	// 	switch (source.type) {
	// 	case TypeId::VARCHAR: {
	// 		auto source_data = (string_t *)source.GetData();
	// 		auto target_data = (string_t *)target.GetData();
	// 		VectorOperations::Exec(source, [&](idx_t i, idx_t k) {
	// 			if (!target.nullmask[old_count + k]) {
	// 				target_data[old_count + k] = target.AddString(source_data[i]);
	// 			}
	// 		});
	// 	} break;
	// 	case TypeId::STRUCT: {
	// 		// recursively apply to children
	// 		auto &source_children = source.GetStructEntries();
	// 		auto &target_children = target.GetStructEntries();
	// 		assert(source_children.size() == target_children.size());
	// 		for (size_t i = 0; i < source_children.size(); i++) {
	// 			assert(target_children[i].first == target_children[i].first);
	// 			VectorOperations::Append(*source_children[i].second, *target_children[i].second);
	// 		}
	// 	} break;

	// 	case TypeId::LIST: {
	// 		// recursively apply to children

	// 		if (!source.HasListEntry()) {
	// 			assert(!VectorOperations::HasNotNull(source));
	// 			auto new_source_child = make_unique<ChunkCollection>();
	// 			source.SetListEntry(move(new_source_child));
	// 		}

	// 		if (!target.HasListEntry()) {
	// 			assert(!VectorOperations::HasNotNull(target));
	// 			auto new_target_child = make_unique<ChunkCollection>();
	// 			target.SetListEntry(move(new_target_child));
	// 		}

	// 		auto &source_child = source.GetListEntry();
	// 		auto &target_child = target.GetListEntry();
	// 		// append to list index
	// 		auto old_target_child_len = target_child.count;
	// 		target_child.Append(source_child);

	// 		auto target_data = ((list_entry_t *)target.GetData());
	// 		auto source_data = ((list_entry_t *)source.GetData());

	// 		VectorOperations::Exec(source, [&](idx_t i, idx_t k) {
	// 			if (!target.nullmask[old_count + k]) {
	// 				target_data[old_count + k].length = source_data[i].length;
	// 				target_data[old_count + k].offset = source_data[i].offset + old_target_child_len;
	// 			}
	// 		});
	// 	} break;
	// 	default:
	// 		throw NotImplementedException("Unimplemented type for APPEND");
	// 	}
	// } else {
	// 	VectorOperations::Copy(source, target.GetData() + old_count * GetTypeIdSize(source.type));
	// }
}
