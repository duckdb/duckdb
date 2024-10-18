#pragma once

#include "delta_kernel_ffi.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/common/enum_util.hpp"
#include <iostream>

// TODO: clean up this file as we go

namespace duckdb {

// SchemaVisitor is used to parse the schema of a Delta table from the Kernel
class SchemaVisitor {
public:
	using FieldList = child_list_t<LogicalType>;

	static unique_ptr<FieldList> VisitSnapshotSchema(ffi::SharedSnapshot *snapshot);

private:
	unordered_map<uintptr_t, unique_ptr<FieldList>> inflight_lists;
	uintptr_t next_id = 1;

	typedef void(SimpleTypeVisitorFunction)(void *, uintptr_t, ffi::KernelStringSlice);

	template <LogicalTypeId TypeId>
	static SimpleTypeVisitorFunction *VisitSimpleType() {
		return (SimpleTypeVisitorFunction *)&VisitSimpleTypeImpl<TypeId>;
	}
	template <LogicalTypeId TypeId>
	static void VisitSimpleTypeImpl(SchemaVisitor *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name) {
		state->AppendToList(sibling_list_id, name, TypeId);
	}

	static void VisitDecimal(SchemaVisitor *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name,
	                         uint8_t precision, uint8_t scale);
	static uintptr_t MakeFieldList(SchemaVisitor *state, uintptr_t capacity_hint);
	static void VisitStruct(SchemaVisitor *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name,
	                        uintptr_t child_list_id);
	static void VisitArray(SchemaVisitor *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name,
	                       bool contains_null, uintptr_t child_list_id);
	static void VisitMap(SchemaVisitor *state, uintptr_t sibling_list_id, ffi::KernelStringSlice name,
	                     bool contains_null, uintptr_t child_list_id);

	uintptr_t MakeFieldListImpl(uintptr_t capacity_hint);
	void AppendToList(uintptr_t id, ffi::KernelStringSlice name, LogicalType &&child);
	unique_ptr<FieldList> TakeFieldList(uintptr_t id);
};

// Allocator for errors that the kernel might throw
struct DuckDBEngineError : ffi::EngineError {
	// Allocate a DuckDBEngineError, function ptr passed to kernel for error allocation
	static ffi::EngineError *AllocateError(ffi::KernelError etype, ffi::KernelStringSlice msg);
	// Convert a kernel error enum to a string
	static string KernelErrorEnumToString(ffi::KernelError err);

	// Throw the error as an IOException
	[[noreturn]] void Throw(string from_info);

	// The error message from Kernel
	string error_message;
};

// RAII wrapper that returns ownership of a kernel pointer to kernel when it goes out of
// scope. Similar to std::unique_ptr. but does not define operator->() and does not require the
// kernel type to be complete.
template <typename KernelType>
struct UniqueKernelPointer {
	UniqueKernelPointer() : ptr(nullptr), free(nullptr) {
	}

	// Takes ownership of a pointer with associated deleter.
	UniqueKernelPointer(KernelType *ptr, void (*free)(KernelType *)) : ptr(ptr), free(free) {
	}

	// movable but not copyable
	UniqueKernelPointer(UniqueKernelPointer &&other) : ptr(other.ptr) {
		other.ptr = nullptr;
	}
	UniqueKernelPointer &operator=(UniqueKernelPointer &&other) {
		std::swap(ptr, other.ptr);
		std::swap(free, other.free);
		return *this;
	}
	UniqueKernelPointer(const UniqueKernelPointer &) = delete;
	UniqueKernelPointer &operator=(const UniqueKernelPointer &) = delete;

	~UniqueKernelPointer() {
		if (ptr && free) {
			free(ptr);
		}
	}

	KernelType *get() const {
		return ptr;
	}

private:
	KernelType *ptr;
	void (*free)(KernelType *) = nullptr;
};

// Syntactic sugar around the different kernel types
template <typename KernelType, void (*DeleteFunction)(KernelType *)>
struct TemplatedUniqueKernelPointer : public UniqueKernelPointer<KernelType> {
	TemplatedUniqueKernelPointer() : UniqueKernelPointer<KernelType>() {};
	TemplatedUniqueKernelPointer(KernelType *ptr) : UniqueKernelPointer<KernelType>(ptr, DeleteFunction) {};
};

typedef TemplatedUniqueKernelPointer<ffi::SharedSnapshot, ffi::drop_snapshot> KernelSnapshot;
typedef TemplatedUniqueKernelPointer<ffi::SharedExternEngine, ffi::drop_engine> KernelExternEngine;
typedef TemplatedUniqueKernelPointer<ffi::SharedScan, ffi::drop_scan> KernelScan;
typedef TemplatedUniqueKernelPointer<ffi::SharedGlobalScanState, ffi::drop_global_scan_state> KernelGlobalScanState;
typedef TemplatedUniqueKernelPointer<ffi::SharedScanDataIterator, ffi::kernel_scan_data_free> KernelScanDataIterator;

struct KernelUtils {
	static ffi::KernelStringSlice ToDeltaString(const string &str);
	static string FromDeltaString(const struct ffi::KernelStringSlice slice);
	static vector<bool> FromDeltaBoolSlice(const struct ffi::KernelBoolSlice slice);

	// TODO: all kernel results need to be unpacked, not doing so will result in an error. This should be cleaned up
	template <class T>
	static T UnpackResult(ffi::ExternResult<T> result, const string &from_where) {
		if (result.tag == ffi::ExternResult<T>::Tag::Err) {
			if (result.err._0) {
				auto error_cast = static_cast<DuckDBEngineError *>(result.err._0);
				error_cast->Throw(from_where);
			} else {
				throw IOException("Hit DeltaKernel FFI error (from: %s): Hit error, but error was nullptr",
				                  from_where.c_str());
			}
		} else if (result.tag == ffi::ExternResult<T>::Tag::Ok) {
			return result.ok._0;
		}
		throw IOException("Invalid error ExternResult tag found!");
	}
};

class PredicateVisitor : public ffi::EnginePredicate {
public:
	PredicateVisitor(const vector<string> &column_names, optional_ptr<TableFilterSet> filters);

private:
	unordered_map<string, TableFilter *> column_filters;

	static uintptr_t VisitPredicate(PredicateVisitor *predicate, ffi::KernelExpressionVisitorState *state);

	uintptr_t VisitConstantFilter(const string &col_name, const ConstantFilter &filter,
	                              ffi::KernelExpressionVisitorState *state);
	uintptr_t VisitAndFilter(const string &col_name, const ConjunctionAndFilter &filter,
	                         ffi::KernelExpressionVisitorState *state);
	uintptr_t VisitFilter(const string &col_name, const TableFilter &filter, ffi::KernelExpressionVisitorState *state);
};

} // namespace duckdb
