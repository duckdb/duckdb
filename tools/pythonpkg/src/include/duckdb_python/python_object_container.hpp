//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/python_object_container.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

template <typename TGT_PY_TYPE, typename SRC_PY_TYPE>
struct PythonAssignmentFunction {
	typedef void (*assign_t)(TGT_PY_TYPE &, SRC_PY_TYPE &);
};

struct PythonGILWrapper {
	py::gil_scoped_acquire acquire;
};

//! Every Python Object Must be created through our container
//! The Container ensures that the GIL is HOLD on Python Object Construction/Destruction/Modification
template <class PY_TYPE>
class PythonObjectContainer {
public:
	PythonObjectContainer() {
	}

	~PythonObjectContainer() {
		py::gil_scoped_acquire acquire;
		py_obj.clear();
	}

	unique_ptr<PythonGILWrapper> GetLock() {
		return make_unique<PythonGILWrapper>();
	}

	template <class NEW_PY_TYPE>
	void AssignInternal(typename PythonAssignmentFunction<PY_TYPE, NEW_PY_TYPE>::assign_t lambda,
	                    NEW_PY_TYPE &new_value, PythonGILWrapper &lock) {
		PY_TYPE obj;
		lambda(obj, new_value);
		PushInternal(lock, obj);
	}

	void PushInternal(PythonGILWrapper &lock, PY_TYPE obj) {
		py_obj.push_back(obj);
	}

	void Push(PY_TYPE obj) {
		auto lock = GetLock();
		PushInternal(*lock, move(obj));
	}

	const PY_TYPE *GetPointerTop() {
		return &py_obj.back();
	}

private:
	vector<PY_TYPE> py_obj;
};
} // namespace duckdb
