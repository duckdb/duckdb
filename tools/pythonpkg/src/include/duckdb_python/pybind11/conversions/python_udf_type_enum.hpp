#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

namespace duckdb {

enum class PythonUDFType : uint8_t { NATIVE, ARROW };

} // namespace duckdb

using duckdb::PythonUDFType;

namespace py = pybind11;

static PythonUDFType PythonUDFTypeFromString(const string &type) {
	auto ltype = StringUtil::Lower(type);
	if (ltype.empty() || ltype == "default" || ltype == "native") {
		return PythonUDFType::NATIVE;
	} else if (ltype == "arrow") {
		return PythonUDFType::ARROW;
	} else {
		throw InvalidInputException("'%s' is not a recognized type for 'udf_type'", type);
	}
}

static PythonUDFType PythonUDFTypeFromInteger(int64_t value) {
	if (value == 0) {
		return PythonUDFType::NATIVE;
	} else if (value == 1) {
		return PythonUDFType::ARROW;
	} else {
		throw InvalidInputException("'%d' is not a recognized type for 'udf_type'", value);
	}
}

namespace PYBIND11_NAMESPACE {
namespace detail {

template <>
struct type_caster<PythonUDFType> : public type_caster_base<PythonUDFType> {
	using base = type_caster_base<PythonUDFType>;
	PythonUDFType tmp;

public:
	bool load(handle src, bool convert) {
		if (base::load(src, convert)) {
			return true;
		} else if (py::isinstance<py::str>(src)) {
			tmp = PythonUDFTypeFromString(py::str(src));
			value = &tmp;
			return true;
		} else if (py::isinstance<py::int_>(src)) {
			tmp = PythonUDFTypeFromInteger(src.cast<int64_t>());
			value = &tmp;
			return true;
		}
		return false;
	}

	static handle cast(PythonUDFType src, return_value_policy policy, handle parent) {
		return base::cast(src, policy, parent);
	}
};

} // namespace detail
} // namespace PYBIND11_NAMESPACE
