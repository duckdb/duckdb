#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::FunctionNullHandling;
using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

namespace py = pybind11;

static FunctionNullHandling FunctionNullHandlingFromString(const string &type) {
	auto ltype = StringUtil::Lower(type);
	if (ltype.empty() || ltype == "default") {
		return FunctionNullHandling::DEFAULT_NULL_HANDLING;
	} else if (ltype == "special") {
		return FunctionNullHandling::SPECIAL_HANDLING;
	} else {
		throw InvalidInputException("'%s' is not a recognized type for 'null_handling'", type);
	}
}

static FunctionNullHandling FunctionNullHandlingFromInteger(int64_t value) {
	if (value == 0) {
		return FunctionNullHandling::DEFAULT_NULL_HANDLING;
	} else if (value == 1) {
		return FunctionNullHandling::SPECIAL_HANDLING;
	} else {
		throw InvalidInputException("'%d' is not a recognized type for 'null_handling'", value);
	}
}

namespace PYBIND11_NAMESPACE {
namespace detail {

template <>
struct type_caster<FunctionNullHandling> : public type_caster_base<FunctionNullHandling> {
	using base = type_caster_base<FunctionNullHandling>;
	FunctionNullHandling tmp;

public:
	bool load(handle src, bool convert) {
		if (base::load(src, convert)) {
			return true;
		} else if (py::isinstance<py::str>(src)) {
			tmp = FunctionNullHandlingFromString(py::str(src));
			value = &tmp;
			return true;
		} else if (py::isinstance<py::int_>(src)) {
			tmp = FunctionNullHandlingFromInteger(src.cast<int64_t>());
			value = &tmp;
			return true;
		}
		return false;
	}

	static handle cast(FunctionNullHandling src, return_value_policy policy, handle parent) {
		return base::cast(src, policy, parent);
	}
};

} // namespace detail
} // namespace PYBIND11_NAMESPACE
