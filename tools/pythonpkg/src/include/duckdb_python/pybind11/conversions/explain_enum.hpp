#pragma once

#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::ExplainType;
using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

namespace py = pybind11;

static ExplainType ExplainTypeFromString(const string &type) {
	auto ltype = StringUtil::Lower(type);
	if (ltype.empty() || ltype == "standard") {
		return ExplainType::EXPLAIN_STANDARD;
	} else if (ltype == "analyze") {
		return ExplainType::EXPLAIN_ANALYZE;
	} else {
		throw InvalidInputException("Unrecognized type for 'explain'");
	}
}

static ExplainType ExplainTypeFromInteger(int64_t value) {
	if (value == 0) {
		return ExplainType::EXPLAIN_STANDARD;
	} else if (value == 1) {
		return ExplainType::EXPLAIN_ANALYZE;
	} else {
		throw InvalidInputException("Unrecognized type for 'explain'");
	}
}

namespace PYBIND11_NAMESPACE {
namespace detail {

template <>
struct type_caster<ExplainType> : public type_caster_base<ExplainType> {
	using base = type_caster_base<ExplainType>;
	ExplainType tmp;

public:
	bool load(handle src, bool convert) {
		if (base::load(src, convert)) {
			return true;
		} else if (py::isinstance<py::str>(src)) {
			tmp = ExplainTypeFromString(py::str(src));
			value = &tmp;
			return true;
		} else if (py::isinstance<py::int_>(src)) {
			tmp = ExplainTypeFromInteger(src.cast<int64_t>());
			value = &tmp;
			return true;
		}
		return false;
	}

	static handle cast(ExplainType src, return_value_policy policy, handle parent) {
		return base::cast(src, policy, parent);
	}
};

} // namespace detail
} // namespace PYBIND11_NAMESPACE
