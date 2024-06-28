#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/enum_util.hpp"

using duckdb::InvalidInputException;
using duckdb::RenderMode;
using duckdb::string;
using duckdb::StringUtil;

namespace py = pybind11;

static RenderMode RenderModeFromInteger(int64_t value) {
	if (value == 0) {
		return RenderMode::ROWS;
	} else if (value == 1) {
		return RenderMode::COLUMNS;
	} else {
		throw InvalidInputException("Unrecognized type for 'render_mode'");
	}
}

namespace PYBIND11_NAMESPACE {
namespace detail {

template <>
struct type_caster<RenderMode> : public type_caster_base<RenderMode> {
	using base = type_caster_base<RenderMode>;
	RenderMode tmp;

public:
	bool load(handle src, bool convert) {
		if (base::load(src, convert)) {
			return true;
		} else if (py::isinstance<py::str>(src)) {
			string render_mode_str = py::str(src);
			auto render_mode =
			    duckdb::EnumUtil::FromString<RenderMode>(render_mode_str.empty() ? "ROWS" : render_mode_str);
			value = &render_mode;
			return true;
		} else if (py::isinstance<py::int_>(src)) {
			tmp = RenderModeFromInteger(src.cast<int64_t>());
			value = &tmp;
			return true;
		}
		return false;
	}

	static handle cast(RenderMode src, return_value_policy policy, handle parent) {
		return base::cast(src, policy, parent);
	}
};

} // namespace detail
} // namespace PYBIND11_NAMESPACE
