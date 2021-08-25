#include "duckdb/common/assert.hpp"
#include "include/duckdb_python/arrow_array_stream.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"

namespace duckdb {

unique_ptr<ArrowArrayStreamWrapper> PythonTableArrowArrayStreamFactory::Produce(
    uintptr_t factory_ptr, std::pair<std::unordered_map<idx_t, string>, std::vector<string>> &project_columns,
    TableFilterCollection *filters) {
	py::gil_scoped_acquire acquire;
	PythonTableArrowArrayStreamFactory *factory = (PythonTableArrowArrayStreamFactory *)factory_ptr;
	D_ASSERT(factory->arrow_object);
	py::handle arrow_obj_handle(factory->arrow_object);
	py::object scanner;
	py::object arrow_scanner = py::module_::import("pyarrow.dataset").attr("Scanner").attr("from_dataset");
	auto py_object_type = string(py::str(arrow_obj_handle.get_type().attr("__name__")));
	py::list projection_list = py::cast(project_columns.second);
	bool has_filter = filters && filters->table_filters && !filters->table_filters->filters.empty();
	if (py_object_type == "Table") {
		auto arrow_dataset = py::module_::import("pyarrow.dataset").attr("dataset");
		auto dataset = arrow_dataset(arrow_obj_handle);
		if (project_columns.second.empty()) {
			//! This is only called at the binder to get the schema
			scanner = arrow_scanner(dataset);
		} else {
			if (has_filter) {
				auto filter = TransformFilter(*filters, project_columns.first);
				scanner = arrow_scanner(dataset, py::arg("columns") = projection_list, py::arg("filter") = filter);
			} else {
				scanner = arrow_scanner(dataset, py::arg("columns") = projection_list);
			}
		}

	} else {
		if (project_columns.second.empty()) {
			//! This is only called at the binder to get the schema
			scanner = arrow_scanner(arrow_obj_handle);
		} else {
			if (has_filter) {
				auto filter = TransformFilter(*filters, project_columns.first);
				scanner =
				    arrow_scanner(arrow_obj_handle, py::arg("columns") = projection_list, py::arg("filter") = filter);
			} else {
				scanner = arrow_scanner(arrow_obj_handle, py::arg("columns") = projection_list);
			}
		}
	}
	auto record_batches = scanner.attr("to_reader")();
	auto res = make_unique<ArrowArrayStreamWrapper>();
	auto export_to_c = record_batches.attr("_export_to_c");
	export_to_c((uint64_t)&res->arrow_array_stream);
	return res;
}

py::object GetScalar(Value &constant) {
	py::object scalar = py::module_::import("pyarrow").attr("scalar");
	py::object dataset_scalar = py::module_::import("pyarrow.dataset").attr("scalar");
	py::object scalar_value;
	switch (constant.type().id()) {
	case LogicalTypeId::BOOLEAN:
		scalar_value = dataset_scalar(constant.GetValue<bool>());
		return scalar_value;
	case LogicalTypeId::TINYINT:
		scalar_value = dataset_scalar(constant.GetValue<int8_t>());
		return scalar_value;
	case LogicalTypeId::SMALLINT:
		scalar_value = dataset_scalar(constant.GetValue<int16_t>());
		return scalar_value;
	case LogicalTypeId::INTEGER:
		scalar_value = dataset_scalar(constant.GetValue<int32_t>());
		return scalar_value;
	case LogicalTypeId::BIGINT:
		scalar_value = dataset_scalar(constant.GetValue<int64_t>());
		return scalar_value;
	case LogicalTypeId::DATE: {
		py::object date_type = py::module_::import("pyarrow").attr("date32");
		scalar_value = dataset_scalar(scalar(constant.GetValue<int32_t>(), date_type()));
		return scalar_value;
	}
	case LogicalTypeId::TIME: {
		py::object date_type = py::module_::import("pyarrow").attr("time64");
		scalar_value = dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type("us")));
		return scalar_value;
	}
	case LogicalTypeId::TIMESTAMP: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		scalar_value = dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type("us")));
		return scalar_value;
	}
	case LogicalTypeId::UTINYINT:
		scalar_value = dataset_scalar(constant.GetValue<uint8_t>());
		return scalar_value;
	case LogicalTypeId::USMALLINT:
		scalar_value = dataset_scalar(constant.GetValue<uint16_t>());
		return scalar_value;
	case LogicalTypeId::UINTEGER:
		scalar_value = dataset_scalar(constant.GetValue<uint32_t>());
		return scalar_value;
	case LogicalTypeId::UBIGINT:
		scalar_value = dataset_scalar(constant.GetValue<uint64_t>());
		return scalar_value;
	case LogicalTypeId::FLOAT:
		scalar_value = dataset_scalar(constant.GetValue<float>());
		return scalar_value;
	case LogicalTypeId::DOUBLE:
		scalar_value = dataset_scalar(constant.GetValue<double>());
		return scalar_value;
	case LogicalTypeId::VARCHAR:
		scalar_value = dataset_scalar(constant.ToString());
		return scalar_value;
	case LogicalTypeId::DECIMAL: {
		py::object date_type = py::module_::import("pyarrow").attr("decimal128");
		uint8_t width;
		uint8_t scale;
		constant.type().GetDecimalProperties(width, scale);
		switch (constant.type().InternalType()) {
		case PhysicalType::INT16:
			scalar_value = dataset_scalar(scalar(constant.GetValue<int16_t>(), date_type(width, scale)));
			return scalar_value;
		case PhysicalType::INT32:
			scalar_value = dataset_scalar(scalar(constant.GetValue<int32_t>(), date_type(width, scale)));
			return scalar_value;
		case PhysicalType::INT64:
			scalar_value = dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type(width, scale)));
			return scalar_value;
		default:
			scalar_value = dataset_scalar(scalar(constant.GetValue<hugeint_t>(), date_type(width, scale)));
			return scalar_value;
		}
	}
	default:
		throw NotImplementedException("Unimplemented type \"%s\" for Arrow Filter Pushdown",
		                              constant.type().ToString());
	}
}

py::object TransformFilterRecursive(TableFilter *filter, const string &column_name) {

	py::object field = py::module_::import("pyarrow.dataset").attr("field");
	switch (filter->filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto constant_filter = (ConstantFilter *)filter;
		auto constant_field = field(column_name);
		auto constant_value = GetScalar(constant_filter->constant);
		switch (constant_filter->comparison_type) {
		case ExpressionType::COMPARE_EQUAL: {
			return constant_field.attr("__eq__")(constant_value);
		}
		case ExpressionType::COMPARE_LESSTHAN: {
			return constant_field.attr("__lt__")(constant_value);
		}
		case ExpressionType::COMPARE_GREATERTHAN: {
			return constant_field.attr("__gt__")(constant_value);
		}
		case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
			return constant_field.attr("__le__")(constant_value);
		}
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
			return constant_field.attr("__ge__")(constant_value);
		}
		default:
			throw NotImplementedException("Comparison Type can't be an Arrow Scan Pushdown Filter");
		}
	}
	//! We do not pushdown is null yet
	case TableFilterType::IS_NULL: {
		auto constant_field = field(column_name);
		return constant_field.attr("is_null")();
	}
	case TableFilterType::IS_NOT_NULL: {
		auto constant_field = field(column_name);
		return constant_field.attr("is_valid")();
	}
	//! We do not pushdown or conjuctions yet
	case TableFilterType::CONJUNCTION_OR: {
		idx_t i = 0;
		auto or_filter = (ConjunctionOrFilter *)filter;
		//! Get first non null filter type
		auto child_filter = or_filter->child_filters[i++].get();
		py::object expression = TransformFilterRecursive(child_filter, column_name);
		while (i < or_filter->child_filters.size()) {
			child_filter = or_filter->child_filters[i++].get();
			py::object child_expression = TransformFilterRecursive(child_filter, column_name);
			expression = expression.attr("__or__")(child_expression);
		}
		return expression;
	}
	case TableFilterType::CONJUNCTION_AND: {
		idx_t i = 0;
		auto and_filter = (ConjunctionAndFilter *)filter;
		auto child_filter = and_filter->child_filters[i++].get();
		py::object expression = TransformFilterRecursive(child_filter, column_name);
		while (i < and_filter->child_filters.size()) {
			child_filter = and_filter->child_filters[i++].get();
			py::object child_expression = TransformFilterRecursive(child_filter, column_name);
			expression = expression.attr("__and__")(child_expression);
		}
		return expression;
	}
	default:
		throw NotImplementedException("Pushdown Filter Type not supported in Arrow Scans");
	}
}

py::object PythonTableArrowArrayStreamFactory::TransformFilter(TableFilterCollection &filter_collection,
                                                               std::unordered_map<idx_t, string> &columns) {
	auto filters_map = &filter_collection.table_filters->filters;
	auto it = filters_map->begin();
	D_ASSERT(columns.find(it->first) != columns.end());
	py::object expression = TransformFilterRecursive(it->second.get(), columns[it->first]);
	while (it != filters_map->end()) {
		py::object child_expression = TransformFilterRecursive(it->second.get(), columns[it->first]);
		expression = expression.attr("__and__")(child_expression);
		it++;
	}
	return expression;
}

} // namespace duckdb