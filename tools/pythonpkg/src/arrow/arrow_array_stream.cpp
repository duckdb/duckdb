#include "duckdb_python/arrow/arrow_array_stream.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyresult.hpp"

namespace duckdb {

void TransformDuckToArrowChunk(ArrowSchema &arrow_schema, ArrowArray &data, py::list &batches) {
	py::gil_assert();
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
	auto batch_import_func = pyarrow_lib_module.attr("RecordBatch").attr("_import_from_c");
	batches.append(batch_import_func(reinterpret_cast<uint64_t>(&data), reinterpret_cast<uint64_t>(&arrow_schema)));
}

void VerifyArrowDatasetLoaded() {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	if (!import_cache.arrow_dataset().IsLoaded()) {
		throw InvalidInputException("Optional module 'pyarrow.dataset' is required to perform this action");
	}
}

PyArrowObjectType GetArrowType(const py::handle &obj) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	// First Verify Lib Types
	auto table_class = import_cache.arrow_lib().Table();
	auto record_batch_reader_class = import_cache.arrow_lib().RecordBatchReader();
	if (py::isinstance(obj, table_class)) {
		return PyArrowObjectType::Table;
	} else if (py::isinstance(obj, record_batch_reader_class)) {
		return PyArrowObjectType::RecordBatchReader;
	}
	// Then Verify dataset types
	auto dataset_class = import_cache.arrow_dataset().Dataset();
	auto scanner_class = import_cache.arrow_dataset().Scanner();

	if (py::isinstance(obj, scanner_class)) {
		return PyArrowObjectType::Scanner;
	} else if (py::isinstance(obj, dataset_class)) {
		return PyArrowObjectType::Dataset;
	}
	return PyArrowObjectType::Invalid;
}

py::object PythonTableArrowArrayStreamFactory::ProduceScanner(py::object &arrow_scanner, py::handle &arrow_obj_handle,
                                                              ArrowStreamParameters &parameters,
                                                              const ClientProperties &client_properties) {
	auto filters = parameters.filters;
	auto &column_list = parameters.projected_columns.columns;
	bool has_filter = filters && !filters->filters.empty();
	py::list projection_list = py::cast(column_list);
	if (has_filter) {
		auto filter = TransformFilter(*filters, parameters.projected_columns.projection_map, client_properties);
		if (column_list.empty()) {
			return arrow_scanner(arrow_obj_handle, py::arg("filter") = filter);
		} else {
			return arrow_scanner(arrow_obj_handle, py::arg("columns") = projection_list, py::arg("filter") = filter);
		}
	} else {
		if (column_list.empty()) {
			return arrow_scanner(arrow_obj_handle);
		} else {
			return arrow_scanner(arrow_obj_handle, py::arg("columns") = projection_list);
		}
	}
}
unique_ptr<ArrowArrayStreamWrapper> PythonTableArrowArrayStreamFactory::Produce(uintptr_t factory_ptr,
                                                                                ArrowStreamParameters &parameters) {
	py::gil_scoped_acquire acquire;
	auto factory = static_cast<PythonTableArrowArrayStreamFactory *>(reinterpret_cast<void *>(factory_ptr));
	D_ASSERT(factory->arrow_object);
	py::handle arrow_obj_handle(factory->arrow_object);
	auto arrow_object_type = GetArrowType(arrow_obj_handle);

	py::object scanner;
	py::object arrow_batch_scanner = py::module_::import("pyarrow.dataset").attr("Scanner").attr("from_batches");
	switch (arrow_object_type) {
	case PyArrowObjectType::Table: {
		auto arrow_dataset = py::module_::import("pyarrow.dataset").attr("dataset");
		auto dataset = arrow_dataset(arrow_obj_handle);
		py::object arrow_scanner = dataset.attr("__class__").attr("scanner");
		scanner = ProduceScanner(arrow_scanner, dataset, parameters, factory->client_properties);
		break;
	}
	case PyArrowObjectType::RecordBatchReader: {
		scanner = ProduceScanner(arrow_batch_scanner, arrow_obj_handle, parameters, factory->client_properties);
		break;
	}
	case PyArrowObjectType::Scanner: {
		// If it's a scanner we have to turn it to a record batch reader, and then a scanner again since we can't stack
		// scanners on arrow Otherwise pushed-down projections and filters will disappear like tears in the rain
		auto record_batches = arrow_obj_handle.attr("to_reader")();
		scanner = ProduceScanner(arrow_batch_scanner, record_batches, parameters, factory->client_properties);
		break;
	}
	case PyArrowObjectType::Dataset: {
		py::object arrow_scanner = arrow_obj_handle.attr("__class__").attr("scanner");
		scanner = ProduceScanner(arrow_scanner, arrow_obj_handle, parameters, factory->client_properties);
		break;
	}
	default: {
		auto py_object_type = string(py::str(arrow_obj_handle.get_type().attr("__name__")));
		throw InvalidInputException("Object of type '%s' is not a recognized Arrow object", py_object_type);
	}
	}

	auto record_batches = scanner.attr("to_reader")();
	auto res = make_uniq<ArrowArrayStreamWrapper>();
	auto export_to_c = record_batches.attr("_export_to_c");
	export_to_c(reinterpret_cast<uint64_t>(&res->arrow_array_stream));
	return res;
}

void PythonTableArrowArrayStreamFactory::GetSchema(uintptr_t factory_ptr, ArrowSchemaWrapper &schema) {
	py::gil_scoped_acquire acquire;
	auto factory = static_cast<PythonTableArrowArrayStreamFactory *>(reinterpret_cast<void *>(factory_ptr));
	D_ASSERT(factory->arrow_object);
	auto table_class = py::module::import("pyarrow").attr("Table");
	py::handle arrow_obj_handle(factory->arrow_object);
	if (py::isinstance(arrow_obj_handle, table_class)) {
		auto obj_schema = arrow_obj_handle.attr("schema");
		auto export_to_c = obj_schema.attr("_export_to_c");
		export_to_c(reinterpret_cast<uint64_t>(&schema));
		return;
	}

	VerifyArrowDatasetLoaded();

	auto scanner_class = py::module::import("pyarrow.dataset").attr("Scanner");

	if (py::isinstance(arrow_obj_handle, scanner_class)) {
		auto obj_schema = arrow_obj_handle.attr("projected_schema");
		auto export_to_c = obj_schema.attr("_export_to_c");
		export_to_c(reinterpret_cast<uint64_t>(&schema));
	} else {
		auto obj_schema = arrow_obj_handle.attr("schema");
		auto export_to_c = obj_schema.attr("_export_to_c");
		export_to_c(reinterpret_cast<uint64_t>(&schema));
	}
}

py::object GetScalar(Value &constant, const string &timezone_config) {
	py::object scalar = py::module_::import("pyarrow").attr("scalar");
	py::object dataset_scalar = py::module_::import("pyarrow.dataset").attr("scalar");
	py::object scalar_value;
	switch (constant.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return dataset_scalar(constant.GetValue<bool>());
	case LogicalTypeId::TINYINT:
		return dataset_scalar(constant.GetValue<int8_t>());
	case LogicalTypeId::SMALLINT:
		return dataset_scalar(constant.GetValue<int16_t>());
	case LogicalTypeId::INTEGER:
		return dataset_scalar(constant.GetValue<int32_t>());
	case LogicalTypeId::BIGINT:
		return dataset_scalar(constant.GetValue<int64_t>());
	case LogicalTypeId::DATE: {
		py::object date_type = py::module_::import("pyarrow").attr("date32");
		return dataset_scalar(scalar(constant.GetValue<int32_t>(), date_type()));
	}
	case LogicalTypeId::TIME: {
		py::object date_type = py::module_::import("pyarrow").attr("time64");
		return dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type("us")));
	}
	case LogicalTypeId::TIMESTAMP: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type("us")));
	}
	case LogicalTypeId::TIMESTAMP_MS: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type("ms")));
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type("ns")));
	}
	case LogicalTypeId::TIMESTAMP_SEC: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type("s")));
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type("us", py::arg("tz") = timezone_config)));
	}
	case LogicalTypeId::UTINYINT:
		return dataset_scalar(constant.GetValue<uint8_t>());
	case LogicalTypeId::USMALLINT:
		return dataset_scalar(constant.GetValue<uint16_t>());
	case LogicalTypeId::UINTEGER:
		return dataset_scalar(constant.GetValue<uint32_t>());
	case LogicalTypeId::UBIGINT:
		return dataset_scalar(constant.GetValue<uint64_t>());
	case LogicalTypeId::FLOAT:
		return dataset_scalar(constant.GetValue<float>());
	case LogicalTypeId::DOUBLE:
		return dataset_scalar(constant.GetValue<double>());
	case LogicalTypeId::VARCHAR:
		return dataset_scalar(constant.ToString());
	case LogicalTypeId::BLOB:
		return dataset_scalar(constant.GetValueUnsafe<string>());
	case LogicalTypeId::DECIMAL: {
		py::object date_type = py::module_::import("pyarrow").attr("decimal128");
		uint8_t width;
		uint8_t scale;
		constant.type().GetDecimalProperties(width, scale);
		switch (constant.type().InternalType()) {
		case PhysicalType::INT16:
			return dataset_scalar(scalar(constant.GetValue<int16_t>(), date_type(width, scale)));
		case PhysicalType::INT32:
			return dataset_scalar(scalar(constant.GetValue<int32_t>(), date_type(width, scale)));
		case PhysicalType::INT64:
			return dataset_scalar(scalar(constant.GetValue<int64_t>(), date_type(width, scale)));
		default: {
			auto hugeint_value = constant.GetValue<hugeint_t>();
			auto hugeint_value_py = py::cast(hugeint_value.upper);
			hugeint_value_py = hugeint_value_py.attr("__mul__")(NumericLimits<uint64_t>::Maximum());
			hugeint_value_py = hugeint_value_py.attr("__add__")(hugeint_value.lower);
			return dataset_scalar(scalar(hugeint_value_py, date_type(width, scale)));
		}
		}
	}
	default:
		throw NotImplementedException("Unimplemented type \"%s\" for Arrow Filter Pushdown",
		                              constant.type().ToString());
	}
}

py::object TransformFilterRecursive(TableFilter *filter, const string &column_name, const string &timezone_config) {
	py::object field = py::module_::import("pyarrow.dataset").attr("field");
	switch (filter->filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter->Cast<ConstantFilter>();
		auto constant_field = field(column_name);
		auto constant_value = GetScalar(constant_filter.constant, timezone_config);
		switch (constant_filter.comparison_type) {
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
		auto &or_filter = filter->Cast<ConjunctionOrFilter>();
		//! Get first non null filter type
		auto child_filter = or_filter.child_filters[i++].get();
		py::object expression = TransformFilterRecursive(child_filter, column_name, timezone_config);
		while (i < or_filter.child_filters.size()) {
			child_filter = or_filter.child_filters[i++].get();
			py::object child_expression = TransformFilterRecursive(child_filter, column_name, timezone_config);
			expression = expression.attr("__or__")(child_expression);
		}
		return expression;
	}
	case TableFilterType::CONJUNCTION_AND: {
		idx_t i = 0;
		auto &and_filter = filter->Cast<ConjunctionAndFilter>();
		auto child_filter = and_filter.child_filters[i++].get();
		py::object expression = TransformFilterRecursive(child_filter, column_name, timezone_config);
		while (i < and_filter.child_filters.size()) {
			child_filter = and_filter.child_filters[i++].get();
			py::object child_expression = TransformFilterRecursive(child_filter, column_name, timezone_config);
			expression = expression.attr("__and__")(child_expression);
		}
		return expression;
	}
	default:
		throw NotImplementedException("Pushdown Filter Type not supported in Arrow Scans");
	}
}

py::object PythonTableArrowArrayStreamFactory::TransformFilter(TableFilterSet &filter_collection,
                                                               std::unordered_map<idx_t, string> &columns,
                                                               const ClientProperties &config) {
	auto filters_map = &filter_collection.filters;
	auto it = filters_map->begin();
	D_ASSERT(columns.find(it->first) != columns.end());
	py::object expression = TransformFilterRecursive(it->second.get(), columns[it->first], config.time_zone);
	while (it != filters_map->end()) {
		py::object child_expression = TransformFilterRecursive(it->second.get(), columns[it->first], config.time_zone);
		expression = expression.attr("__and__")(child_expression);
		it++;
	}
	return expression;
}

} // namespace duckdb
