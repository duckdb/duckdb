#include "duckdb_python/arrow/arrow_array_stream.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pyresult.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {

void TransformDuckToArrowChunk(ArrowSchema &arrow_schema, ArrowArray &data, py::list &batches) {
	py::gil_assert();
	auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
	auto batch_import_func = pyarrow_lib_module.attr("RecordBatch").attr("_import_from_c");
	batches.append(batch_import_func(reinterpret_cast<uint64_t>(&data), reinterpret_cast<uint64_t>(&arrow_schema)));
}

void VerifyArrowDatasetLoaded() {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	if (!import_cache.pyarrow.dataset() || !ModuleIsLoaded<PyarrowDatasetCacheItem>()) {
		throw InvalidInputException("Optional module 'pyarrow.dataset' is required to perform this action");
	}
}

py::object PythonTableArrowArrayStreamFactory::ProduceScanner(DBConfig &config, py::object &arrow_scanner,
                                                              py::handle &arrow_obj_handle,
                                                              ArrowStreamParameters &parameters,
                                                              const ClientProperties &client_properties) {
	D_ASSERT(!py::isinstance<py::capsule>(arrow_obj_handle));
	ArrowSchemaWrapper schema;
	PythonTableArrowArrayStreamFactory::GetSchemaInternal(arrow_obj_handle, schema);
	vector<string> unused_names;
	vector<LogicalType> unused_types;
	ArrowTableType arrow_table;
	ArrowTableFunction::PopulateArrowTableType(config, arrow_table, schema, unused_names, unused_types);

	auto filters = parameters.filters;
	auto &column_list = parameters.projected_columns.columns;
	auto &filter_to_col = parameters.projected_columns.filter_to_col;
	py::list projection_list = py::cast(column_list);

	bool has_filter = filters && !filters->filters.empty();
	py::dict kwargs;
	if (!column_list.empty()) {
		kwargs["columns"] = projection_list;
	}

	if (has_filter) {
		auto filter = TransformFilter(*filters, parameters.projected_columns.projection_map, filter_to_col,
		                              client_properties, arrow_table);
		if (!filter.is(py::none())) {
			kwargs["filter"] = filter;
		}
	}
	return arrow_scanner(arrow_obj_handle, **kwargs);
}

unique_ptr<ArrowArrayStreamWrapper> PythonTableArrowArrayStreamFactory::Produce(uintptr_t factory_ptr,
                                                                                ArrowStreamParameters &parameters) {
	py::gil_scoped_acquire acquire;
	auto factory = static_cast<PythonTableArrowArrayStreamFactory *>(reinterpret_cast<void *>(factory_ptr)); // NOLINT
	D_ASSERT(factory->arrow_object);
	py::handle arrow_obj_handle(factory->arrow_object);
	auto arrow_object_type = DuckDBPyConnection::GetArrowType(arrow_obj_handle);

	if (arrow_object_type == PyArrowObjectType::PyCapsule) {
		auto res = make_uniq<ArrowArrayStreamWrapper>();
		auto capsule = py::reinterpret_borrow<py::capsule>(arrow_obj_handle);
		auto stream = capsule.get_pointer<struct ArrowArrayStream>();
		if (!stream->release) {
			throw InternalException("ArrowArrayStream was released by another thread/library");
		}
		res->arrow_array_stream = *stream;
		stream->release = nullptr;
		return res;
	}

	auto &import_cache = *DuckDBPyConnection::ImportCache();
	py::object scanner;
	py::object arrow_batch_scanner = import_cache.pyarrow.dataset.Scanner().attr("from_batches");
	switch (arrow_object_type) {
	case PyArrowObjectType::Table: {
		auto arrow_dataset = import_cache.pyarrow.dataset().attr("dataset");
		auto dataset = arrow_dataset(arrow_obj_handle);
		py::object arrow_scanner = dataset.attr("__class__").attr("scanner");
		scanner = ProduceScanner(factory->config, arrow_scanner, dataset, parameters, factory->client_properties);
		break;
	}
	case PyArrowObjectType::RecordBatchReader: {
		scanner = ProduceScanner(factory->config, arrow_batch_scanner, arrow_obj_handle, parameters,
		                         factory->client_properties);
		break;
	}
	case PyArrowObjectType::Scanner: {
		// If it's a scanner we have to turn it to a record batch reader, and then a scanner again since we can't stack
		// scanners on arrow Otherwise pushed-down projections and filters will disappear like tears in the rain
		auto record_batches = arrow_obj_handle.attr("to_reader")();
		scanner = ProduceScanner(factory->config, arrow_batch_scanner, record_batches, parameters,
		                         factory->client_properties);
		break;
	}
	case PyArrowObjectType::Dataset: {
		py::object arrow_scanner = arrow_obj_handle.attr("__class__").attr("scanner");
		scanner =
		    ProduceScanner(factory->config, arrow_scanner, arrow_obj_handle, parameters, factory->client_properties);
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

void PythonTableArrowArrayStreamFactory::GetSchemaInternal(py::handle arrow_obj_handle, ArrowSchemaWrapper &schema) {
	if (py::isinstance<py::capsule>(arrow_obj_handle)) {
		auto capsule = py::reinterpret_borrow<py::capsule>(arrow_obj_handle);
		auto stream = capsule.get_pointer<struct ArrowArrayStream>();
		if (!stream->release) {
			throw InternalException("ArrowArrayStream was released by another thread/library");
		}
		stream->get_schema(stream, &schema.arrow_schema);
		return;
	}

	auto table_class = py::module::import("pyarrow").attr("Table");
	if (py::isinstance(arrow_obj_handle, table_class)) {
		auto obj_schema = arrow_obj_handle.attr("schema");
		auto export_to_c = obj_schema.attr("_export_to_c");
		export_to_c(reinterpret_cast<uint64_t>(&schema.arrow_schema));
		return;
	}

	VerifyArrowDatasetLoaded();

	auto &import_cache = *DuckDBPyConnection::ImportCache();
	auto scanner_class = import_cache.pyarrow.dataset.Scanner();

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

void PythonTableArrowArrayStreamFactory::GetSchema(uintptr_t factory_ptr, ArrowSchemaWrapper &schema) {
	py::gil_scoped_acquire acquire;
	auto factory = static_cast<PythonTableArrowArrayStreamFactory *>(reinterpret_cast<void *>(factory_ptr)); // NOLINT
	D_ASSERT(factory->arrow_object);
	py::handle arrow_obj_handle(factory->arrow_object);
	GetSchemaInternal(arrow_obj_handle, schema);
}

string ConvertTimestampUnit(ArrowDateTimeType unit) {
	switch (unit) {
	case ArrowDateTimeType::MICROSECONDS:
		return "us";
	case ArrowDateTimeType::MILLISECONDS:
		return "ms";
	case ArrowDateTimeType::NANOSECONDS:
		return "ns";
	case ArrowDateTimeType::SECONDS:
		return "s";
	default:
		throw NotImplementedException("DatetimeType not recognized in ConvertTimestampUnit: %d", (int)unit);
	}
}

int64_t ConvertTimestampTZValue(int64_t base_value, ArrowDateTimeType datetime_type) {
	auto input = timestamp_t(base_value);
	if (!Timestamp::IsFinite(input)) {
		return base_value;
	}

	switch (datetime_type) {
	case ArrowDateTimeType::MICROSECONDS:
		return Timestamp::GetEpochMicroSeconds(input);
	case ArrowDateTimeType::MILLISECONDS:
		return Timestamp::GetEpochMs(input);
	case ArrowDateTimeType::NANOSECONDS:
		return Timestamp::GetEpochNanoSeconds(input);
	case ArrowDateTimeType::SECONDS:
		return Timestamp::GetEpochSeconds(input);
	default:
		throw NotImplementedException("DatetimeType not recognized in ConvertTimestampTZValue");
	}
}

py::object GetScalar(Value &constant, const string &timezone_config, const ArrowType &type) {
	py::object scalar = py::module_::import("pyarrow").attr("scalar");
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	py::object dataset_scalar = import_cache.pyarrow.dataset().attr("scalar");
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
		auto &datetime_info = type.GetTypeInfo<ArrowDateTimeInfo>();
		auto base_value = constant.GetValue<int64_t>();
		auto arrow_datetime_type = datetime_info.GetDateTimeType();
		auto time_unit_string = ConvertTimestampUnit(arrow_datetime_type);
		auto converted_value = ConvertTimestampTZValue(base_value, arrow_datetime_type);
		py::object date_type = py::module_::import("pyarrow").attr("timestamp");
		return dataset_scalar(scalar(converted_value, date_type(time_unit_string, py::arg("tz") = timezone_config)));
	}
	case LogicalTypeId::UTINYINT: {
		py::object integer_type = py::module_::import("pyarrow").attr("uint8");
		return dataset_scalar(scalar(constant.GetValue<uint8_t>(), integer_type()));
	}
	case LogicalTypeId::USMALLINT: {
		py::object integer_type = py::module_::import("pyarrow").attr("uint16");
		return dataset_scalar(scalar(constant.GetValue<uint16_t>(), integer_type()));
	}
	case LogicalTypeId::UINTEGER: {
		py::object integer_type = py::module_::import("pyarrow").attr("uint32");
		return dataset_scalar(scalar(constant.GetValue<uint32_t>(), integer_type()));
	}
	case LogicalTypeId::UBIGINT: {
		py::object integer_type = py::module_::import("pyarrow").attr("uint64");
		return dataset_scalar(scalar(constant.GetValue<uint64_t>(), integer_type()));
	}
	case LogicalTypeId::FLOAT:
		return dataset_scalar(constant.GetValue<float>());
	case LogicalTypeId::DOUBLE:
		return dataset_scalar(constant.GetValue<double>());
	case LogicalTypeId::VARCHAR:
		return dataset_scalar(constant.ToString());
	case LogicalTypeId::BLOB:
		return dataset_scalar(py::bytes(constant.GetValueUnsafe<string>()));
	case LogicalTypeId::DECIMAL: {
		py::object decimal_type = py::module_::import("pyarrow").attr("decimal128");
		uint8_t width;
		uint8_t scale;
		constant.type().GetDecimalProperties(width, scale);
		// pyarrow only allows 'decimal.Decimal' to be used to construct decimal scalars such as 0.05
		auto val = import_cache.decimal.Decimal()(constant.ToString());
		return dataset_scalar(
		    scalar(std::move(val), decimal_type(py::arg("precision") = width, py::arg("scale") = scale)));
	}
	default:
		throw NotImplementedException("Unimplemented type \"%s\" for Arrow Filter Pushdown",
		                              constant.type().ToString());
	}
}

py::object TransformFilterRecursive(TableFilter &filter, vector<string> column_ref, const string &timezone_config,
                                    const ArrowType &type) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	py::object field = import_cache.pyarrow.dataset().attr("field");
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		auto constant_field = field(py::tuple(py::cast(column_ref)));
		auto constant_value = GetScalar(constant_filter.constant, timezone_config, type);
		switch (constant_filter.comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			return constant_field.attr("__eq__")(constant_value);
		case ExpressionType::COMPARE_LESSTHAN:
			return constant_field.attr("__lt__")(constant_value);
		case ExpressionType::COMPARE_GREATERTHAN:
			return constant_field.attr("__gt__")(constant_value);
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return constant_field.attr("__le__")(constant_value);
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return constant_field.attr("__ge__")(constant_value);
		case ExpressionType::COMPARE_NOTEQUAL:
			return constant_field.attr("__ne__")(constant_value);
		default:
			throw NotImplementedException("Comparison Type can't be an Arrow Scan Pushdown Filter");
		}
	}
	//! We do not pushdown is null yet
	case TableFilterType::IS_NULL: {
		auto constant_field = field(py::tuple(py::cast(column_ref)));
		return constant_field.attr("is_null")();
	}
	case TableFilterType::IS_NOT_NULL: {
		auto constant_field = field(py::tuple(py::cast(column_ref)));
		return constant_field.attr("is_valid")();
	}
	//! We do not pushdown or conjunctions yet
	case TableFilterType::CONJUNCTION_OR: {
		auto &or_filter = filter.Cast<ConjunctionOrFilter>();
		py::object expression = py::none();
		for (idx_t i = 0; i < or_filter.child_filters.size(); i++) {
			auto &child_filter = *or_filter.child_filters[i];
			py::object child_expression = TransformFilterRecursive(child_filter, column_ref, timezone_config, type);
			if (child_expression.is(py::none())) {
				continue;
			}
			if (expression.is(py::none())) {
				expression = std::move(child_expression);
			} else {
				expression = expression.attr("__or__")(child_expression);
			}
		}
		return expression;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &and_filter = filter.Cast<ConjunctionAndFilter>();
		py::object expression = py::none();
		for (idx_t i = 0; i < and_filter.child_filters.size(); i++) {
			auto &child_filter = *and_filter.child_filters[i];
			py::object child_expression = TransformFilterRecursive(child_filter, column_ref, timezone_config, type);
			if (child_expression.is(py::none())) {
				continue;
			}
			if (expression.is(py::none())) {
				expression = std::move(child_expression);
			} else {
				expression = expression.attr("__and__")(child_expression);
			}
		}
		return expression;
	}
	case TableFilterType::STRUCT_EXTRACT: {
		auto &struct_filter = filter.Cast<StructFilter>();
		auto &child_name = struct_filter.child_name;
		auto &struct_type_info = type.GetTypeInfo<ArrowStructInfo>();
		auto &struct_child_type = struct_type_info.GetChild(struct_filter.child_idx);

		column_ref.push_back(child_name);
		auto child_expr = TransformFilterRecursive(*struct_filter.child_filter, std::move(column_ref), timezone_config,
		                                           struct_child_type);
		return child_expr;
	}
	case TableFilterType::OPTIONAL_FILTER:
		return py::none();
	default:
		throw NotImplementedException("Pushdown Filter Type not supported in Arrow Scans");
	}
}

py::object PythonTableArrowArrayStreamFactory::TransformFilter(TableFilterSet &filter_collection,
                                                               std::unordered_map<idx_t, string> &columns,
                                                               unordered_map<idx_t, idx_t> filter_to_col,
                                                               const ClientProperties &config,
                                                               const ArrowTableType &arrow_table) {
	auto &filters_map = filter_collection.filters;

	py::object expression = py::none();
	for (auto &it : filters_map) {
		auto column_idx = it.first;
		auto &column_name = columns[column_idx];

		vector<string> column_ref;
		column_ref.push_back(column_name);

		D_ASSERT(columns.find(column_idx) != columns.end());

		auto &arrow_type = arrow_table.GetColumns().at(filter_to_col.at(column_idx));
		py::object child_expression = TransformFilterRecursive(*it.second, column_ref, config.time_zone, *arrow_type);
		if (child_expression.is(py::none())) {
			continue;
		} else if (expression.is(py::none())) {
			expression = std::move(child_expression);
		} else {
			expression = expression.attr("__and__")(child_expression);
		}
	}
	return expression;
}

} // namespace duckdb
