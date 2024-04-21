#include "duckdb_python/python_replacement_scan.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/pybind11/dataframe.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb_python/pandas/pandas_scan.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb_python/pyrelation.hpp"

namespace duckdb {

static void CreateArrowScan(const string &name, py::object entry, TableFunctionRef &table_function,
                            vector<unique_ptr<ParsedExpression>> &children, ClientProperties &client_properties) {
	auto stream_factory = make_uniq<PythonTableArrowArrayStreamFactory>(entry.ptr(), client_properties);
	auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
	auto stream_factory_get_schema = PythonTableArrowArrayStreamFactory::GetSchema;

	children.push_back(make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(stream_factory.get()))));
	children.push_back(make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(stream_factory_produce))));
	children.push_back(make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(stream_factory_get_schema))));

	table_function.function = make_uniq<FunctionExpression>("arrow_scan", std::move(children));
	auto dependency = make_uniq<ExternalDependency>();
	auto dependency_item = PythonDependencyItem::Create(make_uniq<RegisteredArrow>(std::move(stream_factory), entry));
	dependency->AddDependency("replacement_cache", std::move(dependency_item));
	table_function.external_dependency = std::move(dependency);
}

static unique_ptr<TableRef> TryReplacementObject(const py::object &entry, const string &name,
                                                 ClientProperties &client_properties) {
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	NumpyObjectType numpytype; // Identify the type of accepted numpy objects.
	if (DuckDBPyConnection::IsPandasDataframe(entry)) {
		if (PandasDataFrame::IsPyArrowBacked(entry)) {
			auto table = PandasDataFrame::ToArrowTable(entry);
			CreateArrowScan(name, table, *table_function, children, client_properties);
		} else {
			string name = "df_" + StringUtil::GenerateRandomName();
			auto new_df = PandasScanFunction::PandasReplaceCopiedNames(entry);
			children.push_back(make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(new_df.ptr()))));
			table_function->function = make_uniq<FunctionExpression>("pandas_scan", std::move(children));
			auto dependency = make_uniq<ExternalDependency>();
			dependency->AddDependency("replacement_cache", PythonDependencyItem::Create(entry));
			dependency->AddDependency("copy", PythonDependencyItem::Create(new_df));
			table_function->external_dependency = std::move(dependency);
		}
	} else if (DuckDBPyConnection::IsAcceptedArrowObject(entry)) {
		CreateArrowScan(name, entry, *table_function, children, client_properties);
	} else if (DuckDBPyRelation::IsRelation(entry)) {
		auto pyrel = py::cast<DuckDBPyRelation *>(entry);
		// create a subquery from the underlying relation object
		auto select = make_uniq<SelectStatement>();
		select->node = pyrel->GetRel().GetQueryNode();
		auto subquery = make_uniq<SubqueryRef>(std::move(select));
		auto dependency = make_uniq<ExternalDependency>();
		dependency->AddDependency("replacement_cache", PythonDependencyItem::Create(entry));
		subquery->external_dependency = std::move(dependency);
		return std::move(subquery);
	} else if (PolarsDataFrame::IsDataFrame(entry)) {
		auto arrow_dataset = entry.attr("to_arrow")();
		CreateArrowScan(name, arrow_dataset, *table_function, children, client_properties);
	} else if (PolarsDataFrame::IsLazyFrame(entry)) {
		auto materialized = entry.attr("collect")();
		auto arrow_dataset = materialized.attr("to_arrow")();
		CreateArrowScan(name, arrow_dataset, *table_function, children, client_properties);
	} else if ((numpytype = DuckDBPyConnection::IsAcceptedNumpyObject(entry)) != NumpyObjectType::INVALID) {
		string name = "np_" + StringUtil::GenerateRandomName();
		py::dict data; // we will convert all the supported format to dict{"key": np.array(value)}.
		size_t idx = 0;
		switch (numpytype) {
		case NumpyObjectType::NDARRAY1D:
			data["column0"] = entry;
			break;
		case NumpyObjectType::NDARRAY2D:
			idx = 0;
			for (auto item : py::cast<py::array>(entry)) {
				data[("column" + std::to_string(idx)).c_str()] = item;
				idx++;
			}
			break;
		case NumpyObjectType::LIST:
			idx = 0;
			for (auto item : py::cast<py::list>(entry)) {
				data[("column" + std::to_string(idx)).c_str()] = item;
				idx++;
			}
			break;
		case NumpyObjectType::DICT:
			data = py::cast<py::dict>(entry);
			break;
		default:
			throw NotImplementedException("Unsupported Numpy object");
			break;
		}
		children.push_back(make_uniq<ConstantExpression>(Value::POINTER(CastPointerToValue(data.ptr()))));
		table_function->function = make_uniq<FunctionExpression>("pandas_scan", std::move(children));
		auto dependency = make_uniq<ExternalDependency>();
		dependency->AddDependency("replacement_cache", PythonDependencyItem::Create(entry));
		dependency->AddDependency("data", PythonDependencyItem::Create(data));
		table_function->external_dependency = std::move(dependency);
	} else {
		// This throws an error later on!
		return nullptr;
	}
	return std::move(table_function);
}

static unique_ptr<TableRef> TryReplacement(py::dict &dict, const string &name, ClientProperties &client_properties,
                                           py::object &current_frame) {
	auto table_name = py::str(name);
	if (!dict.contains(table_name)) {
		// not present in the globals
		return nullptr;
	}
	const py::object &entry = dict[table_name];
	auto result = TryReplacementObject(entry, name, client_properties);
	if (!result) {
		std::string location = py::cast<py::str>(current_frame.attr("f_code").attr("co_filename"));
		location += ":";
		location += py::cast<py::str>(current_frame.attr("f_lineno"));
		std::string cpp_table_name = table_name;
		auto py_object_type = string(py::str(entry.get_type().attr("__name__")));

		throw InvalidInputException(
		    "Python Object \"%s\" of type \"%s\" found on line \"%s\" not suitable for replacement scans.\nMake sure "
		    "that \"%s\" is either a pandas.DataFrame, duckdb.DuckDBPyRelation, pyarrow Table, Dataset, "
		    "RecordBatchReader, Scanner, or NumPy ndarrays with supported format",
		    cpp_table_name, py_object_type, location, cpp_table_name);
	}
	return result;
}

static unique_ptr<TableRef> ReplaceInternal(ClientContext &context, const string &table_name) {
	py::gil_scoped_acquire acquire;
	// Here we do an exhaustive search on the frame lineage
	auto current_frame = py::module::import("inspect").attr("currentframe")();
	auto client_properties = context.GetClientProperties();
	while (hasattr(current_frame, "f_locals")) {
		auto local_dict = py::reinterpret_borrow<py::dict>(current_frame.attr("f_locals"));
		// search local dictionary
		if (local_dict) {
			auto result = TryReplacement(local_dict, table_name, client_properties, current_frame);
			if (result) {
				return result;
			}
		}
		// search global dictionary
		auto global_dict = py::reinterpret_borrow<py::dict>(current_frame.attr("f_globals"));
		if (global_dict) {
			auto result = TryReplacement(global_dict, table_name, client_properties, current_frame);
			if (result) {
				return result;
			}
		}
		current_frame = current_frame.attr("f_back");
	}
	// Not found :(
	return nullptr;
}
unique_ptr<TableRef> PythonReplacementScan::Replace(ClientContext &context, ReplacementScanInput &input,
                                                    ReplacementScanData *data) {
	auto &table_name = input.table_name;

	// TODO: look up the replacement scan cache from the TableRef

	auto &table_ref = input.ref;
	if (table_ref.external_dependency) {
		auto dependency_item = table_ref.external_dependency->GetDependency("replacement_cache");
		if (dependency_item && dependency_item->type == ExternalDependencyItemType::PYTHON_DEPENDENCY) {
			auto &python_dependency = dependency_item->Cast<PythonDependencyItem>();
			auto &registered_object = *python_dependency.object;
			auto &py_object = registered_object.obj;
			auto client_properties = context.GetClientProperties();
			auto result = TryReplacementObject(py_object, table_name, client_properties);
			// This was cached, so it was successful before, it should be successfull now
			D_ASSERT(result);
			return std::move(result);
		}
	}

	unique_ptr<TableRef> result;
	result = ReplaceInternal(context, table_name);
	if (!result) {
		return nullptr;
	}
	//! a ProxyDependencies object should have been created
	D_ASSERT(table_ref.external_dependency);
	D_ASSERT(result->external_dependency);

	auto dependency_item = result->external_dependency->GetDependency("replacement_cache");
	D_ASSERT(dependency_item && dependency_item->type == ExternalDependencyItemType::PYTHON_DEPENDENCY);
	table_ref.external_dependency->AddDependency("replacement_cache", std::move(dependency_item));
	return result;
}

} // namespace duckdb
