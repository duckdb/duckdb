//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyconnection/pyconnection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb_python/arrow/arrow_array_stream.hpp"
#include "duckdb.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb_python/import_cache/python_import_cache.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb_python/pyrelation.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/path_like.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb_python/pyfilesystem.hpp"
#include "duckdb_python/pybind11/registered_py_object.hpp"
#include "duckdb_python/python_dependency.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb_python/pybind11/conversions/exception_handling_enum.hpp"
#include "duckdb_python/pybind11/conversions/python_udf_type_enum.hpp"
#include "duckdb_python/pybind11/conversions/python_csv_line_terminator_enum.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
struct BoundParameterData;

enum class PythonEnvironmentType { NORMAL, INTERACTIVE, JUPYTER };

struct DuckDBPyRelation;

class RegisteredArrow : public RegisteredObject {

public:
	RegisteredArrow(unique_ptr<PythonTableArrowArrayStreamFactory> arrow_factory_p, py::object obj_p)
	    : RegisteredObject(std::move(obj_p)), arrow_factory(std::move(arrow_factory_p)) {};
	unique_ptr<PythonTableArrowArrayStreamFactory> arrow_factory;
};

struct DefaultConnectionHolder {
public:
	DefaultConnectionHolder() {
	}
	~DefaultConnectionHolder() {
	}

public:
	DefaultConnectionHolder(const DefaultConnectionHolder &other) = delete;
	DefaultConnectionHolder(DefaultConnectionHolder &&other) = delete;
	DefaultConnectionHolder &operator=(const DefaultConnectionHolder &other) = delete;
	DefaultConnectionHolder &operator=(DefaultConnectionHolder &&other) = delete;

public:
	shared_ptr<DuckDBPyConnection> Get();
	void Set(shared_ptr<DuckDBPyConnection> conn);

private:
	shared_ptr<DuckDBPyConnection> connection;
	mutex l;
};

struct ConnectionGuard {
public:
	ConnectionGuard() {
	}
	~ConnectionGuard() {
	}

public:
	DuckDB &GetDatabase() {
		if (!database) {
			ThrowConnectionException();
		}
		return *database;
	}
	const DuckDB &GetDatabase() const {
		if (!database) {
			ThrowConnectionException();
		}
		return *database;
	}
	Connection &GetConnection() {
		if (!connection) {
			ThrowConnectionException();
		}
		return *connection;
	}
	const Connection &GetConnection() const {
		if (!connection) {
			ThrowConnectionException();
		}
		return *connection;
	}
	DuckDBPyRelation &GetResult() {
		if (!result) {
			ThrowConnectionException();
		}
		return *result;
	}
	const DuckDBPyRelation &GetResult() const {
		if (!result) {
			ThrowConnectionException();
		}
		return *result;
	}

public:
	bool HasResult() const {
		return result != nullptr;
	}

public:
	void SetDatabase(shared_ptr<DuckDB> db) {
		database = std::move(db);
	}
	void SetDatabase(ConnectionGuard &con) {
		if (!con.database) {
			ThrowConnectionException();
		}
		database = con.database;
	}
	void SetConnection(unique_ptr<Connection> con) {
		connection = std::move(con);
	}
	void SetResult(unique_ptr<DuckDBPyRelation> res) {
		result = std::move(res);
	}

private:
	void ThrowConnectionException() const {
		throw ConnectionException("Connection already closed!");
	}

private:
	shared_ptr<DuckDB> database;
	unique_ptr<Connection> connection;
	unique_ptr<DuckDBPyRelation> result;
};

struct DuckDBPyConnection : public enable_shared_from_this<DuckDBPyConnection> {
private:
	class Cursors {
	public:
		Cursors() {
		}

	public:
		void AddCursor(shared_ptr<DuckDBPyConnection> conn);
		void ClearCursors();

	private:
		mutex lock;
		vector<weak_ptr<DuckDBPyConnection>> cursors;
	};

public:
	ConnectionGuard con;
	Cursors cursors;
	std::mutex py_connection_lock;
	//! MemoryFileSystem used to temporarily store file-like objects for reading
	shared_ptr<ModifiedMemoryFileSystem> internal_object_filesystem;
	case_insensitive_map_t<unique_ptr<ExternalDependency>> registered_functions;
	case_insensitive_set_t registered_objects;

public:
	explicit DuckDBPyConnection() {
	}
	~DuckDBPyConnection();

public:
	static void Initialize(py::handle &m);
	static void Cleanup();

	shared_ptr<DuckDBPyConnection> Enter();

	static void Exit(DuckDBPyConnection &self, const py::object &exc_type, const py::object &exc,
	                 const py::object &traceback);

	static bool DetectAndGetEnvironment();
	static bool IsJupyter();
	static shared_ptr<DuckDBPyConnection> DefaultConnection();
	static void SetDefaultConnection(shared_ptr<DuckDBPyConnection> conn);
	static PythonImportCache *ImportCache();
	static bool IsInteractive();

	unique_ptr<DuckDBPyRelation> ReadCSV(const py::object &name, py::kwargs &kwargs);

	py::list ExtractStatements(const string &query);

	unique_ptr<DuckDBPyRelation> ReadJSON(
	    const py::object &name, const Optional<py::object> &columns = py::none(),
	    const Optional<py::object> &sample_size = py::none(), const Optional<py::object> &maximum_depth = py::none(),
	    const Optional<py::str> &records = py::none(), const Optional<py::str> &format = py::none(),
	    const Optional<py::object> &date_format = py::none(), const Optional<py::object> &timestamp_format = py::none(),
	    const Optional<py::object> &compression = py::none(),
	    const Optional<py::object> &maximum_object_size = py::none(),
	    const Optional<py::object> &ignore_errors = py::none(),
	    const Optional<py::object> &convert_strings_to_integers = py::none(),
	    const Optional<py::object> &field_appearance_threshold = py::none(),
	    const Optional<py::object> &map_inference_threshold = py::none(),
	    const Optional<py::object> &maximum_sample_files = py::none(),
	    const Optional<py::object> &filename = py::none(), const Optional<py::object> &hive_partitioning = py::none(),
	    const Optional<py::object> &union_by_name = py::none(), const Optional<py::object> &hive_types = py::none(),
	    const Optional<py::object> &hive_types_autocast = py::none());

	shared_ptr<DuckDBPyType> MapType(const shared_ptr<DuckDBPyType> &key_type,
	                                 const shared_ptr<DuckDBPyType> &value_type);
	shared_ptr<DuckDBPyType> StructType(const py::object &fields);
	shared_ptr<DuckDBPyType> ListType(const shared_ptr<DuckDBPyType> &type);
	shared_ptr<DuckDBPyType> ArrayType(const shared_ptr<DuckDBPyType> &type, idx_t size);
	shared_ptr<DuckDBPyType> UnionType(const py::object &members);
	shared_ptr<DuckDBPyType> EnumType(const string &name, const shared_ptr<DuckDBPyType> &type,
	                                  const py::list &values_p);
	shared_ptr<DuckDBPyType> DecimalType(int width, int scale);
	shared_ptr<DuckDBPyType> StringType(const string &collation = string());
	shared_ptr<DuckDBPyType> Type(const string &type_str);

	shared_ptr<DuckDBPyConnection>
	RegisterScalarUDF(const string &name, const py::function &udf, const py::object &arguments = py::none(),
	                  const shared_ptr<DuckDBPyType> &return_type = nullptr, PythonUDFType type = PythonUDFType::NATIVE,
	                  FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                  PythonExceptionHandling exception_handling = PythonExceptionHandling::FORWARD_ERROR,
	                  bool side_effects = false);

	shared_ptr<DuckDBPyConnection> UnregisterUDF(const string &name);

	shared_ptr<DuckDBPyConnection> ExecuteMany(const py::object &query, py::object params = py::list());

	void ExecuteImmediately(vector<unique_ptr<SQLStatement>> statements);
	unique_ptr<PreparedStatement> PrepareQuery(unique_ptr<SQLStatement> statement);
	unique_ptr<QueryResult> ExecuteInternal(PreparedStatement &prep, py::object params = py::list());
	unique_ptr<QueryResult> PrepareAndExecuteInternal(unique_ptr<SQLStatement> statement,
	                                                  py::object params = py::list());

	shared_ptr<DuckDBPyConnection> Execute(const py::object &query, py::object params = py::list());
	shared_ptr<DuckDBPyConnection> ExecuteFromString(const string &query);

	shared_ptr<DuckDBPyConnection> Append(const string &name, const PandasDataFrame &value, bool by_name);

	shared_ptr<DuckDBPyConnection> RegisterPythonObject(const string &name, const py::object &python_object);

	void InstallExtension(const string &extension, bool force_install = false,
	                      const py::object &repository = py::none(), const py::object &repository_url = py::none(),
	                      const py::object &version = py::none());

	void LoadExtension(const string &extension);

	unique_ptr<DuckDBPyRelation> RunQuery(const py::object &query, string alias = "", py::object params = py::list());

	unique_ptr<DuckDBPyRelation> Table(const string &tname);

	unique_ptr<DuckDBPyRelation> Values(const py::args &params);

	unique_ptr<DuckDBPyRelation> View(const string &vname);

	unique_ptr<DuckDBPyRelation> TableFunction(const string &fname, py::object params = py::list());

	unique_ptr<DuckDBPyRelation> FromDF(const PandasDataFrame &value);

	unique_ptr<DuckDBPyRelation> FromParquet(const string &file_glob, bool binary_as_string, bool file_row_number,
	                                         bool filename, bool hive_partitioning, bool union_by_name,
	                                         const py::object &compression = py::none());

	unique_ptr<DuckDBPyRelation> FromParquets(const vector<string> &file_globs, bool binary_as_string,
	                                          bool file_row_number, bool filename, bool hive_partitioning,
	                                          bool union_by_name, const py::object &compression = py::none());

	unique_ptr<DuckDBPyRelation> FromArrow(py::object &arrow_object);

	unique_ptr<DuckDBPyRelation> FromSubstrait(py::bytes &proto);

	unique_ptr<DuckDBPyRelation> GetSubstrait(const string &query, bool enable_optimizer = true);

	unique_ptr<DuckDBPyRelation> GetSubstraitJSON(const string &query, bool enable_optimizer = true);

	unique_ptr<DuckDBPyRelation> FromSubstraitJSON(const string &json);

	unordered_set<string> GetTableNames(const string &query);

	shared_ptr<DuckDBPyConnection> UnregisterPythonObject(const string &name);

	shared_ptr<DuckDBPyConnection> Begin();

	shared_ptr<DuckDBPyConnection> Commit();

	shared_ptr<DuckDBPyConnection> Rollback();

	shared_ptr<DuckDBPyConnection> Checkpoint();

	void Close();

	void Interrupt();

	ModifiedMemoryFileSystem &GetObjectFileSystem();

	// cursor() is stupid
	shared_ptr<DuckDBPyConnection> Cursor();

	Optional<py::list> GetDescription();

	int GetRowcount();

	// these should be functions on the result but well
	Optional<py::tuple> FetchOne();

	py::list FetchMany(idx_t size);

	py::list FetchAll();

	py::dict FetchNumpy();
	PandasDataFrame FetchDF(bool date_as_object);
	PandasDataFrame FetchDFChunk(const idx_t vectors_per_chunk = 1, bool date_as_object = false);

	duckdb::pyarrow::Table FetchArrow(idx_t rows_per_batch);
	PolarsDataFrame FetchPolars(idx_t rows_per_batch);

	py::dict FetchPyTorch();

	py::dict FetchTF();

	duckdb::pyarrow::RecordBatchReader FetchRecordBatchReader(const idx_t rows_per_batch);

	static shared_ptr<DuckDBPyConnection> Connect(const py::object &database, bool read_only, const py::dict &config);

	static vector<Value> TransformPythonParamList(const py::handle &params);
	static case_insensitive_map_t<BoundParameterData> TransformPythonParamDict(const py::dict &params);

	void RegisterFilesystem(AbstractFileSystem filesystem);
	void UnregisterFilesystem(const py::str &name);
	py::list ListFilesystems();
	bool FileSystemIsRegistered(const string &name);

	//! Default connection to an in-memory database
	static DefaultConnectionHolder default_connection;
	//! Caches and provides an interface to get frequently used modules+subtypes
	static shared_ptr<PythonImportCache> import_cache;

	static bool IsPandasDataframe(const py::object &object);
	static bool IsPolarsDataframe(const py::object &object);
	static PyArrowObjectType GetArrowType(const py::handle &obj);
	static bool IsAcceptedArrowObject(const py::object &object);
	static NumpyObjectType IsAcceptedNumpyObject(const py::object &object);

	static unique_ptr<QueryResult> CompletePendingQuery(PendingQueryResult &pending_query);

private:
	PathLike GetPathLike(const py::object &object);
	ScalarFunction CreateScalarUDF(const string &name, const py::function &udf, const py::object &parameters,
	                               const shared_ptr<DuckDBPyType> &return_type, bool vectorized,
	                               FunctionNullHandling null_handling, PythonExceptionHandling exception_handling,
	                               bool side_effects);
	void RegisterArrowObject(const py::object &arrow_object, const string &name);
	vector<unique_ptr<SQLStatement>> GetStatements(const py::object &query);

	static PythonEnvironmentType environment;
	static void DetectEnvironment();
};

template <typename T>
static bool ModuleIsLoaded() {
	auto dict = pybind11::module_::import("sys").attr("modules");
	return dict.contains(py::str(T::Name));
}

} // namespace duckdb
