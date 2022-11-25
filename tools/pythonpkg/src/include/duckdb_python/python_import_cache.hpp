//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/python_object_container.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb_python/python_object_container.hpp"

#include "datetime.h" //From python

namespace duckdb {

struct PythonImportCache;

struct PythonImportCacheItem {
public:
	PythonImportCacheItem() : object(nullptr) {
	}
	virtual ~PythonImportCacheItem() {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) {
	}

public:
	bool IsLoaded() const;
	bool IsInstance(py::handle object) const;
	py::handle operator()(void) const;
	void LoadModule(const string &name, PythonImportCache &cache);
	void LoadAttribute(const string &name, PythonImportCache &cache, PythonImportCacheItem &source);

protected:
	virtual bool IsRequired() const {
		return true;
	}

private:
	PyObject *AddCache(PythonImportCache &cache, py::object object);

private:
	//! The stored item
	PyObject *object;
};

//===--------------------------------------------------------------------===//
// Modules
//===--------------------------------------------------------------------===//

struct IPythonDisplayCacheItem : public PythonImportCacheItem {
public:
	~IPythonDisplayCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		display.LoadAttribute("display", cache, *this);
	}

public:
	PythonImportCacheItem display;
};

struct IPythonCacheItem : public PythonImportCacheItem {
public:
	~IPythonCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		get_ipython.LoadAttribute("get_ipython", cache, *this);
		display.LoadModule("IPython.display", cache);
	}

public:
	PythonImportCacheItem get_ipython;
	IPythonDisplayCacheItem display;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

struct PandasLibsCacheItem : public PythonImportCacheItem {
public:
	~PandasLibsCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		NAType.LoadAttribute("NAType", cache, *this);
	}

public:
	PythonImportCacheItem NAType;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

struct PandasCacheItem : public PythonImportCacheItem {
public:
	~PandasCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		DataFrame.LoadAttribute("DataFrame", cache, *this);
		libs.LoadModule("pandas._libs.missing", cache);
	}

public:
	//! pandas.DataFrame
	PythonImportCacheItem DataFrame;
	PandasLibsCacheItem libs;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

struct ArrowLibCacheItem : public PythonImportCacheItem {
public:
	~ArrowLibCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		Table.LoadAttribute("Table", cache, *this);
		RecordBatchReader.LoadAttribute("RecordBatchReader", cache, *this);
	}

public:
	PythonImportCacheItem Table;
	PythonImportCacheItem RecordBatchReader;
};

struct ArrowDatasetCacheItem : public PythonImportCacheItem {
public:
	~ArrowDatasetCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		Dataset.LoadAttribute("Dataset", cache, *this);
		Scanner.LoadAttribute("Scanner", cache, *this);
	}

public:
	PythonImportCacheItem Dataset;
	PythonImportCacheItem Scanner;
};

struct ArrowCacheItem : public PythonImportCacheItem {
public:
	~ArrowCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		lib.LoadAttribute("lib", cache, *this);
		dataset.LoadModule("pyarrow.dataset", cache);
	}

public:
	ArrowLibCacheItem lib;
	ArrowDatasetCacheItem dataset;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

struct DecimalCacheItem : public PythonImportCacheItem {
public:
	~DecimalCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		Decimal.LoadAttribute("Decimal", cache, *this);
	}

public:
	//! decimal.Decimal
	PythonImportCacheItem Decimal;
};

struct UUIDCacheItem : public PythonImportCacheItem {
public:
	~UUIDCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		UUID.LoadAttribute("UUID", cache, *this);
	}

public:
	//! uuid.UUID
	PythonImportCacheItem UUID;
};

struct NumpyCacheItem : public PythonImportCacheItem {
public:
	~NumpyCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		ndarray.LoadAttribute("ndarray", cache, *this);
	}

public:
	PythonImportCacheItem ndarray;
};

struct DatetimeCacheItem : public PythonImportCacheItem {
public:
	~DatetimeCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		datetime.LoadAttribute("datetime", cache, *this);
		date.LoadAttribute("date", cache, *this);
		time.LoadAttribute("time", cache, *this);
		timedelta.LoadAttribute("timedelta", cache, *this);
	}

public:
	PythonImportCacheItem datetime;
	PythonImportCacheItem date;
	PythonImportCacheItem time;
	PythonImportCacheItem timedelta;
};

//===--------------------------------------------------------------------===//
// PythonImportCache
//===--------------------------------------------------------------------===//

struct PythonImportCache {
public:
	explicit PythonImportCache() {
		py::gil_scoped_acquire acquire;
		numpy.LoadModule("numpy", *this);
		datetime.LoadModule("datetime", *this);
		decimal.LoadModule("decimal", *this);
		uuid.LoadModule("uuid", *this);
		pandas.LoadModule("pandas", *this);
		arrow.LoadModule("pyarrow", *this);
		IPython.LoadModule("IPython", *this);
	}
	~PythonImportCache();

public:
	NumpyCacheItem numpy;
	DatetimeCacheItem datetime;
	DecimalCacheItem decimal;
	UUIDCacheItem uuid;
	PandasCacheItem pandas;
	ArrowCacheItem arrow;
	IPythonCacheItem IPython;

public:
	PyObject *AddCache(py::object item);

private:
	vector<py::object> owned_objects;
};

} // namespace duckdb
