#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "duckdb/main/secret/file_credentials.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <atomic>
#include <chrono>
#include <fstream>
#include <sstream>
#include <thread>

using namespace duckdb;

namespace {

//! The secret type the test file system reads, and one it does not
constexpr const char *TEST_TYPE = "file_credentials_test";
constexpr const char *OTHER_TYPE = "file_credentials_test_other";
//! What a read records when no secret and no setting give a key
constexpr const char *NO_KEY = "<none>";

duckdb::unique_ptr<BaseSecret> CreateTestSecret(ClientContext &context, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);
	for (auto &option : input.options) {
		secret->secret_map[Identifier(option.first)] = Value(option.second.ToString());
	}
	return std::move(secret);
}

void RegisterTestSecretType(DatabaseInstance &instance) {
	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {instance, extension_info, Identifier("file_credentials_test"), Identifier()};
	ExtensionLoader loader {load_info};
	SecretType secret_type;
	secret_type.name = Identifier(TEST_TYPE);
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	loader.RegisterSecretType(secret_type);
	CreateSecretFunction function = {TEST_TYPE, "config", CreateTestSecret};
	function.named_parameters["key_id"] = LogicalType::VARCHAR;
	function.named_parameters["region"] = LogicalType::VARCHAR;
	loader.RegisterFunction(function);
	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption("file_credentials_test_region", "region a read uses when no secret sets one",
	                          LogicalType::VARCHAR);
}

//! A secret of `type` with key id `key_id`
shared_ptr<const KeyValueSecret> TestSecret(const string &key_id, const char *type = TEST_TYPE) {
	auto secret = make_shared_ptr<KeyValueSecret>(vector<string>(), Identifier(type), Identifier("test"), Identifier());
	secret->secret_map[Identifier("key_id")] = Value(key_id);
	return std::move(secret);
}

//! One open, read or glob the test file system served, with the key id and region the secret reader gave it
struct Access {
	string kind;
	string path;
	string key_id;
	string region;
};

struct MemoryFileHandle : public FileHandle {
	MemoryFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags, string key_id_p)
	    : FileHandle(fs, path, flags), key_id(std::move(key_id_p)) {
	}
	void Close() override {
	}
	string key_id;
	idx_t position = 0;
};

//! Serves files from memory under mem://, and records every open, read and glob with the key id of the secret the
//! secret reader chose for it. A glob lists the files under the pattern's prefix, each with the glob's credentials,
//! or once with each of `listing_credentials` when that is set
class MemoryFileSystem : public FileSystem {
public:
	duckdb::map<string, string> files;
	bool listing_metadata = false;
	duckdb::vector<shared_ptr<FileCredentials>> listing_credentials;
	duckdb::mutex lock;
	duckdb::vector<Access> accesses;

	duckdb::vector<Access> Accesses(const string &kind) {
		duckdb::lock_guard<duckdb::mutex> guard(lock);
		duckdb::vector<Access> result;
		for (auto &access : accesses) {
			if (access.kind == kind) {
				result.push_back(access);
			}
		}
		return result;
	}
	idx_t CountKey(const string &kind, const string &key) {
		idx_t count = 0;
		for (auto &access : Accesses(kind)) {
			count += access.key_id == key;
		}
		return count;
	}
	void ClearAccesses() {
		duckdb::lock_guard<duckdb::mutex> guard(lock);
		accesses.clear();
	}
	string GetName() const override {
		return "MemoryFileSystem";
	}
	using FileSystem::OpenFile;
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		return OpenFileExtended(OpenFileInfo(path), flags, opener);
	}
	bool CanHandleFile(const string &path) override {
		return StringUtil::StartsWith(path, "mem://");
	}
	bool SupportsOpenFileExtended() const override {
		return true;
	}
	bool SupportsGlobExtended() const override {
		return true;
	}
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto &memory_handle = handle.Cast<MemoryFileHandle>();
		auto &data = files.at(handle.path);
		if (location + nr_bytes > data.size()) {
			throw IOException("Read past the end of \"%s\"", handle.path);
		}
		memcpy(buffer, data.data() + location, nr_bytes);
		Record("read", handle.path, memory_handle.key_id, "");
	}
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		auto &memory_handle = handle.Cast<MemoryFileHandle>();
		auto &data = files.at(handle.path);
		auto count = MinValue<int64_t>(nr_bytes, data.size() - memory_handle.position);
		Read(handle, buffer, count, memory_handle.position);
		memory_handle.position += count;
		return count;
	}
	int64_t GetFileSize(FileHandle &handle) override {
		return files.at(handle.path).size();
	}
	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return Timestamp::FromEpochSeconds(1000);
	}
	string GetVersionTag(FileHandle &handle) override {
		return "v1";
	}
	FileType GetFileType(FileHandle &handle) override {
		return FileType::FILE_TYPE_REGULAR;
	}
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	void Seek(FileHandle &handle, idx_t location) override {
		handle.Cast<MemoryFileHandle>().position = location;
	}
	idx_t SeekPosition(FileHandle &handle) override {
		return handle.Cast<MemoryFileHandle>().position;
	}
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return files.count(filename) > 0;
	}

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override {
		FileOpenerInfo info {file.path};
		if (file.extended_info) {
			info.credentials = file.extended_info->credentials;
		}
		string key_id;
		string region;
		ReadSecret(opener, info, key_id, region);
		Record("open", file.path, key_id, region);
		if (!files.count(file.path) && flags.ReturnNullIfNotExists()) {
			return nullptr;
		}
		if (!files.count(file.path)) {
			throw IOException("No file \"%s\"", file.path);
		}
		return make_uniq<MemoryFileHandle>(*this, file.path, flags, key_id);
	}
	unique_ptr<MultiFileList> GlobFilesExtended(const string &pattern, const FileGlobInput &input,
	                                            optional_ptr<FileOpener> opener) override {
		FileOpenerInfo info {pattern, input.credentials};
		string key_id;
		string region;
		ReadSecret(opener, info, key_id, region);
		Record("glob", pattern, key_id, region);
		auto star = pattern.find('*');
		auto listed_prefix = pattern.substr(0, star);
		auto suffix = star == string::npos ? string() : pattern.substr(pattern.rfind('*') + 1);
		vector<OpenFileInfo> result;
		for (auto &entry : files) {
			auto &path = entry.first;
			bool matches = star == string::npos
			                   ? path == pattern
			                   : StringUtil::StartsWith(path, listed_prefix) && StringUtil::EndsWith(path, suffix);
			if (!matches) {
				continue;
			}
			auto all_credentials = listing_credentials;
			if (all_credentials.empty()) {
				all_credentials.push_back(input.credentials);
			}
			for (auto &credentials : all_credentials) {
				OpenFileInfo listed(path);
				listed.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
				listed.extended_info->credentials = credentials;
				if (listing_metadata) {
					listed.extended_info->options["last_modified"] =
					    Value::TIMESTAMP(Timestamp::FromEpochSeconds(1000));
					listed.extended_info->options["etag"] = Value("v1");
				}
				result.push_back(std::move(listed));
			}
		}
		return make_uniq<SimpleMultiFileList>(std::move(result));
	}

private:
	void ReadSecret(optional_ptr<FileOpener> opener, FileOpenerInfo &info, string &key_id, string &region) {
		key_id = NO_KEY;
		region = NO_KEY;
		if (!opener) {
			return;
		}
		KeyValueSecretReader reader(*opener, &info, TEST_TYPE);
		Value value;
		if (reader.TryGetSecretKeyOrSetting("key_id", "file_credentials_test_key_id", value) && !value.IsNull()) {
			key_id = value.ToString();
		}
		if (reader.TryGetSecretKeyOrSetting("region", "file_credentials_test_region", value) && !value.IsNull()) {
			region = value.ToString();
		}
	}
	void Record(const string &kind, const string &path, const string &key_id, const string &region) {
		duckdb::lock_guard<duckdb::mutex> guard(lock);
		accesses.push_back(Access {kind, path, key_id, region});
	}
};

string ReadLocalFile(const string &path) {
	std::ifstream input(path, std::ios::binary);
	std::stringstream buffer;
	buffer << input.rdbuf();
	return buffer.str();
}

//! A database with the test secret type, mem:// files, and a secret matched to every mem:// path, which a read that
//! loses a file's credentials would use
struct CredentialsFixture {
	CredentialsFixture() : db(nullptr), con(db) {
		RegisterTestSecretType(*db.instance);
		auto fs_p = make_uniq<MemoryFileSystem>();
		fs = fs_p.get();
		db.instance->GetFileSystem().RegisterSubSystem(std::move(fs_p));
		fs->files["mem://bucket/table/a.bin"] = "abc";
		fs->files["mem://bucket/table/b.bin"] = "def";
		REQUIRE_NO_FAIL(con.Query("CREATE SECRET by_path (TYPE file_credentials_test, SCOPE 'mem://bucket/', "
		                          "KEY_ID 'path-key', REGION 'path-region')"));
	}
	ClientContext &Context() {
		return *con.context;
	}
	//! Credentials for mem://bucket/table/ whose vends return `key_id` and count in `vends`
	shared_ptr<FileCredentials> Credentials(const string &key_id, const char *type = TEST_TYPE) {
		return FileCredentials::Create(Context(), "mem://bucket/table/", [this, key_id, type](ClientContext &) {
			vends++;
			return TestSecret(key_id, type);
		});
	}
	static OpenFileInfo File(const string &path, const shared_ptr<FileCredentials> &credentials) {
		return OpenFileInfo(path).WithCredentials(credentials);
	}
	//! Open `file` through the client's file system and return the key id the open used
	string OpenKey(const OpenFileInfo &file) {
		auto handle = FileSystem::GetFileSystem(Context()).OpenFile(file, FileFlags::FILE_FLAGS_READ);
		return handle->Cast<MemoryFileHandle>().key_id;
	}
	//! Runs `scan` in a transaction with credentials of key `key_id` on every listed file, and the external file cache
	//! on; returns the row count
	idx_t ScanWith(const string &scan, const string &key_id = "table-key") {
		REQUIRE_NO_FAIL(con.Query("SET enable_external_file_cache = true"));
		con.BeginTransaction();
		fs->listing_credentials = {Credentials(key_id)};
		fs->ClearAccesses();
		auto result = con.Query(scan);
		fs->listing_credentials.clear();
		con.Rollback();
		REQUIRE_NO_FAIL(*result);
		return result->RowCount();
	}
	idx_t CachedEntries(const string &path) {
		auto result = con.Query("SELECT count(*) FROM duckdb_external_file_cache() WHERE path = '" + path + "'");
		REQUIRE_NO_FAIL(*result);
		return NumericCast<idx_t>(result->Fetch()->GetValue(0, 0).GetValue<int64_t>());
	}

	DuckDB db;
	Connection con;
	MemoryFileSystem *fs;
	std::atomic<idx_t> vends {0};
};

void RequireThrowsWith(const std::function<void()> &call, const string &message) {
	try {
		call();
	} catch (std::exception &ex) {
		ErrorData error(ex);
		INFO(error.Message());
		REQUIRE(StringUtil::Contains(error.Message(), message));
		return;
	}
	FAIL("expected an error containing: " << message);
}

} // namespace

TEST_CASE("An open with credentials uses their secret, and no secret or setting chosen by path", "[file_credentials]") {
	CredentialsFixture f;
	REQUIRE_NO_FAIL(f.con.Query("SET file_credentials_test_region = 'setting-region'"));
	f.con.BeginTransaction();
	auto credentials = f.Credentials("table-key");
	REQUIRE(f.OpenKey(CredentialsFixture::File("mem://bucket/table/a.bin", credentials)) == "table-key");
	// their secret sets no region: neither the secret matched to the path nor the setting fills it
	REQUIRE(f.fs->Accesses("open")[0].region == NO_KEY);
	// the same path without credentials reads with the secret matched to it
	REQUIRE(f.OpenKey(OpenFileInfo("mem://bucket/table/a.bin")) == "path-key");
	REQUIRE(f.fs->Accesses("open")[1].region == "path-region");
	f.con.Rollback();
}

TEST_CASE("Credentials of another type fail the read, and a reader of several types takes any of them",
          "[file_credentials]") {
	CredentialsFixture f;
	f.con.BeginTransaction();
	auto credentials = f.Credentials("other-key", OTHER_TYPE);
	RequireThrowsWith([&]() { f.OpenKey(CredentialsFixture::File("mem://bucket/table/a.bin", credentials)); },
	                  "are a secret of type \"file_credentials_test_other\", which this read does not take");
	ClientContextFileOpener opener(f.Context());
	FileOpenerInfo info {"mem://bucket/table/a.bin", credentials};
	const char *with_other[] = {TEST_TYPE, "FILE_CREDENTIALS_TEST_OTHER"};
	KeyValueSecretReader reader(opener, &info, with_other, 2);
	REQUIRE(reader.GetSecretKey("key_id").ToString() == "other-key");
}

TEST_CASE("A glob with credentials lists with them, and each file it lists carries them", "[file_credentials]") {
	CredentialsFixture f;
	f.fs->files["mem://bucket/table/dir/x.parquet"] = "x";
	f.con.BeginTransaction();
	auto credentials = f.Credentials("table-key");
	auto &client_fs = FileSystem::GetFileSystem(f.Context());
	FileGlobInput input(FileGlobOptions::DISALLOW_EMPTY);
	input.credentials = credentials;
	auto files = client_fs.GlobFiles("mem://bucket/table/*.bin", input);
	REQUIRE(files.size() == 2);
	REQUIRE(f.fs->Accesses("glob")[0].key_id == "table-key");
	for (auto &file : files) {
		REQUIRE(file.extended_info->credentials == credentials);
		REQUIRE(f.OpenKey(file) == "table-key");
	}
	// WithCredentials copies the extended information, and keeps credentials a file has
	auto own = f.Credentials("own-key");
	OpenFileInfo given("mem://bucket/table/a.bin");
	given.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	given.extended_info->options["file_size"] = Value::UBIGINT(3);
	auto with = given.WithCredentials(credentials);
	REQUIRE(with.extended_info->options.count("file_size") == 1);
	REQUIRE_FALSE(given.extended_info->credentials);
	REQUIRE(with.WithCredentials(own).extended_info->credentials == credentials);
	REQUIRE_FALSE(OpenFileInfo("mem://bucket/table/a.bin").WithCredentials(nullptr).extended_info);
}

TEST_CASE("A read with credentials neither fills nor is served from the external file cache or the Parquet "
          "metadata cache",
          "[file_credentials]") {
	CredentialsFixture f;
	if (!f.db.ExtensionIsLoaded("parquet")) {
		SKIP_TEST("parquet is not loaded");
		return;
	}
	auto local = TestCreatePath("file_credentials_cache.parquet");
	REQUIRE_NO_FAIL(f.con.Query("COPY (SELECT range AS i FROM range(1000)) TO '" + local + "' (FORMAT parquet)"));
	const string first = "mem://bucket/table/t.parquet";
	f.fs->files[first] = ReadLocalFile(local);
	f.fs->files["mem://bucket/table/t2.parquet"] = ReadLocalFile(local);
	f.fs->listing_metadata = true;
	REQUIRE_NO_FAIL(f.con.Query("SET cache_local_files = true"));
	REQUIRE_NO_FAIL(f.con.Query("SET parquet_metadata_cache = true"));
	REQUIRE_NO_FAIL(f.con.Query("SET threads = 1"));
	const string scan = "SELECT sum(i) FROM read_parquet('mem://bucket/table/*.parquet')";
	const string count = "SELECT count(*) FROM read_parquet('mem://bucket/table/*.parquet')";
	auto metadata_cached = [&]() {
		return ObjectCache::GetObjectCache(f.Context()).GetObject("parquet_metadata-" + first) != nullptr;
	};
	f.ScanWith(scan);
	auto uncached_reads = f.fs->CountKey("read", "table-key");
	REQUIRE(uncached_reads > 0);
	REQUIRE(f.CachedEntries(first) == 0);
	REQUIRE_FALSE(metadata_cached());
	// reads by path fill both caches, and a second read by path is served from them
	REQUIRE_NO_FAIL(f.con.Query(scan));
	REQUIRE(f.CachedEntries(first) > 0);
	REQUIRE(metadata_cached());
	f.fs->ClearAccesses();
	REQUIRE_NO_FAIL(f.con.Query(scan));
	REQUIRE(f.fs->CountKey("read", "path-key") == 0);
	// a read with credentials reads the files itself, as often as when nothing was cached
	f.ScanWith(scan);
	REQUIRE(f.fs->CountKey("read", "table-key") == uncached_reads);
	REQUIRE(f.fs->CountKey("read", "path-key") == 0);
	// and a count(*) is not answered from the cached metadata
	f.ScanWith(count);
	REQUIRE(f.fs->CountKey("read", "table-key") > 0);
	REQUIRE(f.fs->CountKey("read", "path-key") == 0);
}

TEST_CASE("read_csv, read_json and read_parquet open and read every listed file with its credentials",
          "[file_credentials]") {
	CredentialsFixture f;
	string csv = "i\n";
	string json;
	for (idx_t i = 0; i < 2000; i++) {
		csv += to_string(i) + "\n";
		json += "{\"i\": " + to_string(i) + "}\n";
	}
	f.fs->files["mem://bucket/table/t1.csv"] = csv;
	f.fs->files["mem://bucket/table/t2.csv"] = "i\n1\n2\n";
	f.fs->files["mem://bucket/table/t.json"] = json;
	duckdb::vector<std::pair<string, idx_t>> scans {{"SELECT i FROM read_csv('mem://bucket/table/t1.csv')", 2000},
	                                                {"SELECT i FROM read_csv('mem://bucket/table/*.csv')", 2002}};
	if (f.db.ExtensionIsLoaded("json")) {
		scans.emplace_back("SELECT i FROM read_json('mem://bucket/table/*.json')", 2000);
	}
	if (f.db.ExtensionIsLoaded("parquet")) {
		auto local = TestCreatePath("file_credentials_scan.parquet");
		REQUIRE_NO_FAIL(f.con.Query("COPY (SELECT range AS i FROM range(1000)) TO '" + local + "' (FORMAT parquet)"));
		auto data = ReadLocalFile(local);
		f.fs->files["mem://bucket/table/part=1/a.parquet"] = data;
		f.fs->files["mem://bucket/table/part=2/b.parquet"] = data;
		f.fs->files["mem://bucket/table/part=3/c.parquet"] = data;
		scans.emplace_back("SELECT i FROM read_parquet('mem://bucket/table/*/*.parquet')", 3000);
		// a filter on a partition prunes the listed files
		scans.emplace_back(
		    "SELECT i FROM read_parquet('mem://bucket/table/*/*.parquet', hive_partitioning = true) WHERE part = 2",
		    1000);
		for (auto function : {"parquet_metadata", "parquet_schema", "parquet_file_metadata"}) {
			scans.emplace_back(string("SELECT * FROM ") + function + "('mem://bucket/table/*/*.parquet')", 0);
		}
	}
	for (auto &scan : scans) {
		INFO(scan.first);
		auto rows = f.ScanWith(scan.first);
		if (scan.second) {
			REQUIRE(rows == scan.second);
		}
		REQUIRE(f.fs->Accesses("open").size() > 0);
		REQUIRE(f.fs->CountKey("open", "table-key") == f.fs->Accesses("open").size());
		REQUIRE(f.fs->CountKey("read", "table-key") == f.fs->Accesses("read").size());
		if (StringUtil::Contains(scan.first, "part = 2")) {
			// a pruned file is not read; the first is, for the schema
			for (auto &read : f.fs->Accesses("read")) {
				REQUIRE(read.path != "mem://bucket/table/part=3/c.parquet");
			}
		}
	}
	REQUIRE(f.CachedEntries("mem://bucket/table/t1.csv") == 0);
}

TEST_CASE("Credentials serve only their client and their scope, until their transaction ends", "[file_credentials]") {
	CredentialsFixture f;
	ClientContextFileOpener opener(f.Context());
	Connection other(f.db);
	ClientContextFileOpener other_opener(*other.context);
	string outcome;
	SECTION("commit") {
		outcome = "COMMIT";
	}
	SECTION("rollback") {
		outcome = "ROLLBACK";
	}
	SECTION("failed statement") {
		outcome = "SELECT error('fails')";
	}
	REQUIRE_NO_FAIL(f.con.Query("BEGIN"));
	auto credentials = f.Credentials("table-key");
	REQUIRE(credentials->GetSecret(&opener, "mem://bucket/table/a.bin"));
	for (auto &path : {"mem://bucket/other/c.bin", "mem://bucket/table", "mem://bucket/table_archive/a.bin",
	                   "mem://bucket/table/../other/c.bin", "mem://bucket/table/x/..", "mem://bucket/table/..\\c"}) {
		INFO(path);
		RequireThrowsWith([&]() { credentials->GetSecret(&opener, path); }, "cannot be used for");
	}
	// another connection of the database, and an opener without a client
	RequireThrowsWith([&]() { credentials->GetSecret(&other_opener, "mem://bucket/table/a.bin"); },
	                  "serve only the connection that created them");
	RequireThrowsWith([&]() { credentials->GetSecret(nullptr, "mem://bucket/table/a.bin"); },
	                  "serve only the connection that created them");
	f.con.Query(outcome);
	if (f.Context().transaction.HasActiveTransaction()) {
		REQUIRE_NO_FAIL(f.con.Query("ROLLBACK"));
	}
	RequireThrowsWith([&]() { credentials->GetSecret(&opener, "mem://bucket/table/a.bin"); },
	                  "ended with the transaction that created them");
	REQUIRE_FALSE(credentials->Refresh(&opener));
	// an open that carries them fails rather than falling back to the secret matched to the path
	f.con.BeginTransaction();
	RequireThrowsWith([&]() { f.OpenKey(CredentialsFixture::File("mem://bucket/table/a.bin", credentials)); },
	                  "ended with the transaction");
	f.con.Rollback();
	// they are created only in a transaction
	RequireThrowsWith([&]() { f.Credentials("table-key"); }, "an active transaction");
}

TEST_CASE("Credentials vend on first use, once, also when several threads use them first", "[file_credentials]") {
	CredentialsFixture f;
	f.con.BeginTransaction();
	ClientContextFileOpener opener(f.Context());
	auto credentials = FileCredentials::Create(f.Context(), "mem://bucket/table/", [&](ClientContext &) {
		f.vends++;
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		return TestSecret("table-key");
	});
	REQUIRE(f.vends == 0);
	vector<std::thread> threads;
	std::atomic<idx_t> served {0};
	for (idx_t i = 0; i < 8; i++) {
		threads.emplace_back([&]() {
			if (credentials->GetSecret(&opener, "mem://bucket/table/a.bin")->TryGetValue("key_id").ToString() ==
			    "table-key") {
				served++;
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	REQUIRE(served == 8);
	REQUIRE(f.vends == 1);
	f.con.Rollback();
}

TEST_CASE("A failed vend throws and the next use vends again; a refresh replaces the secret, a failed one keeps it",
          "[file_credentials]") {
	CredentialsFixture f;
	f.con.BeginTransaction();
	ClientContextFileOpener opener(f.Context());
	bool fail = true;
	auto credentials = FileCredentials::Create(f.Context(), "mem://bucket/table/", [&](ClientContext &) {
		auto vend = ++f.vends;
		if (fail) {
			throw IOException("the vend failed");
		}
		return TestSecret("table-key#" + to_string(vend));
	});
	auto key = [&]() {
		return credentials->GetSecret(&opener, "mem://bucket/table/a.bin")->TryGetValue("key_id").ToString();
	};
	REQUIRE_THROWS_AS(key(), IOException);
	fail = false;
	REQUIRE(key() == "table-key#2");
	REQUIRE(credentials->Refresh(&opener));
	REQUIRE(key() == "table-key#3");
	fail = true;
	REQUIRE_THROWS_AS(credentials->Refresh(&opener), IOException);
	REQUIRE(key() == "table-key#3");
	f.con.Rollback();
}
