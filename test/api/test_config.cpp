#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/http/http_util.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/main/user_settings.hpp"
#include "duckdb/common/types/interval.hpp"

#include <set>
#include <map>
#include <limits>
#include <cmath>

using namespace duckdb;

TEST_CASE("User settings report changes without treating unset values as NULL", "[api][settings]") {
	UserSettingsMap settings;
	Value value;
	REQUIRE_FALSE(settings.TryGetSetting(2, value));
	REQUIRE(settings.SetUserSetting(2, Value(LogicalType::INTEGER)));
	REQUIRE(settings.IsSet(2));
	REQUIRE(settings.TryGetSetting(2, value));
	REQUIRE(value.type() == LogicalType::INTEGER);
	REQUIRE(value.IsNull());
	REQUIRE_FALSE(settings.SetUserSetting(2, Value(LogicalType::INTEGER)));
	REQUIRE(settings.SetUserSetting(2, Value(LogicalType::BIGINT)));
	REQUIRE(settings.SetUserSetting(2, Value::BIGINT(1)));
	REQUIRE_FALSE(settings.SetUserSetting(2, Value::BIGINT(1)));
	REQUIRE(settings.SetUserSetting(2, Value::BIGINT(2)));
	settings.ClearSetting(2);
	REQUIRE_FALSE(settings.IsSet(2));
	REQUIRE(settings.SetUserSetting(2, Value::BIGINT(2)));
}

TEST_CASE("User settings compare NULL before reading scalar or nested values", "[api][settings]") {
	const vector<Value> values {Value::FLOAT(1.0F),
	                            Value::DOUBLE(1.0),
	                            Value::INTERVAL(1, 0, 0),
	                            Value::STRUCT({{"i", Value::INTEGER(1)}}),
	                            Value::LIST(LogicalType::INTEGER, {Value::INTEGER(1)}),
	                            Value::ARRAY(LogicalType::INTEGER, {Value::INTEGER(1)})};
	for (const auto &value : values) {
		UserSettingsMap settings;
		const Value null_value(value.type());
		REQUIRE(settings.SetUserSetting(0, null_value));
		REQUIRE_FALSE(settings.SetUserSetting(0, null_value));
		REQUIRE(settings.SetUserSetting(0, value));
		REQUIRE_FALSE(settings.SetUserSetting(0, value));
		REQUIRE(settings.SetUserSetting(0, null_value));
		REQUIRE_FALSE(settings.SetUserSetting(0, null_value));
	}
}

TEST_CASE("User settings preserve interval fields and signed zero", "[api][settings]") {
	UserSettingsMap settings;
	SECTION("Interval fields") {
		REQUIRE(settings.SetUserSetting(0, Value::INTERVAL(1, 0, 0)));
		REQUIRE_FALSE(settings.SetUserSetting(0, Value::INTERVAL(1, 0, 0)));
		REQUIRE(settings.SetUserSetting(0, Value::INTERVAL(0, 30, 0)));
		Value value;
		REQUIRE(settings.TryGetSetting(0, value));
		REQUIRE(value.GetValueUnsafe<interval_t>().SameFields({0, 30, 0}));
		REQUIRE_FALSE(settings.SetUserSetting(0, Value::INTERVAL(0, 30, 0)));
		REQUIRE(settings.SetUserSetting(0, Value::INTERVAL(0, 29, Interval::MICROS_PER_DAY)));
		REQUIRE(settings.TryGetSetting(0, value));
		REQUIRE(value.GetValueUnsafe<interval_t>().SameFields({0, 29, Interval::MICROS_PER_DAY}));
	}
	SECTION("Strict floating-point equality") {
		REQUIRE(settings.SetUserSetting(0, Value::FLOAT(1.0F)));
		REQUIRE(settings.SetUserSetting(0, Value::FLOAT(std::nextafter(1.0F, 2.0F))));
		REQUIRE_FALSE(settings.SetUserSetting(0, Value::FLOAT(std::nextafter(1.0F, 2.0F))));
		REQUIRE(settings.SetUserSetting(0, Value::DOUBLE(1.0)));
		REQUIRE(settings.SetUserSetting(0, Value::DOUBLE(std::nextafter(1.0, 2.0))));
		REQUIRE_FALSE(settings.SetUserSetting(0, Value::DOUBLE(std::nextafter(1.0, 2.0))));
	}
	SECTION("Float signed zero") {
		REQUIRE(settings.SetUserSetting(0, Value::FLOAT(0.0F)));
		REQUIRE_FALSE(settings.SetUserSetting(0, Value::FLOAT(0.0F)));
		REQUIRE(settings.SetUserSetting(0, Value::FLOAT(-0.0F)));
		Value value;
		REQUIRE(settings.TryGetSetting(0, value));
		REQUIRE(std::signbit(value.GetValueUnsafe<float>()));
		REQUIRE_FALSE(settings.SetUserSetting(0, Value::FLOAT(-0.0F)));
		REQUIRE(settings.SetUserSetting(0, Value::FLOAT(0.0F)));
		REQUIRE(settings.TryGetSetting(0, value));
		REQUIRE_FALSE(std::signbit(value.GetValueUnsafe<float>()));
	}
	SECTION("Double signed zero") {
		REQUIRE(settings.SetUserSetting(0, Value::DOUBLE(0.0)));
		REQUIRE(settings.SetUserSetting(0, Value::DOUBLE(-0.0)));
		Value value;
		REQUIRE(settings.TryGetSetting(0, value));
		REQUIRE(std::signbit(value.GetValueUnsafe<double>()));
		REQUIRE_FALSE(settings.SetUserSetting(0, Value::DOUBLE(-0.0)));
		REQUIRE(settings.SetUserSetting(0, Value::DOUBLE(0.0)));
		REQUIRE(settings.TryGetSetting(0, value));
		REQUIRE_FALSE(std::signbit(value.GetValueUnsafe<double>()));
	}
}

TEST_CASE("User settings compare nested values without SQL normalization", "[api][settings]") {
	UserSettingsMap settings;
	Value before, after;
	SECTION("List with NULL") {
		before = Value::LIST(LogicalType::INTEGER, {Value::INTEGER(1), Value(LogicalType::INTEGER)});
		after = Value::LIST(LogicalType::INTEGER, {Value::INTEGER(2), Value(LogicalType::INTEGER)});
	}
	SECTION("Empty list") {
		before = Value::LIST(LogicalType::INTEGER, {});
		after = Value::LIST(LogicalType::INTEGER, {Value::INTEGER(1)});
	}
	SECTION("Struct with nested interval") {
		before = Value::STRUCT({{"values", Value::LIST(LogicalType::INTERVAL, {Value::INTERVAL(1, 0, 0)})}});
		after = Value::STRUCT({{"values", Value::LIST(LogicalType::INTERVAL, {Value::INTERVAL(0, 30, 0)})}});
	}
	SECTION("Array with signed zero") {
		before = Value::ARRAY(LogicalType::DOUBLE, {Value::DOUBLE(0.0), Value(LogicalType::DOUBLE)});
		after = Value::ARRAY(LogicalType::DOUBLE, {Value::DOUBLE(-0.0), Value(LogicalType::DOUBLE)});
	}
	SECTION("Map with signed zero") {
		before = Value::MAP(LogicalType::INTEGER, LogicalType::FLOAT, {Value::INTEGER(1)}, {Value::FLOAT(0.0F)});
		after = Value::MAP(LogicalType::INTEGER, LogicalType::FLOAT, {Value::INTEGER(1)}, {Value::FLOAT(-0.0F)});
	}
	SECTION("Union with interval") {
		child_list_t<LogicalType> members {{"i", LogicalType::INTERVAL}, {"d", LogicalType::DOUBLE}};
		before = Value::UNION(members, 0, Value::INTERVAL(1, 0, 0));
		after = Value::UNION(members, 0, Value::INTERVAL(0, 30, 0));
	}
	SECTION("Tuple with signed zero") {
		before = Value::TUPLE({Value::DOUBLE(0.0)});
		after = Value::TUPLE({Value::DOUBLE(-0.0)});
	}
	SECTION("Nested NULL type changes") {
		before = Value(LogicalType::LIST(LogicalType::INTEGER));
		after = Value(LogicalType::LIST(LogicalType::BIGINT));
	}
	SECTION("Nested NULL collation changes") {
		before = Value(LogicalType::LIST(LogicalType::VARCHAR_COLLATION("nocase")));
		after = Value(LogicalType::LIST(LogicalType::VARCHAR));
	}
	REQUIRE(settings.SetUserSetting(0, before));
	REQUIRE_FALSE(settings.SetUserSetting(0, before));
	REQUIRE(settings.SetUserSetting(0, after));
	REQUIRE_FALSE(settings.SetUserSetting(0, after));
	Value stored;
	REQUIRE(settings.TryGetSetting(0, stored));
	REQUIRE(stored.type().EqualsIncludingCollation(after.type()));
	if (!after.IsNull()) {
		// Verify the stored representation, which SQL equality can normalize.
		REQUIRE(stored.ToString() == after.ToString());
	}
}

TEST_CASE("User settings compare additional scalar representations", "[api][settings]") {
	UserSettingsMap settings;
	const vector<Value> values {Value::BIT("0101"), Value("12345678901234567890").DefaultCastAs(LogicalType::BIGNUM),
	                            Value::TYPE(LogicalType::INTEGER),
	                            Value::INTEGER(1).DefaultCastAs(LogicalType::VARIANT())};
	for (const auto &value : values) {
		REQUIRE(settings.SetUserSetting(0, value));
		REQUIRE_FALSE(settings.SetUserSetting(0, value));
	}
	const auto nan = Value::DOUBLE(std::numeric_limits<double>::quiet_NaN());
	REQUIRE(settings.SetUserSetting(0, nan));
	REQUIRE(settings.SetUserSetting(0, nan));
	const auto nested_nan = Value::LIST(LogicalType::DOUBLE, {nan});
	REQUIRE(settings.SetUserSetting(0, nested_nan));
	REQUIRE(settings.SetUserSetting(0, nested_nan));
}

TEST_CASE("User settings retain type metadata changes", "[api][settings]") {
	UserSettingsMap settings;
	REQUIRE(settings.SetUserSetting(0, Value("value").WithType(LogicalType::JSON())));
	REQUIRE_FALSE(settings.SetUserSetting(0, Value("value").WithType(LogicalType::JSON())));
	REQUIRE(settings.SetUserSetting(0, Value("value")));
	REQUIRE(settings.SetUserSetting(0, Value("value").WithType(LogicalType::VARCHAR_COLLATION("nocase"))));
	REQUIRE_FALSE(settings.SetUserSetting(0, Value("value").WithType(LogicalType::VARCHAR_COLLATION("nocase"))));
	REQUIRE(settings.SetUserSetting(0, Value("VALUE").WithType(LogicalType::VARCHAR_COLLATION("nocase"))));
	REQUIRE(settings.SetUserSetting(0, Value("VALUE")));

	auto extension_info = make_uniq<ExtensionTypeInfo>();
	extension_info->properties.emplace("property", Value::INTEGER(1));
	auto extended_type = LogicalType(LogicalType::INTEGER).WithExtensionInfo(std::move(extension_info));
	REQUIRE(settings.SetUserSetting(1, Value::INTEGER(1).WithType(extended_type)));
	REQUIRE(settings.SetUserSetting(1, Value::INTEGER(1).WithType(extended_type)));
	REQUIRE(settings.SetUserSetting(1, Value::INTEGER(1)));
	REQUIRE(settings.SetUserSetting(1, Value(LogicalType::LIST(extended_type))));
	REQUIRE(settings.SetUserSetting(1, Value(LogicalType::LIST(extended_type))));
	REQUIRE(settings.SetUserSetting(1, Value(LogicalType::LIST(LogicalType::INTEGER))));
}

#ifndef __MINGW32__
TEST_CASE("Repeated global settings keep the read cache valid", "[api][settings]") {
	GlobalUserSettings settings;
	auto expect_same_value = [&](Value value) {
		settings.SetUserSetting(0, value);
		const auto version = settings.GetSettings().version;
		settings.SetUserSetting(0, std::move(value));
		REQUIRE(settings.GetSettings().version == version);
	};
	auto expect_repeated_update = [&](Value value) {
		settings.SetUserSetting(0, value);
		const auto version = settings.GetSettings().version;
		settings.SetUserSetting(0, std::move(value));
		REQUIRE(settings.GetSettings().version == version + 1);
	};

	expect_same_value(Value::BOOLEAN(false));
	expect_same_value(Value::TINYINT(1));
	expect_same_value(Value::SMALLINT(1));
	expect_same_value(Value::INTEGER(1));
	expect_same_value(Value::BIGINT(1));
	expect_same_value(Value::HUGEINT(hugeint_t(1)));
	expect_same_value(Value::UTINYINT(1));
	expect_same_value(Value::USMALLINT(1));
	expect_same_value(Value::UINTEGER(1));
	expect_same_value(Value::UBIGINT(1));
	expect_same_value(Value::UHUGEINT(uhugeint_t(1)));
	expect_same_value(Value::FLOAT(1.5F));
	expect_same_value(Value::DOUBLE(1.5));
	expect_same_value(Value::DECIMAL(125, 9, 2));
	expect_same_value(Value("value"));
	expect_same_value(Value::BLOB(string("blob")));
	expect_same_value(Value::DATE(date_t(1)));
	expect_same_value(Value::TIME(dtime_t(1)));
	expect_same_value(Value::TIME_NS(dtime_ns_t(1)));
	expect_same_value(Value::TIMETZ(dtime_tz_t(dtime_t(1), 0)));
	expect_same_value(Value::TIMESTAMP(timestamp_t(1)));
	expect_same_value(Value::TIMESTAMPSEC(timestamp_sec_t(1)));
	expect_same_value(Value::TIMESTAMPMS(timestamp_ms_t(1)));
	expect_same_value(Value::TIMESTAMPNS(timestamp_ns_t(1)));
	expect_same_value(Value::TIMESTAMPTZ(timestamp_tz_t(1)));
	expect_same_value(Value::TIMESTAMPTZNS(timestamp_tz_ns_t(1)));
	expect_same_value(Value::UUID(hugeint_t(1)));
	expect_same_value(Value::INTERVAL(1, 1, 1));
	expect_same_value(Value(LogicalType::BOOLEAN));
	expect_same_value(Value(LogicalType::TINYINT));
	expect_same_value(Value(LogicalType::SMALLINT));
	expect_same_value(Value(LogicalType::INTEGER));
	expect_same_value(Value(LogicalType::BIGINT));
	expect_same_value(Value(LogicalType::HUGEINT));
	expect_same_value(Value(LogicalType::UTINYINT));
	expect_same_value(Value(LogicalType::USMALLINT));
	expect_same_value(Value(LogicalType::UINTEGER));
	expect_same_value(Value(LogicalType::UBIGINT));
	expect_same_value(Value(LogicalType::UHUGEINT));
	expect_same_value(Value(LogicalType::FLOAT));
	expect_same_value(Value(LogicalType::DOUBLE));
	expect_same_value(Value(LogicalType::DECIMAL(9, 2)));
	expect_same_value(Value(LogicalType::VARCHAR));
	expect_same_value(Value(LogicalType::BLOB));
	expect_same_value(Value(LogicalType::DATE));
	expect_same_value(Value(LogicalType::TIME));
	expect_same_value(Value(LogicalType::TIME_NS));
	expect_same_value(Value(LogicalType::TIME_TZ));
	expect_same_value(Value(LogicalType::TIMESTAMP));
	expect_same_value(Value(LogicalType::TIMESTAMP_S));
	expect_same_value(Value(LogicalType::TIMESTAMP_MS));
	expect_same_value(Value(LogicalType::TIMESTAMP_NS));
	expect_same_value(Value(LogicalType::TIMESTAMP_TZ));
	expect_same_value(Value(LogicalType::TIMESTAMP_TZ_NS));
	expect_same_value(Value(LogicalType::UUID));
	expect_same_value(Value(LogicalType::INTERVAL));

	expect_repeated_update(Value::FLOAT(std::numeric_limits<float>::quiet_NaN()));
	expect_repeated_update(Value::DOUBLE(std::numeric_limits<double>::quiet_NaN()));

	settings.SetUserSetting(0, Value(LogicalType::VARCHAR));
	Value value;
	REQUIRE(static_cast<bool>(settings.TryGetSetting(0, value)));
	REQUIRE(value.type() == LogicalType::VARCHAR);
	REQUIRE(value.IsNull());

	settings.SetUserSetting(2, Value::INTEGER(1));
	const auto before_type_change = settings.GetSettings().version;
	settings.SetUserSetting(2, Value::BIGINT(1));
	REQUIRE(settings.GetSettings().version == before_type_change + 1);
	settings.SetUserSetting(3, Value::DECIMAL(125, 9, 2));
	const auto before_decimal_type_change = settings.GetSettings().version;
	settings.SetUserSetting(3, Value::DECIMAL(125, 10, 2));
	REQUIRE(settings.GetSettings().version == before_decimal_type_change + 1);
	const auto before_unset = settings.GetSettings().version;
	settings.SetUserSetting(1, Value::INTEGER(1));
	REQUIRE(settings.GetSettings().version == before_unset + 1);
	settings.SetUserSetting(1, Value::INTEGER(1));
	REQUIRE(settings.GetSettings().version == before_unset + 1);

	expect_same_value(Value(LogicalType(LogicalTypeId::CHAR)));
	expect_same_value(Value("{\"key\": 1}").WithType(LogicalType::JSON()));
	expect_same_value(Value("value").WithType(LogicalType::VARCHAR_COLLATION("nocase")));
	auto extension_info = make_uniq<ExtensionTypeInfo>();
	extension_info->modifiers.emplace_back(Value::INTEGER(1));
	auto extended_integer = LogicalType(LogicalType::INTEGER).WithExtensionInfo(std::move(extension_info));
	expect_repeated_update(Value::INTEGER(1).WithType(extended_integer));
	settings.SetUserSetting(4, Value::INTEGER(1).WithType(extended_integer));
	const auto before_plain_integer = settings.GetSettings().version;
	settings.SetUserSetting(4, Value::INTEGER(1));
	REQUIRE(settings.GetSettings().version == before_plain_integer + 1);
	expect_same_value(Value(LogicalType::LIST(LogicalType::INTEGER)));
	expect_same_value(Value(LogicalType::STRUCT({{"a", LogicalType::INTEGER}})));
	expect_same_value(Value(LogicalType::MAP(LogicalType::INTEGER, LogicalType::VARCHAR)));
	expect_same_value(Value(LogicalType::UNION({{"a", LogicalType::INTEGER}})));
	expect_same_value(Value(LogicalType::ARRAY(LogicalType::INTEGER, 1)));
	expect_same_value(Value(LogicalType::VARIANT()));

	const auto before_clear = settings.GetSettings().version;
	settings.ClearSetting(0);
	REQUIRE(settings.GetSettings().version == before_clear + 1);
	REQUIRE_FALSE(static_cast<bool>(settings.TryGetSetting(0, value)));
	settings.ClearSetting(0);
	REQUIRE(settings.GetSettings().version == before_clear + 2);
}

TEST_CASE("Global settings invalidate the cache when representations change", "[api][settings]") {
	GlobalUserSettings settings;
	const vector<pair<Value, Value>> changes {
	    {Value::INTERVAL(1, 0, 0), Value::INTERVAL(0, 30, 0)},
	    {Value::FLOAT(0.0F), Value::FLOAT(-0.0F)},
	    {Value::DOUBLE(0.0), Value::DOUBLE(-0.0)},
	    {Value::LIST(LogicalType::INTEGER, {Value::INTEGER(1)}),
	     Value::LIST(LogicalType::INTEGER, {Value::INTEGER(2)})},
	    {Value::STRUCT({{"i", Value::INTERVAL(1, 0, 0)}}), Value::STRUCT({{"i", Value::INTERVAL(0, 30, 0)}})}};
	for (const auto &change : changes) {
		settings.SetUserSetting(0, change.first);
		const auto version = settings.GetSettings().version;
		settings.SetUserSetting(0, change.second);
		REQUIRE(settings.GetSettings().version == version + 1);
		Value stored;
		auto lookup = settings.TryGetSetting(0, stored);
		REQUIRE(lookup.GetScope() == SettingScope::GLOBAL);
		REQUIRE(stored.ToString() == change.second.ToString());
		settings.SetUserSetting(0, change.second);
		REQUIRE(settings.GetSettings().version == version + 1);
	}
}

namespace {
struct CountingSettingInfo : public ExtensionOptionInfo {
	mutable idx_t calls = 0;
	void OnSet(ClientContext &, SetScope, Value &, bool) const override {
		calls++;
	}
};
} // namespace

TEST_CASE("Repeated settings still invoke setting callbacks", "[api][settings]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &config = DBConfig::GetConfig(*db.instance);
	auto info = make_shared_ptr<CountingSettingInfo>();
	ExtensionOption option("An option", LogicalType::INTEGER, nullptr, Value::INTEGER(0), SetScope::GLOBAL, false,
	                       false);
	option.info = info;
	config.AddExtensionOption("counting_setting", std::move(option));
	REQUIRE_NO_FAIL(con.Query("SET GLOBAL counting_setting=1"));
	const auto version = config.user_settings.GetSettings().version;
	REQUIRE_NO_FAIL(con.Query("SET GLOBAL counting_setting=1"));
	REQUIRE(info->calls == 2);
	REQUIRE(config.user_settings.GetSettings().version == version);
}

#endif

class FileCleaner {
public:
	FileCleaner(FileSystem *fs, string file) : fs(*fs), file(std::move(file)) {
	}

	~FileCleaner() {
		if (fs.FileExists(file)) {
			fs.RemoveFile(file);
		}
	}

private:
	FileSystem &fs;
	string file;
};

TEST_CASE("Test DB config configuration", "[api]") {
	DBConfig config;

	auto options = config.GetOptions();

	map<string, duckdb::vector<string>> test_options;
	test_options["access_mode"] = {"automatic", "read_only", "read_write"};
	test_options["default_order"] = {"asc", "desc"};
	test_options["default_null_order"] = {"nulls_first", "nulls_last"};
	test_options["enable_external_access"] = {"true", "false"};
	test_options["max_memory"] = {"-1", "16GB"};
	test_options["threads"] = {"1", "4"};

	REQUIRE(config.GetOptionByName("unknownoption") == nullptr);

	for (auto &option : options) {
		auto op = config.GetOptionByName(option.name);
		REQUIRE(op);

		auto entry = test_options.find(option.name);
		if (entry != test_options.end()) {
			for (auto &str_val : entry->second) {
				Value val(str_val);
				REQUIRE_NOTHROW(config.SetOption(option, val));
			}
			Value invalid_val("___this_is_probably_invalid");
			REQUIRE_THROWS(config.SetOption(option, invalid_val));
		}
	}
}

TEST_CASE("Test allowed options", "[api]") {
	identifier_map_t<Value> config_dict;
	string option;

	SECTION("allowed_directories") {
		config_dict.emplace("allowed_directories", Value::LIST({Value("test")}));
		option = "allowed_directories";
	}
	SECTION("allowed_paths") {
		config_dict.emplace("allowed_paths", Value::LIST({Value("test")}));
		option = "allowed_paths";
	}

	SECTION("enable_logging") {
		config_dict.emplace("enable_logging", Value::BOOLEAN(false));
		option = "enable_logging";
	}

	SECTION("disabled_filesystems") {
		config_dict.emplace("disabled_filesystems", Value::BOOLEAN(false));
		option = "disabled_filesystems";
	}

	SECTION("logging_mode") {
		config_dict.emplace("logging_mode", Value::BOOLEAN(false));
		option = "logging_mode";
	}

	SECTION("logging_sink") {
		config_dict.emplace("logging_sink", Value::BOOLEAN(false));
		option = "logging_sink";
	}

	SECTION("logging_storage") {
		config_dict.emplace("logging_storage", Value::BOOLEAN(false));
		option = "logging_storage";
	}

	SECTION("logging_level") {
		config_dict.emplace("logging_level", Value::BOOLEAN(false));
		option = "logging_level";
	}

	SECTION("enabled_log_types") {
		config_dict.emplace("enabled_log_types", Value::BOOLEAN(false));
		option = "enabled_log_types";
	}

	SECTION("disabled_log_types") {
		config_dict.emplace("disabled_log_types", Value::BOOLEAN(false));
		option = "disabled_log_types";
	}

	try {
		DBConfig config(config_dict, false);
	} catch (std::exception &ex) {
		ErrorData error_data(ex);
		REQUIRE(error_data.Type() == ExceptionType::INVALID_INPUT);
		REQUIRE(error_data.RawMessage() ==
		        StringUtil::Format("Cannot change/set %s before the database is started", option));
	}
}

TEST_CASE("Test user_agent", "[api]") {
	{
		// Default duckdb_api is cpp
		DuckDB db(nullptr);
		Connection con(db);
		auto res = con.Query("PRAGMA user_agent");
		REQUIRE_THAT(res->Collection().GetValue(0, 0).ToString(), Catch::Matchers::Matches("duckdb/.*(.*) cpp"));
	}
	{
		// The latest provided duckdb_api is used
		DBConfig config;
		config.SetOptionByName("duckdb_api", "capi");
		config.SetOptionByName("duckdb_api", "go");
		DuckDB db("", &config);
		Connection con(db);
		auto res = con.Query("PRAGMA user_agent");
		REQUIRE_THAT(res->Collection().GetValue(0, 0).ToString(), Catch::Matchers::Matches("duckdb/.*(.*) go"));
	}
	{
		DuckDB db(nullptr);
		HTTPHeaders headers(*db.instance);
		auto user_agent = headers.GetHeaderValue("User-Agent");
		REQUIRE(!user_agent.empty());
		REQUIRE(user_agent.back() != ' ');
		REQUIRE(user_agent.back() != '\t');
	}
}

TEST_CASE("HTTPHeaders preserves repeated field values", "[api]") {
	HTTPHeaders headers;
	headers.InsertIfAbsent("Cache-Control", "max-age=600");
	headers.Append("cache-control", "no-store");

	REQUIRE(headers.GetHeaderValue("CACHE-CONTROL") == "max-age=600");
	headers.InsertIfAbsent("CACHE-CONTROL", "ignored");
	REQUIRE(headers.GetHeaderValues("Cache-Control") == vector<string> {"max-age=600", "no-store"});

	headers.Set("CACHE-CONTROL", "private");
	REQUIRE(headers.GetHeaderValue("cache-control") == "private");
	REQUIRE(headers.GetHeaderValues("Cache-Control") == vector<string> {"private"});

	headers.Set("X-Empty", "");
	headers.Append("Cache-Control", "no-cache");
	case_insensitive_map_t<vector<string>> fields;
	for (const auto &field : headers) {
		fields[field.first].push_back(field.second);
	}
	REQUIRE(fields.size() == 2);
	REQUIRE(fields.at("cache-control") == vector<string> {"private", "no-cache"});
	REQUIRE(fields.at("x-empty") == vector<string> {""});

	headers.RemoveAll("cAcHe-CoNtRoL");
	REQUIRE_FALSE(headers.HasHeader("Cache-Control"));
	headers.RemoveAll("Cache-Control");
	REQUIRE(headers.GetHeaderValue("X-Empty").empty());
}

TEST_CASE("Test secret_directory configuration", "[api]") {
	DBConfig config;

	auto options = config.GetOptions();

	config.SetOptionByName("secret_directory", Value("my_secret_dir"));
	config.SetOptionByName("extension_directory", Value("my_extension_dir"));

	DuckDB db(nullptr, &config);
	Connection con(db);

	// Ensure that the extension directory is set correctly (according to the inital config)
	auto select_extension_dir = con.Query("SELECT current_setting('extension_directory') AS extdir;");
	REQUIRE(select_extension_dir->Collection().GetValue(0, 0).ToString() == "my_extension_dir");

	auto select_secret_dir = con.Query("SELECT current_setting('secret_directory') AS secretdir;");
	REQUIRE(select_secret_dir->Collection().GetValue(0, 0).ToString() == "my_secret_dir");
}

TEST_CASE("Test secret creation with a custom secret_directory configuration", "[api]") {
	DuckDB fs_db(nullptr);
	auto &fs = FileSystem::GetLocal(*fs_db.instance);
	string directory_path = TestDirectoryPath();
	string my_secret_dir = fs.JoinPath(directory_path, "my_secret_dir");
	string my_secret_file = fs.JoinPath(my_secret_dir, "my_secret.duckdb_secret");
	FileCleaner cleaner(&fs, my_secret_file);

	DBConfig config;

	auto options = config.GetOptions();

	config.SetOptionByName("secret_directory", Value(my_secret_dir));

	DuckDB db(nullptr, &config);
	Connection con(db);

	// Ensure that the extension directory is set correctly (according to the inital config)
	auto select_secret_dir = con.Query("SELECT current_setting('secret_directory') AS secretdir;");
	REQUIRE(select_secret_dir->Collection().GetValue(0, 0).ToString() == my_secret_dir);

	// Ensure that creating a secret works and the secret file is created in the correct directory
	auto create_secret = con.Query("CREATE PERSISTENT SECRET my_secret (TYPE http, BEARER_TOKEN 'token')");
	REQUIRE(create_secret->Collection().GetValue(0, 0).GetValue<bool>());
	REQUIRE(fs.FileExists(my_secret_file));
}

TEST_CASE("Test secret creation with a custom secret_directory configuration update", "[api]") {
	DuckDB fs_db(nullptr);
	auto &fs = FileSystem::GetLocal(*fs_db.instance);
	string directory_path = TestDirectoryPath();
	string my_secret_dir = fs.JoinPath(directory_path, "my_secret_dir");
	string new_secret_dir = fs.JoinPath(directory_path, "new_secret_dir");
	string my_other_secret_file = fs.JoinPath(new_secret_dir, "my_other_secret.duckdb_secret");
	FileCleaner cleaner(&fs, my_other_secret_file);

	DBConfig config;

	auto options = config.GetOptions();

	config.SetOptionByName("secret_directory", Value(my_secret_dir));

	DuckDB db(nullptr, &config);
	Connection con(db);

	// Ensure that the extension directory is set correctly (according to the inital config)
	auto select_secret_dir = con.Query("SELECT current_setting('secret_directory') AS secretdir;");
	REQUIRE(select_secret_dir->Collection().GetValue(0, 0).ToString() == my_secret_dir);

	// Do not create a secret here because it will initialize the secret manager and forbid us to update the value.

	// Update the secret directory and ensure that the setting is updated
	con.Query("SET secret_directory='" + new_secret_dir + "';");
	auto select_new_secret_dir = con.Query("SELECT current_setting('secret_directory') AS secretdir;");
	REQUIRE(select_new_secret_dir->Collection().GetValue(0, 0).ToString() == new_secret_dir);

	// Create another secret and ensure that it is created in the new directory
	auto new_create_secret = con.Query("CREATE PERSISTENT SECRET my_other_secret (TYPE http, BEARER_TOKEN 'token')");
	REQUIRE(new_create_secret->Collection().GetValue(0, 0).GetValue<bool>());
	REQUIRE(fs.FileExists(my_other_secret_file));
}

TEST_CASE("Test secret_directory configuration update after secret creation", "[api]") {
	DuckDB fs_db(nullptr);
	auto &fs = FileSystem::GetLocal(*fs_db.instance);
	string directory_path = TestDirectoryPath();
	string my_secret_dir = fs.JoinPath(directory_path, "my_secret_dir");
	string my_secret_file = fs.JoinPath(my_secret_dir, "my_secret.duckdb_secret");
	FileCleaner cleaner(&fs, my_secret_file);

	DBConfig config;

	auto options = config.GetOptions();

	config.SetOptionByName("secret_directory", Value(my_secret_dir));

	DuckDB db(nullptr, &config);
	Connection con(db);

	// Ensure that the extension directory is set correctly (according to the inital config)
	auto select_secret_dir = con.Query("SELECT current_setting('secret_directory') AS secretdir;");
	REQUIRE(select_secret_dir->Collection().GetValue(0, 0).ToString() == my_secret_dir);

	// Create a secret here to initialize the secret manager
	auto create_secret = con.Query("CREATE PERSISTENT SECRET my_secret (TYPE http, BEARER_TOKEN 'token')");
	REQUIRE(create_secret->Collection().GetValue(0, 0).GetValue<bool>());
	REQUIRE(fs.FileExists(my_secret_file));

	// Try to update the secret directory and expect failure as the secret manager is already initialized
	auto update_secret_directory = con.Query("SET secret_directory='new_secret_dir';");
	REQUIRE_FAIL(update_secret_directory);
}

TEST_CASE("Test warnings_as_errors in the startup config", "[api]") {
	DBConfig config;
	REQUIRE_NOTHROW(config.SetOptionByName("warnings_as_errors", Value::BOOLEAN(false)));
	REQUIRE_THROWS_WITH(config.SetOptionByName("warnings_as_errors", Value::BOOLEAN(true)),
	                    Catch::Contains("startup config"));
	// string values, as the C API passes them
	REQUIRE_NOTHROW(config.SetOptionByName("warnings_as_errors", Value("false")));
	REQUIRE_THROWS_WITH(config.SetOptionByName("warnings_as_errors", Value("true")), Catch::Contains("startup config"));
	REQUIRE_THROWS_WITH(config.SetOptionByName("warnings_as_errors", Value()), Catch::Contains("cannot be NULL"));
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto result = con.Query("SELECT current_setting('warnings_as_errors')");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
}

TEST_CASE("Test deprecated extension options warn when set", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &config = DBConfig::GetConfig(*db.instance);
	config.AddExtensionOption("my_deprecated_option", "A deprecated option", LogicalType::BOOLEAN, Value(false),
	                          nullptr, SetScope::SESSION, false, true);
	config.AddExtensionOption("my_option", "An option", LogicalType::BOOLEAN, Value(false));

	REQUIRE_NO_FAIL(con.Query("SET enable_logging=true"));
	REQUIRE_NO_FAIL(con.Query("SET warnings_as_errors=true"));

	auto result = con.Query("SET my_deprecated_option=true");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(),
	                             "The 'my_deprecated_option' setting is deprecated and will be removed"));
	REQUIRE_NO_FAIL(con.Query("RESET my_deprecated_option"));
	REQUIRE_NO_FAIL(con.Query("SET my_option=true"));

	// setting the option directly, as ducklake does, warns as well
	ExtensionOption option;
	REQUIRE(config.TryGetExtensionOption(Identifier("my_deprecated_option"), option));
	REQUIRE_THROWS_WITH(PhysicalSet::SetExtensionVariable(*con.context, option, SetScope::SESSION, Value(true)),
	                    Catch::Contains("is deprecated"));
	option = ExtensionOption();
	REQUIRE(config.TryGetExtensionOption(Identifier("my_option"), option));
	REQUIRE_NOTHROW(PhysicalSet::SetExtensionVariable(*con.context, option, SetScope::SESSION, Value(true)));

	result = con.Query("SELECT count(*) FROM duckdb_settings() WHERE name = 'my_deprecated_option'");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT count(*) FROM duckdb_settings(deprecated := true) WHERE name = 'my_deprecated_option'");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test extension option defaults are not user settings", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &config = DBConfig::GetConfig(*db.instance);
	config.AddExtensionOption("my_option", "An option", LogicalType::VARCHAR, Value("default"));
	ExtensionOption option;
	REQUIRE(config.TryGetExtensionOption(Identifier("my_option"), option));
	auto setting_index = option.setting_index.GetIndex();
	Value value;

	REQUIRE(!config.TryGetCurrentUserSetting(setting_index, value));
	auto result = con.Query("SELECT current_setting('my_option')");
	REQUIRE(CHECK_COLUMN(result, 0, {"default"}));

	// setting the default explicitly is not the same as leaving it unset
	REQUIRE_NO_FAIL(con.Query("SET GLOBAL my_option = 'default'"));
	REQUIRE(bool(config.TryGetCurrentUserSetting(setting_index, value)));
	REQUIRE_NO_FAIL(con.Query("RESET GLOBAL my_option"));
	REQUIRE(!config.TryGetCurrentUserSetting(setting_index, value));
	result = con.Query("SELECT current_setting('my_option')");
	REQUIRE(CHECK_COLUMN(result, 0, {"default"}));

	// RESET clears the session value, so the connection sees the global value again
	REQUIRE_NO_FAIL(con.Query("SET GLOBAL my_option = 'global'"));
	REQUIRE_NO_FAIL(con.Query("SET my_option = 'session'"));
	REQUIRE_NO_FAIL(con.Query("RESET my_option"));
	result = con.Query("SELECT current_setting('my_option')");
	REQUIRE(CHECK_COLUMN(result, 0, {"global"}));

	// an option without a default is unset again after RESET, not NULL
	config.AddExtensionOption("my_option_without_default", "An option", LogicalType::BOOLEAN);
	REQUIRE_NO_FAIL(con.Query("SET my_option_without_default = true"));
	REQUIRE_NO_FAIL(con.Query("RESET my_option_without_default"));
	REQUIRE(!con.context->TryGetCurrentSetting(Identifier("my_option_without_default"), value));
}

TEST_CASE("Test every deprecated setting warns when set", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET enable_logging=true"));
	REQUIRE_NO_FAIL(con.Query("SET warnings_as_errors=true"));
	// profiling_mode only has a value while profiling is on
	REQUIRE_NO_FAIL(con.Query("SET enable_profiling='no_output'"));

	auto settings = con.Query("SELECT name, value FROM duckdb_settings(deprecated := true) ORDER BY name");
	REQUIRE_NO_FAIL(*settings);
	idx_t checked = 0;
	while (auto chunk = settings->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto name = chunk->GetValue(0, i).ToString();
			auto value = chunk->GetValue(1, i).ToSQLString();
			INFO(name);
			// setting a deprecated setting to its current value warns, which warnings_as_errors turns into an error
			auto result = con.Query(StringUtil::Format("SET %s = %s", name, value));
			REQUIRE(result->HasError());
			if (name == "vacuum_rebuild_indexes") {
				// only settable in the startup config, so it never reaches the warning
				REQUIRE(StringUtil::Contains(result->GetError(), "while database is running"));
			} else {
				REQUIRE(StringUtil::Contains(result->GetError(), "deprecated"));
			}
			checked++;
		}
	}
	REQUIRE(checked > 0);
}

TEST_CASE("Test the per-connection progress bar options in the startup config", "[api]") {
	DBConfig config;
	REQUIRE_THROWS_WITH(config.SetOptionByName("enable_progress_bar", Value("true")),
	                    Catch::Contains("enable_progress_bar") && Catch::Contains("as a global option"));
	REQUIRE_THROWS_WITH(config.SetOptionByName("enable_progress_bar_print", Value("false")),
	                    Catch::Contains("enable_progress_bar_print") && Catch::Contains("as a global option"));
	REQUIRE_THROWS_WITH(config.SetOptionByName("progress_bar_time", Value("100")),
	                    Catch::Contains("progress_bar_time") && Catch::Contains("as a global option"));
	// the options remain settable per connection
	DuckDB db(nullptr, &config);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET progress_bar_time = 100"));
	auto result = con.Query("SELECT current_setting('progress_bar_time'), current_setting('enable_progress_bar')");
	REQUIRE(CHECK_COLUMN(result, 0, {100}));
	REQUIRE(CHECK_COLUMN(result, 1, {true}));
}

TEST_CASE("Test profiling_renderer_settings is rejected in the startup config", "[api]") {
	DBConfig config;
	auto settings =
	    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, {Value("operator_casing")}, {Value("bogus")});
	REQUIRE_THROWS_WITH(config.SetOptionByName("profiling_renderer_settings", settings),
	                    Catch::Contains("profiling_renderer_settings") && Catch::Contains("as a global option"));
	DuckDB db(nullptr, &config);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("EXPLAIN SELECT 42"));
	REQUIRE_NO_FAIL(con.Query("SET profiling_renderer_settings = MAP {'operator_casing': 'title'}"));
	REQUIRE_FAIL(con.Query("SET profiling_renderer_settings = MAP {'operator_casing': 'bogus'}"));
}

TEST_CASE("Test buffer_manager_track_eviction_timestamps is fixed once the database is running", "[api]") {
	DBConfig config;
	REQUIRE_NOTHROW(config.SetOptionByName("buffer_manager_track_eviction_timestamps", Value::BOOLEAN(true)));
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto result = con.Query("SET buffer_manager_track_eviction_timestamps = false");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "Cannot change buffer_manager_track_eviction_timestamps setting "
	                                                 "while database is running"));
	result = con.Query("SELECT current_setting('buffer_manager_track_eviction_timestamps')");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
}

TEST_CASE("Test tracked_metrics in the startup config", "[api]") {
	DBConfig config;
	REQUIRE_NOTHROW(config.SetOptionByName("tracked_metrics", Value("['query.*']")));
	REQUIRE_THROWS_WITH(config.SetOptionByName("tracked_metrics", Value("['query.*', NULL]")),
	                    Catch::Contains("cannot contain NULL entries"));
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto expected = Value::LIST(LogicalType::VARCHAR, {Value("query.*")});
	auto result = con.Query("SELECT current_setting('tracked_metrics')");
	REQUIRE(CHECK_COLUMN(result, 0, {expected}));
	// a session value resets to the configured global value
	REQUIRE_NO_FAIL(con.Query("SET tracked_metrics = ['optimizer.*']"));
	REQUIRE_NO_FAIL(con.Query("RESET tracked_metrics"));
	result = con.Query("SELECT current_setting('tracked_metrics')");
	REQUIRE(CHECK_COLUMN(result, 0, {expected}));
}
