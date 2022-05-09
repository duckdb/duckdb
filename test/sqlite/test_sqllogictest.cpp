#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "test_helpers.hpp"

#include "sqllogic_test_runner.hpp"

#include <functional>
#include <string>
#include <vector>

using namespace duckdb;
using namespace std;

// code below traverses the test directory and makes individual test cases out
// of each script
static void listFiles(FileSystem &fs, const string &path, std::function<void(const string &)> cb) {
	fs.ListFiles(path, [&](string fname, bool is_dir) {
		string full_path = fs.JoinPath(path, fname);
		if (is_dir) {
			// recurse into directory
			listFiles(fs, full_path, cb);
		} else {
			cb(full_path);
		}
	});
}

static bool endsWith(const string &mainStr, const string &toMatch) {
	return (mainStr.size() >= toMatch.size() &&
	        mainStr.compare(mainStr.size() - toMatch.size(), toMatch.size(), toMatch) == 0);
}

static void testRunner() {
	// this is an ugly hack that uses the test case name to pass the script file
	// name if someone has a better idea...
	auto name = Catch::getResultCapture().getCurrentTestName();
	// fprintf(stderr, "%s\n", name.c_str());
	string initial_dbpath;
	if (TestForceStorage()) {
		auto storage_name = StringUtil::Replace(name, "/", "_");
		storage_name = StringUtil::Replace(storage_name, ".", "_");
		storage_name = StringUtil::Replace(storage_name, "\\", "_");
		initial_dbpath = TestCreatePath(storage_name + ".db");
	}
	SQLLogicTestRunner runner(move(initial_dbpath));
	runner.output_sql = Catch::getCurrentContext().getConfig()->outputSQL();
	runner.ExecuteFile(name);
}

static string ParseGroupFromPath(string file) {
	string extension = "";
	if (file.find(".test_slow") != std::string::npos) {
		// "slow" in the name indicates a slow test (i.e. only run as part of allunit)
		extension = "[.]";
	}
	if (file.find(".test_coverage") != std::string::npos) {
		// "slow" in the name indicates a slow test (i.e. only run as part of allunit)
		return "[coverage][.]";
	}
	// move backwards to the last slash
	int group_begin = -1, group_end = -1;
	for (idx_t i = file.size(); i > 0; i--) {
		if (file[i - 1] == '/' || file[i - 1] == '\\') {
			if (group_end == -1) {
				group_end = i - 1;
			} else {
				group_begin = i;
				return "[" + file.substr(group_begin, group_end - group_begin) + "]" + extension;
			}
		}
	}
	if (group_end == -1) {
		return "[" + file + "]" + extension;
	}
	return "[" + file.substr(0, group_end) + "]" + extension;
}

namespace duckdb {

void RegisterSqllogictests() {
	vector<string> excludes = {
	    "test/select1.test", // tested separately
	    "test/select2.test", "test/select3.test", "test/select4.test",
	    "test/index",                     // no index yet
	    "random/groupby/",                // having column binding issue with first
	    "random/select/slt_good_70.test", // join on not between
	    "random/expr/slt_good_10.test",   // these all fail because the AVG
	                                      // decimal rewrite
	    "random/expr/slt_good_102.test", "random/expr/slt_good_107.test", "random/expr/slt_good_108.test",
	    "random/expr/slt_good_109.test", "random/expr/slt_good_111.test", "random/expr/slt_good_112.test",
	    "random/expr/slt_good_113.test", "random/expr/slt_good_115.test", "random/expr/slt_good_116.test",
	    "random/expr/slt_good_117.test", "random/expr/slt_good_13.test", "random/expr/slt_good_15.test",
	    "random/expr/slt_good_16.test", "random/expr/slt_good_17.test", "random/expr/slt_good_19.test",
	    "random/expr/slt_good_21.test", "random/expr/slt_good_22.test", "random/expr/slt_good_24.test",
	    "random/expr/slt_good_28.test", "random/expr/slt_good_29.test", "random/expr/slt_good_3.test",
	    "random/expr/slt_good_30.test", "random/expr/slt_good_34.test", "random/expr/slt_good_38.test",
	    "random/expr/slt_good_4.test", "random/expr/slt_good_41.test", "random/expr/slt_good_44.test",
	    "random/expr/slt_good_45.test", "random/expr/slt_good_49.test", "random/expr/slt_good_52.test",
	    "random/expr/slt_good_53.test", "random/expr/slt_good_55.test", "random/expr/slt_good_59.test",
	    "random/expr/slt_good_6.test", "random/expr/slt_good_60.test", "random/expr/slt_good_63.test",
	    "random/expr/slt_good_64.test", "random/expr/slt_good_67.test", "random/expr/slt_good_69.test",
	    "random/expr/slt_good_7.test", "random/expr/slt_good_71.test", "random/expr/slt_good_72.test",
	    "random/expr/slt_good_8.test", "random/expr/slt_good_80.test", "random/expr/slt_good_82.test",
	    "random/expr/slt_good_85.test", "random/expr/slt_good_9.test", "random/expr/slt_good_90.test",
	    "random/expr/slt_good_91.test", "random/expr/slt_good_94.test", "random/expr/slt_good_95.test",
	    "random/expr/slt_good_96.test", "random/expr/slt_good_99.test", "random/aggregates/slt_good_2.test",
	    "random/aggregates/slt_good_5.test", "random/aggregates/slt_good_7.test", "random/aggregates/slt_good_9.test",
	    "random/aggregates/slt_good_17.test", "random/aggregates/slt_good_28.test",
	    "random/aggregates/slt_good_45.test", "random/aggregates/slt_good_50.test",
	    "random/aggregates/slt_good_52.test", "random/aggregates/slt_good_58.test",
	    "random/aggregates/slt_good_65.test", "random/aggregates/slt_good_66.test",
	    "random/aggregates/slt_good_76.test", "random/aggregates/slt_good_81.test",
	    "random/aggregates/slt_good_90.test", "random/aggregates/slt_good_96.test",
	    "random/aggregates/slt_good_102.test", "random/aggregates/slt_good_106.test",
	    "random/aggregates/slt_good_112.test", "random/aggregates/slt_good_118.test",
	    "third_party/sqllogictest/test/evidence/in1.test", // UNIQUE index on text
	    "evidence/slt_lang_replace.test",                  // feature not supported
	    "evidence/slt_lang_reindex.test",                  // "
	    "evidence/slt_lang_dropindex.test",                // "
	    "evidence/slt_lang_createtrigger.test",            // "
	    "evidence/slt_lang_droptrigger.test",              // "
	    "evidence/slt_lang_update.test",                   //  Multiple assignments to same column "x"
	    // these fail because of overflows in multiplications (sqlite does automatic upcasting)
	    "random/aggregates/slt_good_51.test", "random/aggregates/slt_good_73.test", "random/aggregates/slt_good_3.test",
	    "random/aggregates/slt_good_64.test", "random/aggregates/slt_good_122.test",
	    "random/aggregates/slt_good_110.test", "random/aggregates/slt_good_101.test",
	    "random/aggregates/slt_good_56.test", "random/aggregates/slt_good_75.test", "random/expr/slt_good_51.test",
	    "random/expr/slt_good_77.test", "random/expr/slt_good_66.test", "random/expr/slt_good_0.test",
	    "random/expr/slt_good_61.test", "random/expr/slt_good_47.test", "random/expr/slt_good_11.test",
	    "random/expr/slt_good_40.test", "random/expr/slt_good_42.test", "random/expr/slt_good_27.test",
	    "random/expr/slt_good_103.test", "random/expr/slt_good_75.test"};
	unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	listFiles(*fs, fs->JoinPath(fs->JoinPath("third_party", "sqllogictest"), "test"), [excludes](const string &path) {
		if (endsWith(path, ".test")) {
			for (auto excl : excludes) {
				if (path.find(excl) != string::npos) {
					return;
				}
			}
			REGISTER_TEST_CASE(testRunner, StringUtil::Replace(path, "\\", "/"), "[sqlitelogic][.]");
		}
	});
	listFiles(*fs, "test", [excludes](const string &path) {
		if (endsWith(path, ".test") || endsWith(path, ".test_slow") || endsWith(path, ".test_coverage")) {
			// parse the name / group from the test
			REGISTER_TEST_CASE(testRunner, StringUtil::Replace(path, "\\", "/"), ParseGroupFromPath(path));
		}
	});
}
} // namespace duckdb