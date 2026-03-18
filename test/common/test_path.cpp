#include "catch.hpp"
#include "duckdb/common/path.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"

using namespace duckdb;
using namespace std;

// ------------------------------------------------------------------------------------------------
// Path struct tests
// ------------------------------------------------------------------------------------------------

TEST_CASE("Path parses and correctly structures fields", "[path]") {
	Path output;

	SECTION("local files") {
		output = Path::FromString("a/b");
		CHECK(output.GetScheme() == "");
		CHECK(output.GetAuthority() == "");
		CHECK(output.GetAnchor() == "");
		CHECK(output.IsAbsolute() == false);
		CHECK(output.GetPath() == "a/b");

		output = Path::FromString("/..////a/./b/../c");
		CHECK(output.GetScheme() == "");
		CHECK(output.GetAuthority() == "");
		CHECK(output.GetAnchor() == "/");
		CHECK(output.GetPath() == "a/c");
		CHECK(output.IsAbsolute() == true);
	}

	SECTION("file schemes") {
		output = Path::FromString("file:/a/b");
		CHECK(output.GetScheme() == "file:");
		CHECK(output.GetAuthority() == "");
		CHECK(output.GetAnchor() == "/");
		CHECK(output.IsAbsolute() == true);
		CHECK(output.GetPath() == "a/b");

		output = Path::FromString("file://localhost/a/b");
		CHECK(output.GetScheme() == "file://");
		CHECK(output.GetAuthority() == "localhost");
		CHECK(output.GetAnchor() == "/");
		CHECK(output.IsAbsolute() == true);
		CHECK(output.GetPath() == "a/b");

		output = Path::FromString("file:///a/b");
		CHECK(output.GetScheme() == "file://");
		CHECK(output.GetAuthority() == "");
		CHECK(output.GetAnchor() == "/");
		CHECK(output.IsAbsolute() == true);
		CHECK(output.GetPath() == "a/b");

		output = Path::FromString("file://LOCALHOST/a/b");
		CHECK(output.GetScheme() == "file://");
		CHECK(output.GetAuthority() == "LOCALHOST");
		CHECK(output.GetAnchor() == "/");
		CHECK(output.GetPath() == "a/b");

#if defined(_WIN32)
		output = Path::FromString(R"(c:..\foo)");
		CHECK(output.GetScheme() == "");
		CHECK(output.GetAuthority() == "");
		CHECK(output.GetAnchor() == "C:");
		CHECK(output.IsAbsolute() == false);
		CHECK(output.GetPath() == R"(..\foo)");

		output = Path::FromString(R"(c:\..\foo)");
		CHECK(output.GetScheme() == "");
		CHECK(output.GetAuthority() == "");
		CHECK(output.GetAnchor() == R"(C:\)");
		CHECK(output.IsAbsolute() == true);
		CHECK(output.GetPath() == "foo");
#endif
	}

	SECTION("URI schemes") {
		output = Path::FromString("az://container/b/c");
		CHECK(output.GetScheme() == "az://");
		CHECK(output.GetAuthority() == "container");
		CHECK(output.GetAnchor() == "/");
		CHECK(output.IsAbsolute());
		CHECK(output.GetPath() == "b/c");
	}

#if defined(_WIN32)
	SECTION("UNC schemes") {
		output = Path::FromString(R"(\\foo\bar)");
		CHECK(output.GetScheme() == R"(\\)");
		CHECK(output.GetAuthority() == R"(foo\bar)");
		CHECK(output.GetAnchor() == R"(\)");
		CHECK(output.GetPath().empty());

		CHECK_THROWS(Path::FromString(R"(\\\\ab)"));
		CHECK_THROWS(Path::FromString(R"(\\foo\)"));
		CHECK_THROWS(Path::FromString(R"(\\foo\\)"));
	}
#endif
}

TEST_CASE("Path::FromString/ToString round-trips", "[path]") {
	using std::make_tuple;

	enum ResultType { ERR = false, OK_ = true };

	ResultType return_exp;
	std::string input;
	std::string output_exp;

	// L() converts / to the local separator (mirrors playground L helper)
	auto L = [](const string &path) {
		string out(path);
#ifdef _WIN32
		for (size_t pos = 0; (pos = out.find('/', pos)) != string::npos; pos++) {
			out[pos] = '\\';
		}
#endif
		return out;
	};

	// clang-format off
	SECTION("local files") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "",         "."),
		    make_tuple(OK_, "a",        "a"),
		    make_tuple(OK_, "a/b",      "a/b"),
		    make_tuple(OK_, ".",        "."),
		    make_tuple(OK_, "..",       ".."),
		    make_tuple(OK_, "/",        "/"),
		    make_tuple(OK_, "/a",       "/a"),
		    make_tuple(OK_, "/a/b",     "/a/b"),
		    make_tuple(OK_, "/a/b/",    "/a/b/"),

		    // backslash ok separator on all platforms; confirm output sep = first sep in input
		    make_tuple(OK_, R"(a\b)",             "a/b"),
		    make_tuple(OK_, R"(/a\b\c)",          "/a/b/c"),
		    make_tuple(OK_, R"(/data\csv\*.csv)", "/data/csv/*.csv"),

		    make_tuple(OK_, "foo/bar://baz",  "foo/bar:/baz"),
		    make_tuple(OK_, "/foo/bar://baz", "/foo/bar:/baz"),
		}));
	}

#if defined(_WIN32)
	SECTION("local files (windows drive)") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "C:",   "C:"),
		    make_tuple(OK_, "c:b",  "C:b"),
		    make_tuple(OK_, "c:/b", "C:/b"),
		}));
	}
#endif

	SECTION("file schemes") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "file:/",       "file:/"),
		    make_tuple(OK_, "file:/a",      "file:/a"),
		    make_tuple(OK_, "file:/a/b",    "file:/a/b"),

		    make_tuple(OK_, "file://localhost/",    "file://localhost/"),
		    make_tuple(OK_, "file://localhost/a",   "file://localhost/a"),
		    make_tuple(OK_, "file://localhost/a/b", "file://localhost/a/b"),

		    make_tuple(ERR, "file://otherhost/a/b", ""),

		    make_tuple(OK_, "file:///",     "file:///"),
		    make_tuple(OK_, "file:///a",    "file:///a"),
		    make_tuple(OK_, "file:///a/b",  "file:///a/b"),
		}));
	}

#if defined(_WIN32)
	SECTION("file schemes (windows drive)") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "file:/c:/b",               "file:/C:/b"),
		    make_tuple(OK_, "file://localhost/c://b",   "file://localhost/C:/b"),
		    make_tuple(OK_, "file:///c:///b",           "file:///C:/b"),
		    make_tuple(OK_, "file://localhost//c://b",  "file://localhost/c:/b"),
		    make_tuple(OK_, "file://localhost/c://localhost/b", "file://localhost/C:/localhost/b"),
		}));
	}
#endif

	SECTION("URI schemes") {
		std::tie(return_exp, input, output_exp) = GENERATE(table<ResultType, std::string, std::string>({
		    make_tuple(OK_, "S3://bucket/bar/baz",     "s3://bucket/bar/baz"),
		    make_tuple(OK_, "s3://bucket/bar/baz",     "s3://bucket/bar/baz"),
		    make_tuple(OK_, "s3://bucket/bar/../baz",  "s3://bucket/baz"),
		    make_tuple(OK_, "s3://bucket/..",          "s3://bucket/"),
		    make_tuple(OK_, "s3://bucket/../..",       "s3://bucket/"),
		    make_tuple(OK_, "s3://bucket/c:/B/",       "s3://bucket/c:/B/"),
		}));
	}
	// clang-format on

	CAPTURE(input);
	if (return_exp == OK_) {
		CHECK(L(Path::FromString(input).ToString()) == L(output_exp));
	} else {
		CHECK_THROWS(Path::FromString(input).ToString());
	}
}

TEST_CASE("Path::JoinPath table-based tests", "[path]") {
	using std::make_tuple;

	enum ResultType { ERR = false, OK_ = true };

	ResultType return_exp;
	std::string lhs, rhs, joined_exp;

	auto L = [](const string &path) {
		string out(path);
#ifdef _WIN32
		for (size_t pos = 0; (pos = out.find('/', pos)) != string::npos; pos++) {
			out[pos] = '\\';
		}
#endif
		return out;
	};

	auto do_join = [](const string &a, const string &b) {
		return Path::FromString(a).Join(b).ToString();
	};

	// clang-format off
	SECTION("local files") {
		std::tie(return_exp, lhs, rhs, joined_exp) = GENERATE(table<ResultType, std::string, std::string, std::string>({
		    make_tuple(OK_, "",   "",  "."),
		    make_tuple(OK_, "",   ".", "."),
		    make_tuple(OK_, ".",  "",  "."),
		    make_tuple(OK_, ".",  ".", "."),
		    make_tuple(OK_, "",   "a", "a"),
		    make_tuple(OK_, "a",  "",  "a"),
		    make_tuple(OK_, "a",  "b", "a/b"),
		    make_tuple(OK_, "/.", ".", "/"),
		    make_tuple(OK_, "/",  "a", "/a"),
		    make_tuple(OK_, "/a", "",  "/a"),
		    make_tuple(OK_, "/a", "b", "/a/b"),

		    // abs path sub-path helper
		    make_tuple(OK_, "/",  "/a",     "/a"),
		    make_tuple(OK_, "/",  "/a/b/c", "/a/b/c"),
		    make_tuple(OK_, "/a", "/a",     "/a"),
		    make_tuple(OK_, "/a", "/a/b",   "/a/b"),

		    // abs path fails
		    make_tuple(ERR, "/a", "/",  ""),
		    make_tuple(ERR, "/a", "/b", ""),

		    // extra slashes
		    make_tuple(OK_, "dir//sub/", "./file",      "dir/sub/file"),
		    make_tuple(OK_, "dir/sub", "../sibling",    "dir/sibling"),
		    make_tuple(OK_, "dir///", "nested///child", "dir/nested/child"),

		    // single & double dots
		    make_tuple(OK_, "/", "./..",         "/"),
		    make_tuple(OK_, "", "./..",          ".."),
		    make_tuple(OK_, "..", "a/..",        ".."),
		    make_tuple(OK_, "..", "../a",        "../../a"),
		    make_tuple(OK_, "./..", "./..",      "../.."),
		    make_tuple(OK_, "a/bar", "..",       "a"),
		    make_tuple(OK_, "a/bar", "../..",    "."),
		    make_tuple(OK_, "a/bar", "../../..", ".."),

		    make_tuple(OK_, "./a/././b/./", "././c/././", "a/b/c/"),

		    make_tuple(OK_, "dir/sub/..", "sibling",        "dir/sibling"),
		    make_tuple(OK_, "./dir/sub/..", "sibling",      "dir/sibling"),
		    make_tuple(OK_, "./dir/sub/./..", "sibling",    "dir/sibling"),
		    make_tuple(OK_, "dir/..", "../..",              "../.."),
		    make_tuple(OK_, "/usr/local", "../..",          "/"),
		    make_tuple(OK_, "/", "usr/local",               "/usr/local"),
		    make_tuple(OK_, "/usr/local", "../../..",       "/"),

		    make_tuple(ERR, "dir", "/abs/path", ""),
		    make_tuple(ERR, "/fo",  "/foobar",  ""),

		    // scheme-like token embedded after leading slash is treated as path, not scheme
		    make_tuple(OK_, "/foo/proto://bar", "a", "/foo/proto:/bar/a"),
		}));
	}

	SECTION("file and URI schemes") {
		std::tie(return_exp, lhs, rhs, joined_exp) = GENERATE(table<ResultType, std::string, std::string, std::string>({
		    make_tuple(OK_, "file:/", "",                   "file:/"),
		    make_tuple(OK_, "file:/", "../..",              "file:/"),
		    make_tuple(OK_, "file:/", "bin",                "file:/bin"),
		    make_tuple(OK_, "file:/usr", "",                "file:/usr"),
		    make_tuple(OK_, "file:/usr", "bin",             "file:/usr/bin"),
		    make_tuple(OK_, "file:/usr/local", "../bin",    "file:/usr/bin"),

		    make_tuple(OK_, "file://localhost/usr", "../bin",       "file://localhost/bin"),
		    make_tuple(OK_, "file://localhost/usr/local", "../bin", "file://localhost/usr/bin"),
		    make_tuple(OK_, "file:///usr/local", "../bin",          "file:///usr/bin"),

		    make_tuple(OK_, "s3://host", "bar/baz", "s3://host/bar/baz"),
		    make_tuple(OK_, "s3://host", "..",      "s3://host/"),
		    make_tuple(OK_, "s3://host", "../..",   "s3://host/"),

		    make_tuple(ERR, "/usr/local", "/var/log",   ""),
		    make_tuple(ERR, "s3://foo", "/foo/bar/baz", ""),
		    make_tuple(ERR, "s3://foo", "az://foo",     ""),

		    make_tuple(ERR, "s3://b/foo",      "s3://b/foobar",       ""),
		    make_tuple(ERR, "s3://BUCKET/foo", "s3://bucket/foo/bar", ""),
		    make_tuple(ERR, "s3://bucket/FOO", "s3://bucket/foo/bar", ""),

		    // sub-path joins
		    make_tuple(OK_, "file:/usr",             "file:/usr/local",            "file:/usr/local"),
		    make_tuple(OK_, "file://localhost/usr",  "file://localhost/usr/local", "file://localhost/usr/local"),
		    make_tuple(OK_, "file:///usr",           "file:///usr/local",          "file:///usr/local"),
		    make_tuple(OK_, "s3://bucket/a",         "s3://bucket/a/b",            "s3://bucket/a/b"),
		    make_tuple(OK_, "s3://bucket/a",         "s3://bucket/a",              "s3://bucket/a"),
		    // scheme normalized to lowercase (RFC 3986)
		    make_tuple(OK_, "S3://bucket/a",         "s3://bucket/a/b",            "s3://bucket/a/b"),
		}));
	}

#if defined(_WIN32)
	SECTION("windows files and schemes") {
		std::tie(return_exp, lhs, rhs, joined_exp) = GENERATE(table<ResultType, std::string, std::string, std::string>({
		    make_tuple(OK_, R"(C:\)", R"(..)",              R"(C:\)"),
		    make_tuple(OK_, R"(C:\)", R"(..\..)",           R"(C:\)"),
		    make_tuple(OK_, R"(C:\)", "system32",           R"(C:\system32)"),

		    make_tuple(OK_, "C:", "system32",               "C:system32"),
		    make_tuple(OK_, "C:relpath", "sub",             R"(C:relpath\sub)"),
		    make_tuple(OK_, "C:relpath", R"(..\sib)",       "C:sib"),

		    make_tuple(ERR, R"(C:\foo)", R"(D:\bar)",       ""),
		    make_tuple(ERR, R"(C:\foo)", R"(D:bar)",        ""),
		    make_tuple(ERR, R"(C:foo)", R"(D:bar)",         ""),
		    make_tuple(ERR, R"(C:\foo)", R"(D:\foo\bar)",   ""),

		    // extended UNC variants
		    make_tuple(OK_, R"(\\server\share)", R"(sub)",              R"(\\server\share\sub)"),
		    make_tuple(OK_, R"(\\?\UNC\server\share)", R"(sub\.\dir)",  R"(\\?\UNC\server\share\sub\dir)"),
		    make_tuple(OK_, R"(\\?\c:\sub)", R"(dir)",                  R"(\\?\C:\sub\dir)"),

		    // '..' dropping at front of abs path
		    make_tuple(OK_, R"(\\server\share)", R"(..\sibling)", R"(\\server\share\sibling)"),

		    // case-insensitive path prefix
		    make_tuple(OK_, R"(C:\FOO)",  R"(C:\foo\bar)",  R"(C:\foo\bar)"),
		    make_tuple(OK_, "C:/FOO",     "C:/foo/bar",     "C:/foo/bar"),

		    // case-insensitive UNC authority
		    make_tuple(OK_, R"(\\Server\Share\foo)",  R"(\\server\share\foo\bar)",  R"(\\Server\Share\foo\bar)"),
		}));
	}
#endif
	// clang-format on

	CAPTURE(lhs, rhs);
	if (return_exp == OK_) {
		CHECK(L(do_join(lhs, rhs)) == L(joined_exp));
	} else {
		CHECK_THROWS(do_join(lhs, rhs));
	}
}

TEST_CASE("Path attributes", "[path]") {
	using std::make_tuple;

	auto L = [](const string &path) {
		string out(path);
#ifdef _WIN32
		for (size_t pos = 0; (pos = out.find('/', pos)) != string::npos; pos++) {
			out[pos] = '\\';
		}
#endif
		return out;
	};

	std::string input;

	SECTION("IsAbsolute + IsRelative + IsLocal") {
		bool is_absolute_exp, is_local_exp;
		enum AbsType { REL = false, ABS = true };
		enum LocalTypeType { REMOTE = false, LOCAL_ = true };

		// clang-format off
		SECTION("posix and URI") {
			std::tie(input, is_absolute_exp, is_local_exp) = GENERATE(table<std::string, bool, bool>({
			    make_tuple("",                    REL, LOCAL_),
			    make_tuple("a/b",                 REL, LOCAL_),
			    make_tuple("/",                   ABS, LOCAL_),
			    make_tuple("/a/b",                ABS, LOCAL_),
			    make_tuple("file:/a/b",           ABS, LOCAL_),
			    make_tuple("file:///a/b",         ABS, LOCAL_),
			    make_tuple("s3://bucket/foo",     ABS, REMOTE),
			    make_tuple("az://container/foo",  ABS, REMOTE),
			    make_tuple("http://host/path",    ABS, REMOTE),
			}));
		}
#if defined(_WIN32)
		SECTION("windows") {
			std::tie(input, is_absolute_exp, is_local_exp) = GENERATE(table<std::string, bool, bool>({
			    make_tuple(R"(C:\foo)",          ABS, LOCAL_),
			    make_tuple(R"(C:foo)",           REL, LOCAL_),
			    make_tuple(R"(\\server\share)",  ABS, REMOTE),
			    make_tuple(R"(\\?\C:\foo)",      ABS, LOCAL_),
			}));
		}
#endif
		// clang-format on

		CAPTURE(input);
		auto p = Path::FromString(input);
		CHECK(p.IsAbsolute() == is_absolute_exp);
		CHECK(p.IsRelative() == !is_absolute_exp);
		CHECK(p.IsLocal() == is_local_exp);
	}

	SECTION("HasTrailingSeparator") {
		bool exp;

		// clang-format off
		SECTION("posix and URI") {
			std::tie(input, exp) = GENERATE(table<std::string, bool>({
			    make_tuple("",           false),
			    make_tuple("foo/bar",    false),
			    make_tuple("foo/bar/",   true),
			    make_tuple("/",          true),
			    make_tuple("/a/b",       false),
			    make_tuple("/a/b/",      true),
			    make_tuple("s3://b/p",   false),
			    make_tuple("s3://b/p/",  true),
			}));
		}
#if defined(_WIN32)
		SECTION("windows") {
			std::tie(input, exp) = GENERATE(table<std::string, bool>({
			    make_tuple("C:/",        true),
			    make_tuple("C:/foo",     false),
			    make_tuple("C:/foo/",    true),
			    make_tuple(R"(C:\)",     true),
			    make_tuple(R"(C:\foo)",  false),
			    make_tuple(R"(C:\foo\)", true),
			}));
		}
#endif

		CAPTURE(input);
		CHECK(Path::FromString(input).HasTrailingSeparator() == exp);
	}

	SECTION("trailing separator ToString") {
		// bare anchor: anchor already ends with sep, no double-emit
		CHECK(L(Path::FromString("/").ToString()) 				== L("/"));
		CHECK(L(Path::FromString("/foo/bar/").ToString()) 		== L("/foo/bar/"));
		CHECK(L(Path::FromString("/foo/bar").ToString()) 		== L("/foo/bar"));
		CHECK(Path::FromString("s3://bucket/a/b/").ToString() 	== "s3://bucket/a/b/");
#if defined(_WIN32)
		CHECK(Path::FromString("C:/").ToString() 				== "C:/");
		CHECK(L(Path::FromString("C:/foo/").ToString()) 		== L("C:/foo/"));
#endif
	}

	SECTION("trailing separator Join") {
		CHECK(Path::FromString("a/b").Join("c/").HasTrailingSeparator() == true);
		CHECK(Path::FromString("a/b").Join("c").HasTrailingSeparator()  == false);
		CHECK(L(Path::FromString("a/b/").Join("c/").ToString()) == L("a/b/c/"));
		CHECK(L(Path::FromString("a/b/").Join("c").ToString())  == L("a/b/c"));
	}

	SECTION("GetTrailingSeparator") {
		// has segments + trailing sep → string(1, separator)
		CHECK(Path::FromString("/foo/bar/").GetTrailingSeparator()  == "/");
		CHECK(Path::FromString("foo/bar/").GetTrailingSeparator()   == "/");
		CHECK(Path::FromString("s3://b/p/").GetTrailingSeparator()  == "/");
		// has segments, no trailing sep → ""
		CHECK(Path::FromString("/foo/bar").GetTrailingSeparator()   == "");
		CHECK(Path::FromString("foo/bar").GetTrailingSeparator()    == "");
		CHECK(Path::FromString("s3://b/p").GetTrailingSeparator()   == "");
		// no segments (bare anchors): anchor already ends with sep, no double-emit
		CHECK(Path::FromString("/").GetTrailingSeparator()          == "");
		CHECK(Path::FromString("").GetTrailingSeparator()           == "");
		CHECK(Path::FromString("s3://b/").GetTrailingSeparator()    == "");
#if defined(_WIN32)
		CHECK(L(Path::FromString(R"(C:\foo\)").GetTrailingSeparator()) == L("/"));
		CHECK(Path::FromString(R"(C:\)").GetTrailingSeparator()        == "");  // no segments
#endif
	}
	// clang-format on
}

TEST_CASE("Path one-off tests", "[path]") {
	// confirm that Join("..") and Parent() behaviors are identical in corner case
	auto dotdot = Path::FromString("..");
	CHECK(dotdot.Parent().ToString() == "../..");
	CHECK(dotdot.Join("..").ToString() == "../..");

	auto dotdot_a = Path::FromString("../a");
	CHECK(dotdot_a.Parent().ToString() == "..");
	CHECK(dotdot_a.Join("..").ToString() == "..");

	CHECK(Path::FromString("a").Join("b").ToString() == "a/b");
	CHECK(Path::FromString("a").Join("b", "c").ToString() == "a/b/c");
	CHECK(Path::FromString("a").Join("b", "c", "d").ToString() == "a/b/c/d");

	CHECK(Path::FromString("a").Join(std::vector<string>()).ToString() == "a");
	CHECK(Path::FromString("a").Join(std::vector<string>({"b"})).ToString() == "a/b");
	CHECK(Path::FromString("a").Join(std::vector<string>({"b", "c"})).ToString() == "a/b/c");
}

TEST_CASE("Path::HasParentage", "[path]") {
	using std::make_tuple;

	std::string path_str, ancestor_str;
	int n;
	bool expected;

	// clang-format off
	SECTION("n=-1 (default): strict ancestry, any depth") {
		std::tie(path_str, ancestor_str, n, expected) = GENERATE(table<std::string, std::string, int, bool>({
		    make_tuple("/a/b/c/d.txt", "/a/b/c",         -1, true),  // depth 1
		    make_tuple("/a/b/c/d.txt", "/a/b",           -1, true),  // depth 2
		    make_tuple("/a/b/c/d.txt", "/a",             -1, true),  // depth 3
		    make_tuple("/a/b/c/d.txt", "/",              -1, true),  // depth 4
		    make_tuple("/a/b/c/d.txt", "/a/b/c/d.txt",   -1, false), // self (depth 0)
		    make_tuple("/a/b/c/d.txt", "/x/y",           -1, false), // different path
		    make_tuple("/a/b/c/d.txt", "/a/b/c/d.txt/e", -1, false), // ancestor longer
		}));
	}

	SECTION("n=0: equality") {
		std::tie(path_str, ancestor_str, n, expected) = GENERATE(table<std::string, std::string, int, bool>({
		    make_tuple("/a/b/c", "/a/b/c",   0, true),
		    make_tuple("/a/b/c", "/a/b",     0, false),
		    make_tuple("/a/b/c", "/a/b/c/d", 0, false),
		}));
	}

	SECTION("n=1: direct parent") {
		std::tie(path_str, ancestor_str, n, expected) = GENERATE(table<std::string, std::string, int, bool>({
		    make_tuple("/a/b/c/d.txt", "/a/b/c",       1, true),
		    make_tuple("/a/b/c/d.txt", "/a/b",         1, false),
		    make_tuple("/a/b/c/d.txt", "/a/b/c/d.txt", 1, false),
		}));
	}

	SECTION("n=2: grandparent") {
		std::tie(path_str, ancestor_str, n, expected) = GENERATE(table<std::string, std::string, int, bool>({
		    make_tuple("/a/b/c/d.txt", "/a/b",   2, true),
		    make_tuple("/a/b/c/d.txt", "/a/b/c", 2, false),
		    make_tuple("/a/b/c/d.txt", "/a",     2, false),
		}));
	}

	SECTION("scheme/authority mismatch") {
		std::tie(path_str, ancestor_str, n, expected) = GENERATE(table<std::string, std::string, int, bool>({
		    make_tuple("s3://bucket/a/b", "s3://bucket/a",  -1, true),
		    make_tuple("s3://bucket/a/b", "s3://other/a",   -1, false),
		    make_tuple("s3://bucket/a/b", "/a",             -1, false),
		    make_tuple("s3://bucket/a/b", "az://bucket/a",  -1, false),
		}));
	}

	SECTION("file: scheme must match exactly") {
		std::tie(path_str, ancestor_str, n, expected) = GENERATE(table<std::string, std::string, int, bool>({
		    make_tuple("file:///a/b/c", "file:///a/b", -1, true),
		    make_tuple("file:///a/b/c", "file:/a/b",   -1, false), // scheme differs
		}));
	}
	// clang-format on

	CAPTURE(path_str, ancestor_str, n);
	CHECK(Path::FromString(path_str).HasParentage(Path::FromString(ancestor_str), n) == expected);
}

TEST_CASE("Path::ToLocal", "[path]") {
	// clang-format off
	// local paths pass through unchanged (scheme/authority stripped)
	CHECK(Path::FromString("/a/b").ToLocal().ToString()                    == "/a/b");
	CHECK(Path::FromString("file:/a/b").ToLocal().ToString()               == "/a/b");
	CHECK(Path::FromString("file:///a/b").ToLocal().ToString()             == "/a/b");
	CHECK(Path::FromString("file://localhost/a/b").ToLocal().ToString()    == "/a/b");
	CHECK(Path::FromString("a/b").ToLocal().ToString()                     == "a/b");

	// trailing separator preserved
	CHECK(Path::FromString("file:///a/b/").ToLocal().ToString()        == "/a/b/");

	// remote paths throw
	CHECK_THROWS(Path::FromString("s3://bucket/foo").ToLocal());
	CHECK_THROWS(Path::FromString("az://container/foo").ToLocal());
	// clang-format on
}

TEST_CASE("Path::ToFileLocal", "[path]") {
	// clang-format off
	// all local forms normalize to file:///...
	CHECK(Path::FromString("/a/b").ToFileLocal().ToString()                 == "file:///a/b");
	CHECK(Path::FromString("file:/a/b").ToFileLocal().ToString()            == "file:///a/b");
	CHECK(Path::FromString("file:///a/b").ToFileLocal().ToString()          == "file:///a/b");
	CHECK(Path::FromString("file://localhost/a/b").ToFileLocal().ToString() == "file:///a/b"); // authority cleared

	// trailing separator preserved
	CHECK(Path::FromString("/a/b/").ToFileLocal().ToString()                == "file:///a/b/");

	// remote paths throw
	CHECK_THROWS(Path::FromString("s3://bucket/foo").ToFileLocal());
	CHECK_THROWS(Path::FromString("az://container/foo").ToFileLocal());
	// clang-format on
}
