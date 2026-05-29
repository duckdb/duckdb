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
		CHECK(output.GetAuthority() == "localhost");
		CHECK(output.GetAnchor() == "/");
		CHECK(output.GetPath() == "a/b");

#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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

#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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
#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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

#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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

#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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
#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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

		    make_tuple(ERR, "s3://b/foo",      "s3://b/foobar",   ""),
		    make_tuple(ERR, "s3://this/foo", "s3://that/foo/bar", ""),
		    make_tuple(ERR, "s3://this/FOO", "s3://this/foo/bar", ""),

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

#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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
#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
		for (size_t pos = 0; (pos = out.find('/', pos)) != string::npos; pos++) {
			out[pos] = '\\';
		}
#endif
		return out;
	};

	std::string input;

	SECTION("IsAbsolute + IsRelative + IsLocal") {
		bool is_absolute_exp = false, is_local_exp = false;
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
#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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
		bool exp = false;

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
#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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
#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
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
#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
		CHECK(L(Path::FromString(R"(C:\foo\)").GetTrailingSeparator()) == L("/"));
		CHECK(Path::FromString(R"(C:\)").GetTrailingSeparator()        == "");  // no segments
#endif
	}
	// clang-format on
}

TEST_CASE("Path::GetSchemeKind", "[path]") {
	using SK = Path::SchemeKind;
	// clang-format off
	CHECK(Path::FromString("").GetSchemeKind()                    == SK::None);
	CHECK(Path::FromString("a/b").GetSchemeKind()                 == SK::None);
	CHECK(Path::FromString("/a/b").GetSchemeKind()                == SK::None);

	CHECK(Path::FromString("file:/a/b").GetSchemeKind()           == SK::File);
	CHECK(Path::FromString("file:///a/b").GetSchemeKind()         == SK::File);
	CHECK(Path::FromString("file://localhost/a/b").GetSchemeKind()== SK::File);

	CHECK(Path::FromString("s3://bucket/foo").GetSchemeKind()     == SK::URI);
	CHECK(Path::FromString("az://container/foo").GetSchemeKind()  == SK::URI);
	CHECK(Path::FromString("http://host/path").GetSchemeKind()    == SK::URI);

#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
	CHECK(Path::FromString(R"(C:\foo)").GetSchemeKind()               == SK::None);
	CHECK(Path::FromString(R"(\\server\share\p)").GetSchemeKind()     == SK::UNC);
	CHECK(Path::FromString(R"(\\?\C:\foo)").GetSchemeKind()           == SK::Verbatim);
	CHECK(Path::FromString(R"(\\?\UNC\server\share)").GetSchemeKind() == SK::Verbatim);

	// IsLocal() in terms of SchemeKind
	CHECK( Path::FromString(R"(C:\foo)").IsLocal());         // None + drive
	CHECK(!Path::FromString(R"(\\server\share\p)").IsLocal()); // UNC = remote
	CHECK( Path::FromString(R"(\\?\C:\foo)").IsLocal());     // Verbatim + drive = local
	CHECK(!Path::FromString(R"(\\?\UNC\s\sh)").IsLocal());   // Verbatim + no drive = remote
#endif

	// Is*Scheme wrappers
	CHECK( Path::FromString("/a/b").IsNoneScheme());
	CHECK(!Path::FromString("/a/b").IsFileScheme());
	CHECK( Path::FromString("file:///a/b").IsFileScheme());
	CHECK( Path::FromString("s3://b/p").IsURIScheme());
	CHECK(!Path::FromString("s3://b/p").IsLocal());
	// clang-format on
}

TEST_CASE("Path Join tests", "[path]") {
	// confirm that Join("..") and Parent() behaviors are identical in corner case
	auto dotdot = Path::FromString("..");
	CHECK(dotdot.Parent().ToString() == "../..");
	CHECK(dotdot.Join("..").ToString() == "../..");

	auto dotdot_a = Path::FromString("../a");
	CHECK(dotdot_a.Parent().ToString() == "..");
	CHECK(dotdot_a.Join("..").ToString() == "..");

	// confirm multi-joins
	CHECK(Path::FromString("a").Join("b").ToString() == "a/b");
	CHECK(Path::FromString("a").Join("b", "c").ToString() == "a/b/c");
	CHECK(Path::FromString("a").Join("b", "c", "d").ToString() == "a/b/c/d");

	CHECK(Path::FromString("a").Join(std::vector<string>()).ToString() == "a");
	CHECK(Path::FromString("a").Join(std::vector<string>({"b"})).ToString() == "a/b");
	CHECK(Path::FromString("a").Join(std::vector<string>({"b", "c"})).ToString() == "a/b/c");
}

TEST_CASE("Path::IsRelativeTo + RelativeTo", "[path]") {
	using std::make_tuple;

	// clang-format off
	SECTION("IsRelativeTo") {
		std::string path_str, base_str;
		bool expected = false;
		std::tie(path_str, base_str, expected) = GENERATE(table<std::string, std::string, bool>({
		    make_tuple("/a/b/c/d.txt",    "/a/b/c",         true),  // direct parent
		    make_tuple("/a/b/c/d.txt",    "/a/b",           true),  // grandparent
		    make_tuple("/a/b/c/d.txt",    "/a",             true),  // further ancestor
		    make_tuple("/a/b/c/d.txt",    "/",              true),  // root
		    make_tuple("/a/b/c/d.txt",    "/a/b/c/d.txt",   true),  // self (equality)
		    make_tuple("/a/b/c/d.txt",    "/x/y",           false), // different path
		    make_tuple("/a/b/c/d.txt",    "/a/b/c/d.txt/e", false), // base longer than path
		    make_tuple("s3://bucket/a/b", "s3://bucket/a",  true),
		    make_tuple("s3://bucket/a/b", "s3://other/a",   false), // authority mismatch
		    make_tuple("s3://bucket/a/b", "/a",             false), // scheme mismatch
		    make_tuple("s3://bucket/a/b", "az://bucket/a",  false), // scheme mismatch
		    make_tuple("file:///a/b/c",   "file:///a/b",    true),
		    make_tuple("file:///a/b/c",   "file:/a/b",      false), // scheme differs
		}));

		CAPTURE(path_str, base_str);
		CHECK(Path::FromString(path_str).IsRelativeTo(Path::FromString(base_str)) == expected);
	}

	SECTION("RelativeTo") {
		std::string path_str, base_str, rel_exp;
		std::tie(path_str, base_str, rel_exp) = GENERATE(table<std::string, std::string, std::string>({
		    make_tuple("/a/b/c/d.txt", "/a/b/c",        "d.txt"),
		    make_tuple("/a/b/c/d.txt", "/a/b",          "c/d.txt"),
		    make_tuple("/a/b/c/d.txt", "/",             "a/b/c/d.txt"),
		    make_tuple("/a/b/c",       "/a/b/c",        "."),       // equality → "."
		    make_tuple("/a/b/c/",      "/a/b",          "c/"),      // trailing sep preserved
		    make_tuple("s3://b/x/y/z", "s3://b/x",      "y/z"),
		}));

		CAPTURE(path_str, base_str);
		CHECK(Path::FromString(path_str).RelativeTo(Path::FromString(base_str)).ToString() == rel_exp);
	}

	SECTION("RelativeTo throws") {
		CHECK_THROWS(Path::FromString("/a/b/c").RelativeTo(Path::FromString("/x/y")));
		CHECK_THROWS(Path::FromString("/a/b/c").RelativeTo(Path::FromString("/a/b/c/d")));
		CHECK_THROWS(Path::FromString("s3://b/a").RelativeTo(Path::FromString("/a")));
	}
	// clang-format on
}

TEST_CASE("Path::ToBareLocal", "[path]") {
	// clang-format off
	// absolute: scheme/authority stripped
	CHECK(Path::FromString("/a/b").ToBareLocal().ToString()                    == "/a/b");
	CHECK(Path::FromString("file:/a/b").ToBareLocal().ToString()               == "/a/b");
	CHECK(Path::FromString("file://localhost/a/b").ToBareLocal().ToString()    == "/a/b");
	CHECK(Path::FromString("file:///a/b").ToBareLocal().ToString()             == "/a/b");
	CHECK(Path::FromString("file:///a/b/").ToBareLocal().ToString()            == "/a/b/");

	// relative: already bare, passes through
	CHECK(Path::FromString("a/b").ToBareLocal().ToString()                     == "a/b");

	// remote throws
	CHECK_THROWS(Path::FromString("s3://bucket/foo").ToBareLocal());
	CHECK_THROWS(Path::FromString("az://container/foo").ToBareLocal());
	// clang-format on
}

TEST_CASE("Path::ToFileURI", "[path]") {
	// clang-format off
	// local absolute forms normalize to file:///...
	CHECK(Path::FromString("/a/b").ToFileURI().ToString()                 == "file:///a/b");
	CHECK(Path::FromString("file:/a/b").ToFileURI().ToString()            == "file:///a/b");
	CHECK(Path::FromString("file:///a/b").ToFileURI().ToString()          == "file:///a/b");
	CHECK(Path::FromString("file://localhost/a/b").ToFileURI().ToString() == "file:///a/b");
	CHECK(Path::FromString("/a/b/").ToFileURI().ToString()                == "file:///a/b/");

	// remote or relative throws
	CHECK_THROWS(Path::FromString("s3://bucket/foo").ToFileURI());
	CHECK_THROWS(Path::FromString("az://container/foo").ToFileURI());
	CHECK_THROWS(Path::FromString("a/b").ToFileURI());
	// clang-format on
}

TEST_CASE("Path::IsDot", "[path]") {
	// clang-format off
	CHECK(Path().IsDot()                      == true);
	CHECK(Path::FromString("").IsDot()        == true);
	CHECK(Path::FromString(".").IsDot()       == true);
	CHECK(Path::FromString("a/..").IsDot()    == true);
	CHECK(Path::FromString("a/../.").IsDot()  == true);

	CHECK(Path::FromString("/").IsDot()       == false); // absolute root
	CHECK(Path::FromString("/a").IsDot()      == false);
	CHECK(Path::FromString("a").IsDot()       == false);
	CHECK(Path::FromString("..").IsDot()      == false); // relative but has segment
	// clang-format on
}

TEST_CASE("Path::Equals / EqualsCI / EqualsFS", "[path]") {
	// clang-format off

	// Equals: always case-sensitive, equivalent to ToString() == ToString()
	CHECK( Path::FromString("/a/b").Equals(Path::FromString("/a/b")));
	CHECK(!Path::FromString("/a/b").Equals(Path::FromString("file:/a/b")));         // scheme differs
	CHECK(!Path::FromString("file:/a/b").Equals(Path::FromString("file:///a/b")));  // scheme differs
	CHECK(!Path::FromString("/a/b").Equals(Path::FromString("/a/B")));              // case differs

	// EqualsCI: always case-insensitive throughout
	CHECK( Path::FromString("/a/b").EqualsCI(Path::FromString("/a/B")));
	CHECK( Path::FromString("/A/B").EqualsCI(Path::FromString("/a/b")));
	CHECK(!Path::FromString("/a/b").EqualsCI(Path::FromString("file:/a/b")));       // scheme still differs

	// EqualsFS: CI for Windows-shaped paths, case-sensitive otherwise
    const auto fs_win =
#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
        true;
#else
        false;
#endif
	CHECK(Path::FromString("/a/b").EqualsFS(Path::FromString("/a/B")) == fs_win);               // posix local: CS
	CHECK(Path::FromString("a/b").EqualsFS(Path::FromString("a/B")) == fs_win);                 // relative off-win: CS
	CHECK(!Path::FromString("s3://bucket/a/b").EqualsFS(Path::FromString("s3://bucket/a/B")));  // URI: CS

	// ToCanonical: relative paths throw
	CHECK_THROWS(Path::FromString("a/b").ToCanonical());

	// CanonicalFromString
	CHECK(Path::CanonicalFromString("file:/a/b"           ).ToString() == "/a/b");
	CHECK(Path::CanonicalFromString("file:///a/b"         ).ToString() == "/a/b");
	CHECK(Path::CanonicalFromString("file://localhost/a/b").ToString() == "/a/b");
	CHECK(Path::CanonicalFromString("/a/b"                ).ToString() == "/a/b");            // no-op
	CHECK(Path::CanonicalFromString("s3://bucket/a/b"     ).ToString() == "s3://bucket/a/b"); // remote: no-op
	CHECK_THROWS(Path::CanonicalFromString("a/b"));                                           // relative: throws

#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
	// \\?\ extended-length local → strip prefix
	CHECK(Path::CanonicalFromString(R"(\\?\C:\foo\bar)"        ).ToString() == R"(C:\foo\bar)");
	// \\?\UNC\ extended UNC → regular UNC
	CHECK(Path::CanonicalFromString(R"(\\?\UNC\server\share\p)").ToString() == R"(\\server\share\p)");
	// regular UNC and drive paths are already canonical
	CHECK(Path::CanonicalFromString(R"(\\server\share\p)"      ).ToString() == R"(\\server\share\p)");
	CHECK(Path::CanonicalFromString(R"(C:\foo\bar)"            ).ToString() == R"(C:\foo\bar)");
#endif
	// clang-format on
}
