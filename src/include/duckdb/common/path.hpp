//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/path.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//
// Parsed representation of a path string, covering posix, windows, URI, and UNC forms.
//
// FromString(raw) parses and normalizes the input; ToString() reconstructs it as
// scheme + authority + anchor + join(segments, sep) + trailing_sep
//
// Supported input forms and their parsed fields:
//
//   Input                         scheme        authority      anchor   segments      IsAbsolute
//   ----------------------------  ------------  -----------    -------  ------------  -----------
//   "a/b"                         ""            ""             ""       "a/b"         false
//   ""  (empty)                   ""            ""             ""       ""            false
//   "/"                           ""            ""             "/"      ""            true   * see below
//   "/a/b"                        ""            ""             "/"      "a/b"         true
//   "/a/b/"                       ""            ""             "/"      "a/b"         true   (HasTrailingSeparator)
//   "file:/a/b"                   "file:"       ""             "/"      "a/b"         true
//   "file:///a/b"                 "file://"     ""             "/"      "a/b"         true
//   "file://localhost/a/b"        "file://"     "localhost"    "/"      "a/b"         true
//   "s3://bucket/bar/baz"         "s3://"       "bucket"       "/"      "bar/baz"     true
//   "C:\foo" (win)                ""            ""             "C:\"    "foo"         true
//   "C:relpath" (win)             ""            ""             "C:"     "relpath"     false
//   "\\server\share\p" (win)      "\\"          "server\share" "\"      "p"           true
//   "\\?\UNC\server\share" (win)  "\\?\UNC\"    "server\share" "\"      ""            true
//   "\\?\C:\foo" (win)            "\\?\"        ""             "C:\"    "foo"         true
//
// *On empty Paths: Path("") == Path("."), Path("").IsDot() == true, and Path("").ToString == "."
//
// HasTrailingSeparator is true when raw input ends with a separator. ToString() re-emits the
// separator after the last segment (skipped when segments is empty — anchor already ends with it).
// When Join()ing, LHS inherits trailing_separator from RHS. This is useful (and semantically
// meaningful) in distinguishing e.g. '/foo/*/'=dirs from '/foo/*'=all.
//
// Windows local paths may use '/' or '\\' -- whichever is found first will be applied to
// remainder of the normalized path; defaults to '/'.
//
class Path {
public:
	enum class SchemeKind {
		None,     // no scheme — plain /foo, C:\foo, relative paths
		File,     // file: prefix — file:/foo, file:///foo, file://localhost/foo
		UNC,      // network UNC — \\server\share\... (plain UNC; \\?\UNC\ parses as Verbatim)
		URI,      // remote URI — s3://, az://, http://, etc.
		Verbatim, // \\?\ extended-length prefix (Windows) — collapses to None or UNC on ToCanonical()
	};

	static Path FromString(const string &raw);
	static Path CanonicalFromString(const string &raw); // FromString(raw).ToCanonical()

	Path() = default;
	explicit Path(const string &s) : Path(FromString(s)) {
	}

	string ToString() const;

	// 3 constituent parts of to string -- base + path + trailing separator (IFF path not empty)
	string GetBase() const;               // scheme + authority + anchor
	string GetPath() const;               // relative path segments: join(segments, sep), or "." for empty relative
	string GetTrailingSeparator() const { // returns "" or string(1, separator)
		return !segments.empty() && has_trailing_separator ? string(1, separator) : "";
	}
	const string &GetScheme() const {
		return scheme;
	}
	const string &GetAuthority() const {
		return authority;
	}
	const string &GetAnchor() const {
		return anchor;
	}
	char GetSeparator() const {
		return separator;
	}

	bool HasScheme() const {
		return !scheme.empty();
	}
	bool HasAuthority() const {
		return !authority.empty();
	}
	bool HasAnchor() const {
		return !anchor.empty();
	}

	bool HasDrive() const;     // true IFF GetDriveChar() != '\0' — only set by the Windows path parser
	char GetDriveChar() const; // returns \0 if no drive

	bool HasPathSegments() const {
		return !segments.empty();
	}

	bool HasTrailingSeparator() const {
		return has_trailing_separator;
	}

	// true for all network paths, /*,  c:/* (not c:foo), file:/*, \\?\C:\*
	bool IsAbsolute() const {
		return is_absolute;
	}

	bool IsRelative() const {
		return !is_absolute;
	}

	// True IFF this is the "current directory" relative path: no scheme, authority, anchor, or segments.
	// Both "" and "." normalize to this; Path("a/..") does too after normalization.
	// Path(".").Equals(Path()) == Path("").Equals(Path(".")) == true.
	bool IsDot() const {
		return !is_absolute && scheme.empty() && authority.empty() && anchor.empty() && segments.empty();
	}

	SchemeKind GetSchemeKind() const;

	bool IsNoneScheme() const {
		return GetSchemeKind() == SchemeKind::None;
	}
	bool IsFileScheme() const {
		return GetSchemeKind() == SchemeKind::File;
	}
	bool IsUNCScheme() const {
		return GetSchemeKind() == SchemeKind::UNC;
	}
	bool IsURIScheme() const {
		return GetSchemeKind() == SchemeKind::URI;
	}
	bool IsVerbatimScheme() const {
		return GetSchemeKind() == SchemeKind::Verbatim;
	}

	// Local: None (all bare paths), File scheme, or Verbatim with a drive letter (\\?\C:\).
	// Verbatim UNC (\\?\UNC\), plain UNC, and URI schemes are all remote.
	bool IsLocal() const {
		switch (GetSchemeKind()) {
		case SchemeKind::None:
			return true;
		case SchemeKind::File:
			return true;
		case SchemeKind::Verbatim:
			return HasDrive(); // \\?\C:\ is local; \\?\UNC\ is not
		default:
			return false;
		}
	}

	bool IsRemote() const {
		return !IsLocal();
	}

	const vector<string> &GetPathSegments() const {
		return segments;
	}

	// Join RHS onto this path:
	//   - relative RHS: appends to LHS (resolving any .. against LHS segments)
	//   - absolute RHS: returned as-is IFF RHS.IsRelativeTo(LHS) (i.e. RHS extends LHS within same root)
	//   - all others: incompatible, throws InvalidInputException
	Path Join(const Path &rhs) const;
	Path Join(const string &rhs) const;

	template <typename... Args>
	Path Join(const string &first, const Args &...rest) const {
		return Join(first).Join(rest...);
	}

	Path Join(const vector<string> &paths) const {
		Path result = *this;
		for (const auto &rhs : paths) {
			result = result.Join(rhs);
		}
		return result;
	}

	Path Parent(int n = 1) const; // apply ".." n times; n=0 returns self

	// True if `base` is a prefix of (or equal to) this path — same scheme/authority/anchor required.
	bool IsRelativeTo(const Path &base) const;
	// Returns the suffix of this path after stripping `base`; throws if !IsRelativeTo(base).
	// e.g. Path("/a/b/c").RelativeTo("/a") == "b/c";  Path("/a").RelativeTo("/a") == "."
	Path RelativeTo(const Path &base) const;

	// strip scheme/authority → bare notation (no scheme); throws if non-local. Relative passes through.
	Path ToBareLocal() const;
	// normalize to file:///... form; throws if non-local or relative
	Path ToFileURI() const;
	// Canonical form (throws if relative): collapses file: variants and \\?\ extended-length prefixes
	// to their simplest absolute form; remote paths are unchanged. Canonical implies absolute.
	Path ToCanonical() const;

	// operator== is intentionally omitted: normalization does not imply canonicalization, so naive
	// field equality yields surprises. "file:/foo", "file:///foo", and "/foo" are structurally
	// distinct after parsing but refer to the same path. Use one of:
	//   Equals()   — field-by-field, always case-sensitive; equivalent to ToString() == ToString()
	//   EqualsCI() — always case-insensitive throughout
	//   EqualsFS() — filesystem-aware: CI for Windows-shaped paths (bare on Windows, UNC, Verbatim,
	//                file: on Windows), case-sensitive otherwise (posix, URI, relative off-Windows)
	// bool operator==(const Path &) const; // intentionally omitted — see above
	bool Equals(const Path &other) const;
	bool EqualsCI(const Path &other) const;
	bool EqualsFS(const Path &other) const;

	// public convenience - string-to-string normalize
	static string Normalize(const string &input) {
		return FromString(input).ToString();
	}

	static string AddSuffixToPath(const string &path, const string &suffix);

private:
	string scheme;
	string authority;
	string anchor;
	vector<string> segments;

	char separator = '/';
	bool has_trailing_separator = false;
	bool is_absolute = false;

	// True when path should use case-insensitive comparison: UNC and Verbatim always; bare/file: on Windows;
	// off-platform, drive-letter bare paths (cross-platform correctness for recorded/replayed paths).
	bool IsLocalWindows() const;
	// std::equal with case sensitivity matching the path's filesystem
	bool SegmentsEqual(vector<string>::const_iterator a, vector<string>::const_iterator a_end,
	                   vector<string>::const_iterator b) const;

	void NormalizeSegments(const string &raw, size_t path_offset);

	static size_t ParseURIScheme(const string &input, Path &parsed);
	static size_t ParseFilePathTail(const string &input, size_t start, Path &parsed);
	static size_t ParseFileSchemes(const string &input, Path &parsed);
#if defined(_WIN32) || defined(DUCKDB_WIN32_COMPAT)
	static size_t ParseUNCScheme(const string &input, Path &parsed);
#endif
};

} // namespace duckdb
