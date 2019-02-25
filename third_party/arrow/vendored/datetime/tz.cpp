// The MIT License (MIT)
//
// Copyright (c) 2015, 2016, 2017 Howard Hinnant
// Copyright (c) 2015 Ville Voutilainen
// Copyright (c) 2016 Alexander Kormanovsky
// Copyright (c) 2016, 2017 Jiangang Zhuang
// Copyright (c) 2017 Nicolas Veloz Savino
// Copyright (c) 2017 Florian Dang
// Copyright (c) 2017 Aaron Bishop
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// Our apologies.  When the previous paragraph was written, lowercase had not yet
// been invented (that would involve another several millennia of evolution).
// We did not mean to shout.

// wesm: This is required so that symbols are properly exported from the DLL
#include "visibility.h"

#ifdef _WIN32
   // windows.h will be included directly and indirectly (e.g. by curl).
   // We need to define these macros to prevent windows.h bringing in
   // more than we need and do it early so windows.h doesn't get included
   // without these macros having been defined.
   // min/max macros interfere with the C++ versions.
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
   // We don't need all that Windows has to offer.
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif

   // for wcstombs
#  ifndef _CRT_SECURE_NO_WARNINGS
#    define _CRT_SECURE_NO_WARNINGS
#  endif

   // None of this happens with the MS SDK (at least VS14 which I tested), but:
   // Compiling with mingw, we get "error: 'KF_FLAG_DEFAULT' was not declared in this scope."
   // and error: 'SHGetKnownFolderPath' was not declared in this scope.".
   // It seems when using mingw NTDDI_VERSION is undefined and that
   // causes KNOWN_FOLDER_FLAG and the KF_ flags to not get defined.
   // So we must define NTDDI_VERSION to get those flags on mingw.
   // The docs say though here:
   // https://msdn.microsoft.com/en-nz/library/windows/desktop/aa383745(v=vs.85).aspx
   // that "If you define NTDDI_VERSION, you must also define _WIN32_WINNT."
   // So we declare we require Vista or greater.
#  ifdef __MINGW32__

#    ifndef NTDDI_VERSION
#      define NTDDI_VERSION 0x06000000
#      define _WIN32_WINNT _WIN32_WINNT_VISTA
#    elif NTDDI_VERSION < 0x06000000
#      warning "If this fails to compile NTDDI_VERSION may be to low. See comments above."
#    endif
     // But once we define the values above we then get this linker error:
     // "tz.cpp:(.rdata$.refptr.FOLDERID_Downloads[.refptr.FOLDERID_Downloads]+0x0): "
     //     "undefined reference to `FOLDERID_Downloads'"
     // which #include <initguid.h> cures see:
     // https://support.microsoft.com/en-us/kb/130869
#    include <initguid.h>
     // But with <initguid.h> included, the error moves on to:
     // error: 'FOLDERID_Downloads' was not declared in this scope
     // Which #include <knownfolders.h> cures.
#    include <knownfolders.h>

#  endif  // __MINGW32__

#  include <windows.h>
#endif  // _WIN32

#include "tz_private.h"

#ifdef __APPLE__
#  include "ios.h"
#else
#  define TARGET_OS_IPHONE 0
#endif

#if USE_OS_TZDB
#  include <dirent.h>
#endif
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#if USE_OS_TZDB
#  include <queue>
#endif
#include <sstream>
#include <string>
#include <tuple>
#include <vector>
#include <sys/stat.h>

// unistd.h is used on some platforms as part of the the means to get
// the current time zone. On Win32 windows.h provides a means to do it.
// gcc/mingw supports unistd.h on Win32 but MSVC does not.

#ifdef _WIN32
#  include <io.h> // _unlink etc.

#  if defined(__clang__)
    struct IUnknown;    // fix for issue with static_cast<> in objbase.h
                        //   (see https://github.com/philsquared/Catch/issues/690)
#  endif

#  include <shlobj.h> // CoTaskFree, ShGetKnownFolderPath etc.
#  if HAS_REMOTE_API
#    include <direct.h> // _mkdir
#    include <shellapi.h> // ShFileOperation etc.
#  endif  // HAS_REMOTE_API
#else   // !_WIN32
#  include <unistd.h>
#  if !USE_OS_TZDB
#    include <wordexp.h>
#  endif
#  include <limits.h>
#  include <string.h>
#  if !USE_SHELL_API
#    include <sys/stat.h>
#    include <sys/fcntl.h>
#    include <dirent.h>
#    include <cstring>
#    include <sys/wait.h>
#    include <sys/types.h>
#  endif //!USE_SHELL_API
#endif  // !_WIN32


#if HAS_REMOTE_API
   // Note curl includes windows.h so we must include curl AFTER definitions of things
   // that affect windows.h such as NOMINMAX.
#if defined(_MSC_VER) && defined(SHORTENED_CURL_INCLUDE)
   // For rmt_curl nuget package
#  include <curl.h>
#else
#  include <curl/curl.h>
#endif
#endif

#ifdef _WIN32
static CONSTDATA char folder_delimiter = '\\';
#else   // !_WIN32
static CONSTDATA char folder_delimiter = '/';
#endif  // !_WIN32

#if defined(__GNUC__) && __GNUC__ < 5
   // GCC 4.9 Bug 61489 Wrong warning with -Wmissing-field-initializers
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif  // defined(__GNUC__) && __GNUC__ < 5

#if !USE_OS_TZDB

#  ifdef _WIN32

namespace
{
    struct task_mem_deleter
    {
        void operator()(wchar_t buf[])
        {
            if (buf != nullptr)
                CoTaskMemFree(buf);
        }
    };
    using co_task_mem_ptr = std::unique_ptr<wchar_t[], task_mem_deleter>;
}

// We might need to know certain locations even if not using the remote API,
// so keep these routines out of that block for now.
static
std::string
get_known_folder(const GUID& folderid)
{
    std::string folder;
    PWSTR pfolder = nullptr;
    HRESULT hr = SHGetKnownFolderPath(folderid, KF_FLAG_DEFAULT, nullptr, &pfolder);
    if (SUCCEEDED(hr))
    {
        co_task_mem_ptr folder_ptr(pfolder);
        folder = std::string(folder_ptr.get(), folder_ptr.get() + wcslen(folder_ptr.get()));
    }
    return folder;
}

// Usually something like "c:\Users\username\Downloads".
static
std::string
get_download_folder()
{
    return get_known_folder(FOLDERID_Downloads);
}

#  else // !_WIN32

#    if !defined(INSTALL) || HAS_REMOTE_API

static
std::string
expand_path(std::string path)
{
#      if TARGET_OS_IPHONE
    return date::iOSUtils::get_tzdata_path();
#      else  // !TARGET_OS_IPHONE
    ::wordexp_t w{};
    std::unique_ptr<::wordexp_t, void(*)(::wordexp_t*)> hold{&w, ::wordfree};
    ::wordexp(path.c_str(), &w, 0);
    if (w.we_wordc != 1)
        throw std::runtime_error("Cannot expand path: " + path);
    path = w.we_wordv[0];
    return path;
#      endif  // !TARGET_OS_IPHONE
}

static
std::string
get_download_folder()
{
    return expand_path("~/Downloads");
}

#    endif // !defined(INSTALL) || HAS_REMOTE_API

#  endif  // !_WIN32

#endif  // !USE_OS_TZDB

namespace arrow
{
namespace util
{
namespace date
{
// +---------------------+
// | Begin Configuration |
// +---------------------+

using namespace detail;

#if !USE_OS_TZDB

static
std::string&
access_install()
{
    static std::string install
#ifndef INSTALL

    = get_download_folder() + folder_delimiter + "tzdata";

#else   // !INSTALL

#  define STRINGIZEIMP(x) #x
#  define STRINGIZE(x) STRINGIZEIMP(x)

    = STRINGIZE(INSTALL) + std::string(1, folder_delimiter) + "tzdata";

    #undef STRINGIZEIMP
    #undef STRINGIZE
#endif  // !INSTALL

    return install;
}

void
set_install(const std::string& s)
{
    access_install() = s;
}

static
const std::string&
get_install()
{
    static const std::string& ref = access_install();
    return ref;
}

#if HAS_REMOTE_API
static
std::string
get_download_gz_file(const std::string& version)
{
    auto file = get_install() + version + ".tar.gz";
    return file;
}
#endif  // HAS_REMOTE_API

#endif  // !USE_OS_TZDB

// These can be used to reduce the range of the database to save memory
CONSTDATA auto min_year = date::year::min();
CONSTDATA auto max_year = date::year::max();

CONSTDATA auto min_day = date::jan/1;
CONSTDATA auto max_day = date::dec/31;

#if USE_OS_TZDB

CONSTCD14 const sys_seconds min_seconds = sys_days(min_year/min_day);

#endif  // USE_OS_TZDB

#ifndef _WIN32

static
std::string
discover_tz_dir()
{
    struct stat sb;
    using namespace std;
#  ifndef __APPLE__
    CONSTDATA auto tz_dir_default = "/usr/share/zoneinfo";
    CONSTDATA auto tz_dir_buildroot = "/usr/share/zoneinfo/uclibc";

    // Check special path which is valid for buildroot with uclibc builds
    if(stat(tz_dir_buildroot, &sb) == 0 && S_ISDIR(sb.st_mode))
        return tz_dir_buildroot;
    else if(stat(tz_dir_default, &sb) == 0 && S_ISDIR(sb.st_mode))
        return tz_dir_default;
    else
        throw runtime_error("discover_tz_dir failed to find zoneinfo\n");
#  else  // __APPLE__
#      if TARGET_OS_IPHONE
    return "/var/db/timezone/zoneinfo";
#      else
    CONSTDATA auto timezone = "/etc/localtime";
    if (!(lstat(timezone, &sb) == 0 && S_ISLNK(sb.st_mode) && sb.st_size > 0))
        throw runtime_error("discover_tz_dir failed\n");
    string result;
    char rp[PATH_MAX+1] = {};
    if (readlink(timezone, rp, sizeof(rp)-1) > 0)
        result = string(rp);
    else
        throw system_error(errno, system_category(), "readlink() failed");
    auto i = result.find("zoneinfo");
    if (i == string::npos)
        throw runtime_error("discover_tz_dir failed to find zoneinfo\n");
    i = result.find('/', i);
    if (i == string::npos)
        throw runtime_error("discover_tz_dir failed to find '/'\n");
    return result.substr(0, i);
#      endif
#  endif  // __APPLE__
}

static
const std::string&
get_tz_dir()
{
    static const std::string tz_dir = discover_tz_dir();
    return tz_dir;
}

#endif

// +-------------------+
// | End Configuration |
// +-------------------+

namespace detail
{
struct undocumented {explicit undocumented() = default;};
}

#ifndef _MSC_VER
static_assert(min_year <= max_year, "Configuration error");
#endif

static std::unique_ptr<tzdb> init_tzdb();

tzdb_list::~tzdb_list()
{
    const tzdb* ptr = head_;
    head_ = nullptr;
    while (ptr != nullptr)
    {
        auto next = ptr->next;
        delete ptr;
        ptr = next;
    }
}

tzdb_list::tzdb_list(tzdb_list&& x) noexcept
   : head_{x.head_.exchange(nullptr)}
{
}

void
tzdb_list::push_front(tzdb* tzdb) noexcept
{
    tzdb->next = head_;
    head_ = tzdb;
}

tzdb_list::const_iterator
tzdb_list::erase_after(const_iterator p) noexcept
{
    auto t = p.p_->next;
    p.p_->next = p.p_->next->next;
    delete t;
    return ++p;
}

struct tzdb_list::undocumented_helper
{
    static void push_front(tzdb_list& db_list, tzdb* tzdb) noexcept
    {
        db_list.push_front(tzdb);
    }
};

static
tzdb_list
create_tzdb()
{
    tzdb_list tz_db;
    tzdb_list::undocumented_helper::push_front(tz_db, init_tzdb().release());
    return tz_db;
}

tzdb_list&
get_tzdb_list()
{
    static tzdb_list tz_db = create_tzdb();
    return tz_db;
}

#if !USE_OS_TZDB

#ifdef _WIN32

static
void
sort_zone_mappings(std::vector<date::detail::timezone_mapping>& mappings)
{
    std::sort(mappings.begin(), mappings.end(),
        [](const date::detail::timezone_mapping& lhs,
           const date::detail::timezone_mapping& rhs)->bool
    {
        auto other_result = lhs.other.compare(rhs.other);
        if (other_result < 0)
            return true;
        else if (other_result == 0)
        {
            auto territory_result = lhs.territory.compare(rhs.territory);
            if (territory_result < 0)
                return true;
            else if (territory_result == 0)
            {
                if (lhs.type < rhs.type)
                    return true;
            }
        }
        return false;
    });
}

static
bool
native_to_standard_timezone_name(const std::string& native_tz_name,
                                 std::string& standard_tz_name)
{
    // TOOD! Need be a case insensitive compare?
    if (native_tz_name == "UTC")
    {
        standard_tz_name = "Etc/UTC";
        return true;
    }
    standard_tz_name.clear();
    // TODO! we can improve on linear search.
    const auto& mappings = date::get_tzdb().mappings;
    for (const auto& tzm : mappings)
    {
        if (tzm.other == native_tz_name)
        {
            standard_tz_name = tzm.type;
            return true;
        }
    }
    return false;
}

// Parse this XML file:
// http://unicode.org/repos/cldr/trunk/common/supplemental/windowsZones.xml
// The parsing method is designed to be simple and quick. It is not overly
// forgiving of change but it should diagnose basic format issues.
// See timezone_mapping structure for more info.
static
std::vector<detail::timezone_mapping>
load_timezone_mappings_from_xml_file(const std::string& input_path)
{
    std::size_t line_num = 0;
    std::vector<detail::timezone_mapping> mappings;
    std::string line;

    std::ifstream is(input_path);
    if (!is.is_open())
    {
        // We don't emit file exceptions because that's an implementation detail.
        std::string msg = "Error opening time zone mapping file \"";
        msg += input_path;
        msg += "\".";
        throw std::runtime_error(msg);
    }

    auto error = [&input_path, &line_num](const char* info)
    {
        std::string msg = "Error loading time zone mapping file \"";
        msg += input_path;
        msg += "\" at line ";
        msg += std::to_string(line_num);
        msg += ": ";
        msg += info;
        throw std::runtime_error(msg);
    };
    // [optional space]a="b"
    auto read_attribute = [&line, &error]
                          (const char* name, std::string& value, std::size_t startPos)
                          ->std::size_t
    {
        value.clear();
        // Skip leading space before attribute name.
        std::size_t spos = line.find_first_not_of(' ', startPos);
        if (spos == std::string::npos)
            spos = startPos;
        // Assume everything up to next = is the attribute name
        // and that an = will always delimit that.
        std::size_t epos = line.find('=', spos);
        if (epos == std::string::npos)
            error("Expected \'=\' right after attribute name.");
        std::size_t name_len = epos - spos;
        // Expect the name we find matches the name we expect.
        if (line.compare(spos, name_len, name) != 0)
        {
            std::string msg;
            msg = "Expected attribute name \'";
            msg += name;
            msg += "\' around position ";
            msg += std::to_string(spos);
            msg += " but found something else.";
            error(msg.c_str());
        }
        ++epos; // Skip the '=' that is after the attribute name.
        spos = epos;
        if (spos < line.length() && line[spos] == '\"')
            ++spos; // Skip the quote that is before the attribute value.
        else
        {
            std::string msg = "Expected '\"' to begin value of attribute \'";
            msg += name;
            msg += "\'.";
            error(msg.c_str());
        }
        epos = line.find('\"', spos);
        if (epos == std::string::npos)
        {
            std::string msg = "Expected '\"' to end value of attribute \'";
            msg += name;
            msg += "\'.";
            error(msg.c_str());
        }
        // Extract everything in between the quotes. Note no escaping is done.
        std::size_t value_len = epos - spos;
        value.assign(line, spos, value_len);
        ++epos; // Skip the quote that is after the attribute value;
        return epos;
    };

    // Quick but not overly forgiving XML mapping file processing.
    bool mapTimezonesOpenTagFound = false;
    bool mapTimezonesCloseTagFound = false;
    std::size_t mapZonePos = std::string::npos;
    std::size_t mapTimezonesPos = std::string::npos;
    CONSTDATA char mapTimeZonesOpeningTag[] = { "<mapTimezones " };
    CONSTDATA char mapZoneOpeningTag[] = { "<mapZone " };
    CONSTDATA std::size_t mapZoneOpeningTagLen = sizeof(mapZoneOpeningTag) /
                                                 sizeof(mapZoneOpeningTag[0]) - 1;
    while (!mapTimezonesOpenTagFound)
    {
        std::getline(is, line);
        ++line_num;
        if (is.eof())
        {
            // If there is no mapTimezones tag is it an error?
            // Perhaps if there are no mapZone mappings it might be ok for
            // its parent mapTimezones element to be missing?
            // We treat this as an error though on the assumption that if there
            // really are no mappings we should still get a mapTimezones parent
            // element but no mapZone elements inside. Assuming we must
            // find something will hopefully at least catch more drastic formatting
            // changes or errors than if we don't do this and assume nothing found.
            error("Expected a mapTimezones opening tag.");
        }
        mapTimezonesPos = line.find(mapTimeZonesOpeningTag);
        mapTimezonesOpenTagFound = (mapTimezonesPos != std::string::npos);
    }

    // NOTE: We could extract the version info that follows the opening
    // mapTimezones tag and compare that to the version of other data we have.
    // I would have expected them to be kept in synch but testing has shown
    // it typically does not match anyway. So what's the point?
    while (!mapTimezonesCloseTagFound)
    {
        std::ws(is);
        std::getline(is, line);
        ++line_num;
        if (is.eof())
            error("Expected a mapTimezones closing tag.");
        if (line.empty())
            continue;
        mapZonePos = line.find(mapZoneOpeningTag);
        if (mapZonePos != std::string::npos)
        {
            mapZonePos += mapZoneOpeningTagLen;
            detail::timezone_mapping zm{};
            std::size_t pos = read_attribute("other", zm.other, mapZonePos);
            pos = read_attribute("territory", zm.territory, pos);
            read_attribute("type", zm.type, pos);
            mappings.push_back(std::move(zm));

            continue;
        }
        mapTimezonesPos = line.find("</mapTimezones>");
        mapTimezonesCloseTagFound = (mapTimezonesPos != std::string::npos);
        if (!mapTimezonesCloseTagFound)
        {
            std::size_t commentPos = line.find("<!--");
            if (commentPos == std::string::npos)
                error("Unexpected mapping record found. A xml mapZone or comment "
                      "attribute or mapTimezones closing tag was expected.");
        }
    }

    is.close();
    return mappings;
}

#endif  // _WIN32

// Parsing helpers

static
std::string
parse3(std::istream& in)
{
    std::string r(3, ' ');
    ws(in);
    r[0] = static_cast<char>(in.get());
    r[1] = static_cast<char>(in.get());
    r[2] = static_cast<char>(in.get());
    return r;
}

static
unsigned
parse_dow(std::istream& in)
{
    CONSTDATA char*const dow_names[] =
        {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    auto s = parse3(in);
    auto dow = std::find(std::begin(dow_names), std::end(dow_names), s) - dow_names;
    if (dow >= std::end(dow_names) - std::begin(dow_names))
        throw std::runtime_error("oops: bad dow name: " + s);
    return static_cast<unsigned>(dow);
}

static
unsigned
parse_month(std::istream& in)
{
    CONSTDATA char*const month_names[] =
        {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    auto s = parse3(in);
    auto m = std::find(std::begin(month_names), std::end(month_names), s) - month_names;
    if (m >= std::end(month_names) - std::begin(month_names))
        throw std::runtime_error("oops: bad month name: " + s);
    return static_cast<unsigned>(++m);
}

static
std::chrono::seconds
parse_unsigned_time(std::istream& in)
{
    using namespace std::chrono;
    int x;
    in >> x;
    auto r = seconds{hours{x}};
    if (!in.eof() && in.peek() == ':')
    {
        in.get();
        in >> x;
        r += minutes{x};
        if (!in.eof() && in.peek() == ':')
        {
            in.get();
            in >> x;
            r += seconds{x};
        }
    }
    return r;
}

static
std::chrono::seconds
parse_signed_time(std::istream& in)
{
    ws(in);
    auto sign = 1;
    if (in.peek() == '-')
    {
        sign = -1;
        in.get();
    }
    else if (in.peek() == '+')
        in.get();
    return sign * parse_unsigned_time(in);
}

// MonthDayTime

detail::MonthDayTime::MonthDayTime(local_seconds tp, tz timezone)
    : zone_(timezone)
{
    using namespace date;
    const auto dp = date::floor<days>(tp);
    const auto hms = make_time(tp - dp);
    const auto ymd = year_month_day(dp);
    u = ymd.month() / ymd.day();
    h_ = hms.hours();
    m_ = hms.minutes();
    s_ = hms.seconds();
}

detail::MonthDayTime::MonthDayTime(const date::month_day& md, tz timezone)
    : zone_(timezone)
{
    u = md;
}

date::day
detail::MonthDayTime::day() const
{
    switch (type_)
    {
    case month_day:
        return u.month_day_.day();
    case month_last_dow:
        return date::day{31};
    case lteq:
    case gteq:
        break;
    }
    return u.month_day_weekday_.month_day_.day();
}

date::month
detail::MonthDayTime::month() const
{
    switch (type_)
    {
    case month_day:
        return u.month_day_.month();
    case month_last_dow:
        return u.month_weekday_last_.month();
    case lteq:
    case gteq:
        break;
    }
    return u.month_day_weekday_.month_day_.month();
}

int
detail::MonthDayTime::compare(date::year y, const MonthDayTime& x, date::year yx,
                      std::chrono::seconds offset, std::chrono::minutes prev_save) const
{
    if (zone_ != x.zone_)
    {
        auto dp0 = to_sys_days(y);
        auto dp1 = x.to_sys_days(yx);
        if (std::abs((dp0-dp1).count()) > 1)
            return dp0 < dp1 ? -1 : 1;
        if (zone_ == tz::local)
        {
            auto tp0 = to_time_point(y) - prev_save;
            if (x.zone_ == tz::utc)
                tp0 -= offset;
            auto tp1 = x.to_time_point(yx);
            return tp0 < tp1 ? -1 : tp0 == tp1 ? 0 : 1;
        }
        else if (zone_ == tz::standard)
        {
            auto tp0 = to_time_point(y);
            auto tp1 = x.to_time_point(yx);
            if (x.zone_ == tz::local)
                tp1 -= prev_save;
            else
                tp0 -= offset;
            return tp0 < tp1 ? -1 : tp0 == tp1 ? 0 : 1;
        }
        // zone_ == tz::utc
        auto tp0 = to_time_point(y);
        auto tp1 = x.to_time_point(yx);
        if (x.zone_ == tz::local)
            tp1 -= offset + prev_save;
        else
            tp1 -= offset;
        return tp0 < tp1 ? -1 : tp0 == tp1 ? 0 : 1;
    }
    auto const t0 = to_time_point(y);
    auto const t1 = x.to_time_point(yx);
    return t0 < t1 ? -1 : t0 == t1 ? 0 : 1;
}

sys_seconds
detail::MonthDayTime::to_sys(date::year y, std::chrono::seconds offset,
                     std::chrono::seconds save) const
{
    using namespace date;
    using namespace std::chrono;
    auto until_utc = to_time_point(y);
    if (zone_ == tz::standard)
        until_utc -= offset;
    else if (zone_ == tz::local)
        until_utc -= offset + save;
    return until_utc;
}

detail::MonthDayTime::U&
detail::MonthDayTime::U::operator=(const date::month_day& x)
{
    month_day_ = x;
    return *this;
}

detail::MonthDayTime::U&
detail::MonthDayTime::U::operator=(const date::month_weekday_last& x)
{
    month_weekday_last_ = x;
    return *this;
}

detail::MonthDayTime::U&
detail::MonthDayTime::U::operator=(const pair& x)
{
    month_day_weekday_ = x;
    return *this;
}

date::sys_days
detail::MonthDayTime::to_sys_days(date::year y) const
{
    using namespace std::chrono;
    using namespace date;
    switch (type_)
    {
    case month_day:
        return sys_days(y/u.month_day_);
    case month_last_dow:
        return sys_days(y/u.month_weekday_last_);
    case lteq:
        {
            auto const x = y/u.month_day_weekday_.month_day_;
            auto const wd1 = weekday(static_cast<sys_days>(x));
            auto const wd0 = u.month_day_weekday_.weekday_;
            return sys_days(x) - (wd1-wd0);
        }
    case gteq:
        break;
    }
    auto const x = y/u.month_day_weekday_.month_day_;
    auto const wd1 = u.month_day_weekday_.weekday_;
    auto const wd0 = weekday(static_cast<sys_days>(x));
    return sys_days(x) + (wd1-wd0);
}

sys_seconds
detail::MonthDayTime::to_time_point(date::year y) const
{
    // Add seconds first to promote to largest rep early to prevent overflow
    return to_sys_days(y) + s_ + h_ + m_;
}

void
detail::MonthDayTime::canonicalize(date::year y)
{
    using namespace std::chrono;
    using namespace date;
    switch (type_)
    {
    case month_day:
        return;
    case month_last_dow:
        {
            auto const ymd = year_month_day(sys_days(y/u.month_weekday_last_));
            u.month_day_ = ymd.month()/ymd.day();
            type_ = month_day;
            return;
        }
    case lteq:
        {
            auto const x = y/u.month_day_weekday_.month_day_;
            auto const wd1 = weekday(static_cast<sys_days>(x));
            auto const wd0 = u.month_day_weekday_.weekday_;
            auto const ymd = year_month_day(sys_days(x) - (wd1-wd0));
            u.month_day_ = ymd.month()/ymd.day();
            type_ = month_day;
            return;
        }
    case gteq:
        {
            auto const x = y/u.month_day_weekday_.month_day_;
            auto const wd1 = u.month_day_weekday_.weekday_;
            auto const wd0 = weekday(static_cast<sys_days>(x));
            auto const ymd = year_month_day(sys_days(x) + (wd1-wd0));
            u.month_day_ = ymd.month()/ymd.day();
            type_ = month_day;
            return;
        }
    }
}

std::istream&
detail::operator>>(std::istream& is, MonthDayTime& x)
{
    using namespace date;
    using namespace std::chrono;
    assert(((std::ios::failbit | std::ios::badbit) & is.exceptions()) ==
            (std::ios::failbit | std::ios::badbit));
    x = MonthDayTime{};
    if (!is.eof() && ws(is) && !is.eof() && is.peek() != '#')
    {
        auto m = parse_month(is);
        if (!is.eof() && ws(is) && !is.eof() && is.peek() != '#')
        {
            if (is.peek() == 'l')
            {
                for (int i = 0; i < 4; ++i)
                    is.get();
                auto dow = parse_dow(is);
                x.type_ = MonthDayTime::month_last_dow;
                x.u = date::month(m)/weekday(dow)[last];
            }
            else if (std::isalpha(is.peek()))
            {
                auto dow = parse_dow(is);
                char c{};
                is >> c;
                if (c == '<' || c == '>')
                {
                    char c2{};
                    is >> c2;
                    if (c2 != '=')
                        throw std::runtime_error(std::string("bad operator: ") + c + c2);
                    int d;
                    is >> d;
                    if (d < 1 || d > 31)
                        throw std::runtime_error(std::string("bad operator: ") + c + c2
                                 + std::to_string(d));
                    x.type_ = c == '<' ? MonthDayTime::lteq : MonthDayTime::gteq;
                    x.u = MonthDayTime::pair{ date::month(m) / d, date::weekday(dow) };
                }
                else
                    throw std::runtime_error(std::string("bad operator: ") + c);
            }
            else  // if (std::isdigit(is.peek())
            {
                int d;
                is >> d;
                if (d < 1 || d > 31)
                    throw std::runtime_error(std::string("day of month: ")
                             + std::to_string(d));
                x.type_ = MonthDayTime::month_day;
                x.u = date::month(m)/d;
            }
            if (!is.eof() && ws(is) && !is.eof() && is.peek() != '#')
            {
                int t;
                is >> t;
                x.h_ = hours{t};
                if (!is.eof() && is.peek() == ':')
                {
                    is.get();
                    is >> t;
                    x.m_ = minutes{t};
                    if (!is.eof() && is.peek() == ':')
                    {
                        is.get();
                        is >> t;
                        x.s_ = seconds{t};
                    }
                }
                if (!is.eof() && std::isalpha(is.peek()))
                {
                    char c;
                    is >> c;
                    switch (c)
                    {
                    case 's':
                        x.zone_ = tz::standard;
                        break;
                    case 'u':
                        x.zone_ = tz::utc;
                        break;
                    }
                }
            }
        }
        else
        {
            x.u = month{m}/1;
        }
    }
    return is;
}

std::ostream&
detail::operator<<(std::ostream& os, const MonthDayTime& x)
{
    switch (x.type_)
    {
    case MonthDayTime::month_day:
        os << x.u.month_day_ << "                  ";
        break;
    case MonthDayTime::month_last_dow:
        os << x.u.month_weekday_last_ << "           ";
        break;
    case MonthDayTime::lteq:
        os << x.u.month_day_weekday_.weekday_ << " on or before "
           << x.u.month_day_weekday_.month_day_ << "  ";
        break;
    case MonthDayTime::gteq:
        if ((static_cast<unsigned>(x.day()) - 1) % 7 == 0)
        {
            os << (x.u.month_day_weekday_.month_day_.month() /
                   x.u.month_day_weekday_.weekday_[
                       (static_cast<unsigned>(x.day()) - 1)/7+1]) << "              ";
        }
        else
        {
            os << x.u.month_day_weekday_.weekday_ << " on or after "
               << x.u.month_day_weekday_.month_day_ << "  ";
        }
        break;
    }
    os << date::make_time(x.s_ + x.h_ + x.m_);
    if (x.zone_ == tz::utc)
        os << "UTC   ";
    else if (x.zone_ == tz::standard)
        os << "STD   ";
    else
        os << "      ";
    return os;
}

// Rule

detail::Rule::Rule(const std::string& s)
{
    try
    {
        using namespace date;
        using namespace std::chrono;
        std::istringstream in(s);
        in.exceptions(std::ios::failbit | std::ios::badbit);
        std::string word;
        in >> word >> name_;
        int x;
        ws(in);
        if (std::isalpha(in.peek()))
        {
            in >> word;
            if (word == "min")
            {
                starting_year_ = year::min();
            }
            else
                throw std::runtime_error("Didn't find expected word: " + word);
        }
        else
        {
            in >> x;
            starting_year_ = year{x};
        }
        std::ws(in);
        if (std::isalpha(in.peek()))
        {
            in >> word;
            if (word == "only")
            {
                ending_year_ = starting_year_;
            }
            else if (word == "max")
            {
                ending_year_ = year::max();
            }
            else
                throw std::runtime_error("Didn't find expected word: " + word);
        }
        else
        {
            in >> x;
            ending_year_ = year{x};
        }
        in >> word;  // TYPE (always "-")
        assert(word == "-");
        in >> starting_at_;
        save_ = duration_cast<minutes>(parse_signed_time(in));
        in >> abbrev_;
        if (abbrev_ == "-")
            abbrev_.clear();
        assert(hours{-1} <= save_ && save_ <= hours{2});
    }
    catch (...)
    {
        std::cerr << s << '\n';
        std::cerr << *this << '\n';
        throw;
    }
}

detail::Rule::Rule(const Rule& r, date::year starting_year, date::year ending_year)
    : name_(r.name_)
    , starting_year_(starting_year)
    , ending_year_(ending_year)
    , starting_at_(r.starting_at_)
    , save_(r.save_)
    , abbrev_(r.abbrev_)
{
}

bool
detail::operator==(const Rule& x, const Rule& y)
{
    if (std::tie(x.name_, x.save_, x.starting_year_, x.ending_year_) ==
        std::tie(y.name_, y.save_, y.starting_year_, y.ending_year_))
        return x.month() == y.month() && x.day() == y.day();
    return false;
}

bool
detail::operator<(const Rule& x, const Rule& y)
{
    using namespace std::chrono;
    auto const xm = x.month();
    auto const ym = y.month();
    if (std::tie(x.name_, x.starting_year_, xm, x.ending_year_) <
        std::tie(y.name_, y.starting_year_, ym, y.ending_year_))
        return true;
    if (std::tie(x.name_, x.starting_year_, xm, x.ending_year_) >
        std::tie(y.name_, y.starting_year_, ym, y.ending_year_))
        return false;
    return x.day() < y.day();
}

bool
detail::operator==(const Rule& x, const date::year& y)
{
    return x.starting_year_ <= y && y <= x.ending_year_;
}

bool
detail::operator<(const Rule& x, const date::year& y)
{
    return x.ending_year_ < y;
}

bool
detail::operator==(const date::year& x, const Rule& y)
{
    return y.starting_year_ <= x && x <= y.ending_year_;
}

bool
detail::operator<(const date::year& x, const Rule& y)
{
    return x < y.starting_year_;
}

bool
detail::operator==(const Rule& x, const std::string& y)
{
    return x.name() == y;
}

bool
detail::operator<(const Rule& x, const std::string& y)
{
    return x.name() < y;
}

bool
detail::operator==(const std::string& x, const Rule& y)
{
    return y.name() == x;
}

bool
detail::operator<(const std::string& x, const Rule& y)
{
    return x < y.name();
}

std::ostream&
detail::operator<<(std::ostream& os, const Rule& r)
{
    using namespace date;
    using namespace std::chrono;
    detail::save_stream<char> _(os);
    os.fill(' ');
    os.flags(std::ios::dec | std::ios::left);
    os.width(15);
    os << r.name_;
    os << r.starting_year_ << "    " << r.ending_year_ << "    ";
    os << r.starting_at_;
    if (r.save_ >= minutes{0})
        os << ' ';
    os << date::make_time(r.save_) << "   ";
    os << r.abbrev_;
    return os;
}

date::day
detail::Rule::day() const
{
    return starting_at_.day();
}

date::month
detail::Rule::month() const
{
    return starting_at_.month();
}

struct find_rule_by_name
{
    bool operator()(const Rule& x, const std::string& nm) const
    {
        return x.name() < nm;
    }

    bool operator()(const std::string& nm, const Rule& x) const
    {
        return nm < x.name();
    }
};

bool
detail::Rule::overlaps(const Rule& x, const Rule& y)
{
    // assume x.starting_year_ <= y.starting_year_;
    if (!(x.starting_year_ <= y.starting_year_))
    {
        std::cerr << x << '\n';
        std::cerr << y << '\n';
        assert(x.starting_year_ <= y.starting_year_);
    }
    if (y.starting_year_ > x.ending_year_)
        return false;
    return !(x.starting_year_ == y.starting_year_ && x.ending_year_ == y.ending_year_);
}

void
detail::Rule::split(std::vector<Rule>& rules, std::size_t i, std::size_t k, std::size_t& e)
{
    using namespace date;
    using difference_type = std::vector<Rule>::iterator::difference_type;
    // rules[i].starting_year_ <= rules[k].starting_year_ &&
    //     rules[i].ending_year_ >= rules[k].starting_year_ &&
    //     (rules[i].starting_year_ != rules[k].starting_year_ ||
    //      rules[i].ending_year_ != rules[k].ending_year_)
    assert(rules[i].starting_year_ <= rules[k].starting_year_ &&
           rules[i].ending_year_ >= rules[k].starting_year_ &&
           (rules[i].starting_year_ != rules[k].starting_year_ ||
            rules[i].ending_year_ != rules[k].ending_year_));
    if (rules[i].starting_year_ == rules[k].starting_year_)
    {
        if (rules[k].ending_year_ < rules[i].ending_year_)
        {
            rules.insert(rules.begin() + static_cast<difference_type>(k+1),
                         Rule(rules[i], rules[k].ending_year_ + years{1},
                              std::move(rules[i].ending_year_)));
            ++e;
            rules[i].ending_year_ = rules[k].ending_year_;
        }
        else  // rules[k].ending_year_ > rules[i].ending_year_
        {
            rules.insert(rules.begin() + static_cast<difference_type>(k+1),
                         Rule(rules[k], rules[i].ending_year_ + years{1},
                              std::move(rules[k].ending_year_)));
            ++e;
            rules[k].ending_year_ = rules[i].ending_year_;
        }
    }
    else  // rules[i].starting_year_ < rules[k].starting_year_
    {
        if (rules[k].ending_year_ < rules[i].ending_year_)
        {
            rules.insert(rules.begin() + static_cast<difference_type>(k),
                         Rule(rules[i], rules[k].starting_year_, rules[k].ending_year_));
            ++k;
            rules.insert(rules.begin() + static_cast<difference_type>(k+1),
                         Rule(rules[i], rules[k].ending_year_ + years{1},
                              std::move(rules[i].ending_year_)));
            rules[i].ending_year_ = rules[k].starting_year_ - years{1};
            e += 2;
        }
        else if (rules[k].ending_year_ > rules[i].ending_year_)
        {
            rules.insert(rules.begin() + static_cast<difference_type>(k),
                         Rule(rules[i], rules[k].starting_year_, rules[i].ending_year_));
            ++k;
            rules.insert(rules.begin() + static_cast<difference_type>(k+1),
                         Rule(rules[k], rules[i].ending_year_ + years{1},
                         std::move(rules[k].ending_year_)));
            e += 2;
            rules[k].ending_year_ = std::move(rules[i].ending_year_);
            rules[i].ending_year_ = rules[k].starting_year_ - years{1};
        }
        else  // rules[k].ending_year_ == rules[i].ending_year_
        {
            rules.insert(rules.begin() + static_cast<difference_type>(k),
                         Rule(rules[i], rules[k].starting_year_,
                         std::move(rules[i].ending_year_)));
            ++k;
            ++e;
            rules[i].ending_year_ = rules[k].starting_year_ - years{1};
        }
    }
}

void
detail::Rule::split_overlaps(std::vector<Rule>& rules, std::size_t i, std::size_t& e)
{
    using difference_type = std::vector<Rule>::iterator::difference_type;
    auto j = i;
    for (; i + 1 < e; ++i)
    {
        for (auto k = i + 1; k < e; ++k)
        {
            if (overlaps(rules[i], rules[k]))
            {
                split(rules, i, k, e);
                std::sort(rules.begin() + static_cast<difference_type>(i),
                          rules.begin() + static_cast<difference_type>(e));
            }
        }
    }
    for (; j < e; ++j)
    {
        if (rules[j].starting_year() == rules[j].ending_year())
            rules[j].starting_at_.canonicalize(rules[j].starting_year());
    }
}

void
detail::Rule::split_overlaps(std::vector<Rule>& rules)
{
    using difference_type = std::vector<Rule>::iterator::difference_type;
    for (std::size_t i = 0; i < rules.size();)
    {
        auto e = static_cast<std::size_t>(std::upper_bound(
            rules.cbegin()+static_cast<difference_type>(i), rules.cend(), rules[i].name(),
            [](const std::string& nm, const Rule& x)
            {
                return nm < x.name();
            }) - rules.cbegin());
        split_overlaps(rules, i, e);
        auto first_rule = rules.begin() + static_cast<difference_type>(i);
        auto last_rule = rules.begin() + static_cast<difference_type>(e);
        auto t = std::lower_bound(first_rule, last_rule, min_year);
        if (t > first_rule+1)
        {
            if (t == last_rule || t->starting_year() >= min_year)
                --t;
            auto d = static_cast<std::size_t>(t - first_rule);
            rules.erase(first_rule, t);
            e -= d;
        }
        first_rule = rules.begin() + static_cast<difference_type>(i);
        last_rule = rules.begin() + static_cast<difference_type>(e);
        t = std::upper_bound(first_rule, last_rule, max_year);
        if (t != last_rule)
        {
            auto d = static_cast<std::size_t>(last_rule - t);
            rules.erase(t, last_rule);
            e -= d;
        }
        i = e;
    }
    rules.shrink_to_fit();
}

// Find the rule that comes chronologically before Rule r.  For multi-year rules,
// y specifies which rules in r.  For single year rules, y is assumed to be equal
// to the year specified by r.
// Returns a pointer to the chronologically previous rule, and the year within
// that rule.  If there is no previous rule, returns nullptr and year::min().
// Preconditions:
//     r->starting_year() <= y && y <= r->ending_year()
static
std::pair<const Rule*, date::year>
find_previous_rule(const Rule* r, date::year y)
{
    using namespace date;
    auto const& rules = get_tzdb().rules;
    if (y == r->starting_year())
    {
        if (r == &rules.front() || r->name() != r[-1].name())
            std::terminate();  // never called with first rule
        --r;
        if (y == r->starting_year())
            return {r, y};
        return {r, r->ending_year()};
    }
    if (r == &rules.front() || r->name() != r[-1].name() ||
        r[-1].starting_year() < r->starting_year())
    {
        while (r < &rules.back() && r->name() == r[1].name() &&
               r->starting_year() == r[1].starting_year())
            ++r;
        return {r, --y};
    }
    --r;
    return {r, y};
}

// Find the rule that comes chronologically after Rule r.  For multi-year rules,
// y specifies which rules in r.  For single year rules, y is assumed to be equal
// to the year specified by r.
// Returns a pointer to the chronologically next rule, and the year within
// that rule.  If there is no next rule, return a pointer to a defaulted rule
// and y+1.
// Preconditions:
//     first <= r && r < last && r->starting_year() <= y && y <= r->ending_year()
//     [first, last) all have the same name
static
std::pair<const Rule*, date::year>
find_next_rule(const Rule* first_rule, const Rule* last_rule, const Rule* r, date::year y)
{
    using namespace date;
    if (y == r->ending_year())
    {
        if (r == last_rule-1)
            return {nullptr, year::max()};
        ++r;
        if (y == r->ending_year())
            return {r, y};
        return {r, r->starting_year()};
    }
    if (r == last_rule-1 || r->ending_year() < r[1].ending_year())
    {
        while (r > first_rule && r->starting_year() == r[-1].starting_year())
            --r;
        return {r, ++y};
    }
    ++r;
    return {r, y};
}

// Find the rule that comes chronologically after Rule r.  For multi-year rules,
// y specifies which rules in r.  For single year rules, y is assumed to be equal
// to the year specified by r.
// Returns a pointer to the chronologically next rule, and the year within
// that rule.  If there is no next rule, return nullptr and year::max().
// Preconditions:
//     r->starting_year() <= y && y <= r->ending_year()
static
std::pair<const Rule*, date::year>
find_next_rule(const Rule* r, date::year y)
{
    using namespace date;
    auto const& rules = get_tzdb().rules;
    if (y == r->ending_year())
    {
        if (r == &rules.back() || r->name() != r[1].name())
            return {nullptr, year::max()};
        ++r;
        if (y == r->ending_year())
            return {r, y};
        return {r, r->starting_year()};
    }
    if (r == &rules.back() || r->name() != r[1].name() ||
        r->ending_year() < r[1].ending_year())
    {
        while (r > &rules.front() && r->name() == r[-1].name() &&
               r->starting_year() == r[-1].starting_year())
            --r;
        return {r, ++y};
    }
    ++r;
    return {r, y};
}

static
const Rule*
find_first_std_rule(const std::pair<const Rule*, const Rule*>& eqr)
{
    auto r = eqr.first;
    auto ry = r->starting_year();
    while (r->save() != std::chrono::minutes{0})
    {
        std::tie(r, ry) = find_next_rule(eqr.first, eqr.second, r, ry);
        if (r == nullptr)
            throw std::runtime_error("Could not find standard offset in rule "
                                     + eqr.first->name());
    }
    return r;
}

static
std::pair<const Rule*, date::year>
find_rule_for_zone(const std::pair<const Rule*, const Rule*>& eqr,
                   const date::year& y, const std::chrono::seconds& offset,
                   const MonthDayTime& mdt)
{
    assert(eqr.first != nullptr);
    assert(eqr.second != nullptr);

    using namespace std::chrono;
    using namespace date;
    auto r = eqr.first;
    auto ry = r->starting_year();
    auto prev_save = minutes{0};
    auto prev_year = year::min();
    const Rule* prev_rule = nullptr;
    while (r != nullptr)
    {
        if (mdt.compare(y, r->mdt(), ry, offset, prev_save) <= 0)
            break;
        prev_rule = r;
        prev_year = ry;
        prev_save = prev_rule->save();
        std::tie(r, ry) = find_next_rule(eqr.first, eqr.second, r, ry);
    }
    return {prev_rule, prev_year};
}

static
std::pair<const Rule*, date::year>
find_rule_for_zone(const std::pair<const Rule*, const Rule*>& eqr,
                   const sys_seconds& tp_utc,
                   const local_seconds& tp_std,
                   const local_seconds& tp_loc)
{
    using namespace std::chrono;
    using namespace date;
    auto r = eqr.first;
    auto ry = r->starting_year();
    auto prev_save = minutes{0};
    auto prev_year = year::min();
    const Rule* prev_rule = nullptr;
    while (r != nullptr)
    {
        bool found = false;
        switch (r->mdt().zone())
        {
        case tz::utc:
            found = tp_utc < r->mdt().to_time_point(ry);
            break;
        case tz::standard:
            found = sys_seconds{tp_std.time_since_epoch()} < r->mdt().to_time_point(ry);
            break;
        case tz::local:
            found = sys_seconds{tp_loc.time_since_epoch()} < r->mdt().to_time_point(ry);
            break;
        }
        if (found)
            break;
        prev_rule = r;
        prev_year = ry;
        prev_save = prev_rule->save();
        std::tie(r, ry) = find_next_rule(eqr.first, eqr.second, r, ry);
    }
    return {prev_rule, prev_year};
}

static
sys_info
find_rule(const std::pair<const Rule*, date::year>& first_rule,
          const std::pair<const Rule*, date::year>& last_rule,
          const date::year& y, const std::chrono::seconds& offset,
          const MonthDayTime& mdt, const std::chrono::minutes& initial_save,
          const std::string& initial_abbrev)
{
    using namespace std::chrono;
    using namespace date;
    auto r = first_rule.first;
    auto ry = first_rule.second;
    sys_info x{sys_days(year::min()/min_day), sys_days(year::max()/max_day),
               seconds{0}, initial_save, initial_abbrev};
    while (r != nullptr)
    {
        auto tr = r->mdt().to_sys(ry, offset, x.save);
        auto tx = mdt.to_sys(y, offset, x.save);
        // Find last rule where tx >= tr
        if (tx <= tr || (r == last_rule.first && ry == last_rule.second))
        {
            if (tx < tr && r == first_rule.first && ry == first_rule.second)
            {
                x.end = r->mdt().to_sys(ry, offset, x.save);
                break;
            }
            if (tx < tr)
            {
                std::tie(r, ry) = find_previous_rule(r, ry);  // can't return nullptr for r
                assert(r != nullptr);
            }
            // r != nullptr && tx >= tr (if tr were to be recomputed)
            auto prev_save = initial_save;
            if (!(r == first_rule.first && ry == first_rule.second))
                prev_save = find_previous_rule(r, ry).first->save();
            x.begin = r->mdt().to_sys(ry, offset, prev_save);
            x.save = r->save();
            x.abbrev = r->abbrev();
            if (!(r == last_rule.first && ry == last_rule.second))
            {
                std::tie(r, ry) = find_next_rule(r, ry);  // can't return nullptr for r
                assert(r != nullptr);
                x.end = r->mdt().to_sys(ry, offset, x.save);
            }
            else
                x.end = sys_days(year::max()/max_day);
            break;
        }
        x.save = r->save();
        std::tie(r, ry) = find_next_rule(r, ry);  // Can't return nullptr for r
        assert(r != nullptr);
    }
    return x;
}

// zonelet

detail::zonelet::~zonelet()
{
#if !defined(_MSC_VER) || (_MSC_VER >= 1900)
    using minutes = std::chrono::minutes;
    using string = std::string;
    if (tag_ == has_save)
        u.save_.~minutes();
    else
        u.rule_.~string();
#endif
}

detail::zonelet::zonelet()
{
#if !defined(_MSC_VER) || (_MSC_VER >= 1900)
    ::new(&u.rule_) std::string();
#endif
}

detail::zonelet::zonelet(const zonelet& i)
    : gmtoff_(i.gmtoff_)
    , tag_(i.tag_)
    , format_(i.format_)
    , until_year_(i.until_year_)
    , until_date_(i.until_date_)
    , until_utc_(i.until_utc_)
    , until_std_(i.until_std_)
    , until_loc_(i.until_loc_)
    , initial_save_(i.initial_save_)
    , initial_abbrev_(i.initial_abbrev_)
    , first_rule_(i.first_rule_)
    , last_rule_(i.last_rule_)
{
#if !defined(_MSC_VER) || (_MSC_VER >= 1900)
    if (tag_ == has_save)
        ::new(&u.save_) std::chrono::minutes(i.u.save_);
    else
        ::new(&u.rule_) std::string(i.u.rule_);
#else
    if (tag_ == has_save)
        u.save_ = i.u.save_;
    else
        u.rule_ = i.u.rule_;
#endif
}

#endif  // !USE_OS_TZDB

// time_zone

#if USE_OS_TZDB

time_zone::time_zone(const std::string& s, detail::undocumented)
    : name_(s)
    , adjusted_(new std::once_flag{})
{
}

enum class endian
{
    native = __BYTE_ORDER__,
    little = __ORDER_LITTLE_ENDIAN__,
    big    = __ORDER_BIG_ENDIAN__
};

static
inline
std::uint32_t
reverse_bytes(std::uint32_t i)
{
    return
        (i & 0xff000000u) >> 24 |
        (i & 0x00ff0000u) >> 8 |
        (i & 0x0000ff00u) << 8 |
        (i & 0x000000ffu) << 24;
}

static
inline
std::uint64_t
reverse_bytes(std::uint64_t i)
{
    return
        (i & 0xff00000000000000ull) >> 56 |
        (i & 0x00ff000000000000ull) >> 40 |
        (i & 0x0000ff0000000000ull) >> 24 |
        (i & 0x000000ff00000000ull) >> 8 |
        (i & 0x00000000ff000000ull) << 8 |
        (i & 0x0000000000ff0000ull) << 24 |
        (i & 0x000000000000ff00ull) << 40 |
        (i & 0x00000000000000ffull) << 56;
}

template <class T>
static
inline
void
maybe_reverse_bytes(T&, std::false_type)
{
}

static
inline
void
maybe_reverse_bytes(std::int32_t& t, std::true_type)
{
    t = static_cast<std::int32_t>(reverse_bytes(static_cast<std::uint32_t>(t)));
}

static
inline
void
maybe_reverse_bytes(std::int64_t& t, std::true_type)
{
    t = static_cast<std::int64_t>(reverse_bytes(static_cast<std::uint64_t>(t)));
}

template <class T>
static
inline
void
maybe_reverse_bytes(T& t)
{
    maybe_reverse_bytes(t, std::integral_constant<bool,
                                                  endian::native == endian::little>{});
}

static
void
load_header(std::istream& inf)
{
    // Read TZif
    auto t = inf.get();
    auto z = inf.get();
    auto i = inf.get();
    auto f = inf.get();
#ifndef NDEBUG
    assert(t == 'T');
    assert(z == 'Z');
    assert(i == 'i');
    assert(f == 'f');
#else
    (void)t;
    (void)z;
    (void)i;
    (void)f;
#endif
}

static
unsigned char
load_version(std::istream& inf)
{
    // Read version
    auto v = inf.get();
    assert(v != EOF);
    return static_cast<unsigned char>(v);
}

static
void
skip_reserve(std::istream& inf)
{
    inf.ignore(15);
}

static
void
load_counts(std::istream& inf,
            std::int32_t& tzh_ttisgmtcnt, std::int32_t& tzh_ttisstdcnt,
            std::int32_t& tzh_leapcnt,    std::int32_t& tzh_timecnt,
            std::int32_t& tzh_typecnt,    std::int32_t& tzh_charcnt)
{
    // Read counts;
    inf.read(reinterpret_cast<char*>(&tzh_ttisgmtcnt), 4);
    maybe_reverse_bytes(tzh_ttisgmtcnt);
    inf.read(reinterpret_cast<char*>(&tzh_ttisstdcnt), 4);
    maybe_reverse_bytes(tzh_ttisstdcnt);
    inf.read(reinterpret_cast<char*>(&tzh_leapcnt), 4);
    maybe_reverse_bytes(tzh_leapcnt);
    inf.read(reinterpret_cast<char*>(&tzh_timecnt), 4);
    maybe_reverse_bytes(tzh_timecnt);
    inf.read(reinterpret_cast<char*>(&tzh_typecnt), 4);
    maybe_reverse_bytes(tzh_typecnt);
    inf.read(reinterpret_cast<char*>(&tzh_charcnt), 4);
    maybe_reverse_bytes(tzh_charcnt);
}

template <class TimeType>
static
std::vector<detail::transition>
load_transitions(std::istream& inf, std::int32_t tzh_timecnt)
{
    // Read transitions
    using namespace std::chrono;
    std::vector<detail::transition> transitions;
    transitions.reserve(static_cast<unsigned>(tzh_timecnt));
    for (std::int32_t i = 0; i < tzh_timecnt; ++i)
    {
        TimeType t;
        inf.read(reinterpret_cast<char*>(&t), sizeof(t));
        maybe_reverse_bytes(t);
        transitions.emplace_back(sys_seconds{seconds{t}});
        if (transitions.back().timepoint < min_seconds)
            transitions.back().timepoint = min_seconds;
    }
    return transitions;
}

static
std::vector<std::uint8_t>
load_indices(std::istream& inf, std::int32_t tzh_timecnt)
{
    // Read indices
    std::vector<std::uint8_t> indices;
    indices.reserve(static_cast<unsigned>(tzh_timecnt));
    for (std::int32_t i = 0; i < tzh_timecnt; ++i)
    {
        std::uint8_t t;
        inf.read(reinterpret_cast<char*>(&t), sizeof(t));
        indices.emplace_back(t);
    }
    return indices;
}

static
std::vector<ttinfo>
load_ttinfo(std::istream& inf, std::int32_t tzh_typecnt)
{
    // Read ttinfo
    std::vector<ttinfo> ttinfos;
    ttinfos.reserve(static_cast<unsigned>(tzh_typecnt));
    for (std::int32_t i = 0; i < tzh_typecnt; ++i)
    {
        ttinfo t;
        inf.read(reinterpret_cast<char*>(&t), 6);
        maybe_reverse_bytes(t.tt_gmtoff);
        ttinfos.emplace_back(t);
    }
    return ttinfos;
}

static
std::string
load_abbreviations(std::istream& inf, std::int32_t tzh_charcnt)
{
    // Read abbreviations
    std::string abbrev;
    abbrev.resize(static_cast<unsigned>(tzh_charcnt), '\0');
    inf.read(&abbrev[0], tzh_charcnt);
    return abbrev;
}

#if !MISSING_LEAP_SECONDS

template <class TimeType>
static
std::vector<leap>
load_leaps(std::istream& inf, std::int32_t tzh_leapcnt)
{
    // Read tzh_leapcnt pairs
    using namespace std::chrono;
    std::vector<leap> leap_seconds;
    leap_seconds.reserve(tzh_leapcnt);
    for (std::int32_t i = 0; i < tzh_leapcnt; ++i)
    {
        TimeType     t0;
        std::int32_t t1;
        inf.read(reinterpret_cast<char*>(&t0), sizeof(t0));
        inf.read(reinterpret_cast<char*>(&t1), sizeof(t1));
        maybe_reverse_bytes(t0);
        maybe_reverse_bytes(t1);
        leap_seconds.emplace_back(sys_seconds{seconds{t0 - (t1-1)}},
                                  detail::undocumented{});
    }
    return leap_seconds;
}

template <class TimeType>
static
std::vector<leap>
load_leap_data(std::istream& inf,
               std::int32_t tzh_leapcnt, std::int32_t tzh_timecnt,
               std::int32_t tzh_typecnt, std::int32_t tzh_charcnt)
{
    inf.ignore(tzh_timecnt*sizeof(TimeType) + tzh_timecnt + tzh_typecnt*6 + tzh_charcnt);
    return load_leaps<TimeType>(inf, tzh_leapcnt);
}

static
std::vector<leap>
load_just_leaps(std::istream& inf)
{
    // Read tzh_leapcnt pairs
    using namespace std::chrono;
    load_header(inf);
    auto v = load_version(inf);
    std::int32_t tzh_ttisgmtcnt, tzh_ttisstdcnt, tzh_leapcnt,
                 tzh_timecnt,    tzh_typecnt,    tzh_charcnt;
    skip_reserve(inf);
    load_counts(inf, tzh_ttisgmtcnt, tzh_ttisstdcnt, tzh_leapcnt,
                     tzh_timecnt,    tzh_typecnt,    tzh_charcnt);
    if (v == 0)
        return load_leap_data<int32_t>(inf, tzh_leapcnt, tzh_timecnt, tzh_typecnt,
                                       tzh_charcnt);
#if !defined(NDEBUG)
    inf.ignore((4+1)*tzh_timecnt + 6*tzh_typecnt + tzh_charcnt + 8*tzh_leapcnt +
               tzh_ttisstdcnt + tzh_ttisgmtcnt);
    load_header(inf);
    auto v2 = load_version(inf);
    assert(v == v2);
    skip_reserve(inf);
#else  // defined(NDEBUG)
    inf.ignore((4+1)*tzh_timecnt + 6*tzh_typecnt + tzh_charcnt + 8*tzh_leapcnt +
               tzh_ttisstdcnt + tzh_ttisgmtcnt + (4+1+15));
#endif  // defined(NDEBUG)
    load_counts(inf, tzh_ttisgmtcnt, tzh_ttisstdcnt, tzh_leapcnt,
                     tzh_timecnt,    tzh_typecnt,    tzh_charcnt);
    return load_leap_data<int64_t>(inf, tzh_leapcnt, tzh_timecnt, tzh_typecnt,
                                   tzh_charcnt);
}

#endif  // !MISSING_LEAP_SECONDS

template <class TimeType>
void
time_zone::load_data(std::istream& inf,
                     std::int32_t tzh_leapcnt, std::int32_t tzh_timecnt,
                     std::int32_t tzh_typecnt, std::int32_t tzh_charcnt)
{
    using namespace std::chrono;
    transitions_ = load_transitions<TimeType>(inf, tzh_timecnt);
    auto indices = load_indices(inf, tzh_timecnt);
    auto infos = load_ttinfo(inf, tzh_typecnt);
    auto abbrev = load_abbreviations(inf, tzh_charcnt);
#if !MISSING_LEAP_SECONDS
    auto& leap_seconds = get_tzdb_list().front().leaps;
    if (leap_seconds.empty() && tzh_leapcnt > 0)
        leap_seconds = load_leaps<TimeType>(inf, tzh_leapcnt);
#endif
    ttinfos_.reserve(infos.size());
    for (auto& info : infos)
    {
        ttinfos_.push_back({seconds{info.tt_gmtoff},
                            abbrev.c_str() + info.tt_abbrind,
                            info.tt_isdst != 0});
    }
    auto i = 0u;
    if (transitions_.empty() || transitions_.front().timepoint != min_seconds)
    {
        transitions_.emplace(transitions_.begin(), min_seconds);
        auto tf = std::find_if(ttinfos_.begin(), ttinfos_.end(),
                               [](const expanded_ttinfo& ti)
                                   {return ti.is_dst == 0;});
        if (tf == ttinfos_.end())
            tf = ttinfos_.begin();
        transitions_[i].info = &*tf;
        ++i;
    }
    for (auto j = 0u; i < transitions_.size(); ++i, ++j)
        transitions_[i].info = ttinfos_.data() + indices[j];
}

void
time_zone::init_impl()
{
    using namespace std;
    using namespace std::chrono;
    auto name = get_tz_dir() + ('/' + name_);
    std::ifstream inf(name);
    if (!inf.is_open())
        throw std::runtime_error{"Unable to open " + name};
    inf.exceptions(std::ios::failbit | std::ios::badbit);
    load_header(inf);
    auto v = load_version(inf);
    std::int32_t tzh_ttisgmtcnt, tzh_ttisstdcnt, tzh_leapcnt,
                 tzh_timecnt,    tzh_typecnt,    tzh_charcnt;
    skip_reserve(inf);
    load_counts(inf, tzh_ttisgmtcnt, tzh_ttisstdcnt, tzh_leapcnt,
                     tzh_timecnt,    tzh_typecnt,    tzh_charcnt);
    if (v == 0)
    {
        load_data<int32_t>(inf, tzh_leapcnt, tzh_timecnt, tzh_typecnt, tzh_charcnt);
    }
    else
    {
#if !defined(NDEBUG)
        inf.ignore((4+1)*tzh_timecnt + 6*tzh_typecnt + tzh_charcnt + 8*tzh_leapcnt +
                   tzh_ttisstdcnt + tzh_ttisgmtcnt);
        load_header(inf);
        auto v2 = load_version(inf);
        assert(v == v2);
        skip_reserve(inf);
#else  // defined(NDEBUG)
        inf.ignore((4+1)*tzh_timecnt + 6*tzh_typecnt + tzh_charcnt + 8*tzh_leapcnt +
                   tzh_ttisstdcnt + tzh_ttisgmtcnt + (4+1+15));
#endif  // defined(NDEBUG)
        load_counts(inf, tzh_ttisgmtcnt, tzh_ttisstdcnt, tzh_leapcnt,
                         tzh_timecnt,    tzh_typecnt,    tzh_charcnt);
        load_data<int64_t>(inf, tzh_leapcnt, tzh_timecnt, tzh_typecnt, tzh_charcnt);
    }
#if !MISSING_LEAP_SECONDS
    if (tzh_leapcnt > 0)
    {
        auto& leap_seconds = get_tzdb_list().front().leaps;
        auto itr = leap_seconds.begin();
        auto l = itr->date();
        seconds leap_count{0};
        for (auto t = std::upper_bound(transitions_.begin(), transitions_.end(), l,
                                       [](const sys_seconds& x, const transition& ct)
                                       {
                                           return x < ct.timepoint;
                                       });
                  t != transitions_.end(); ++t)
        {
            while (t->timepoint >= l)
            {
                ++leap_count;
                if (++itr == leap_seconds.end())
                    l = sys_days(max_year/max_day);
                else
                    l = itr->date() + leap_count;
            }
            t->timepoint -= leap_count;
        }
    }
#endif  // !MISSING_LEAP_SECONDS
    auto b = transitions_.begin();
    auto i = transitions_.end();
    if (i != b)
    {
        for (--i; i != b; --i)
        {
            if (i->info->offset == i[-1].info->offset &&
                i->info->abbrev == i[-1].info->abbrev &&
                i->info->is_dst == i[-1].info->is_dst)
                i = transitions_.erase(i);
        }
    }
}

void
time_zone::init() const
{
    std::call_once(*adjusted_, [this]() {const_cast<time_zone*>(this)->init_impl();});
}

sys_info
time_zone::load_sys_info(std::vector<detail::transition>::const_iterator i) const
{
    using namespace std::chrono;
    assert(!transitions_.empty());
    assert(i != transitions_.begin());
    sys_info r;
    r.begin = i[-1].timepoint;
    r.end = i != transitions_.end() ? i->timepoint :
                                      sys_seconds(sys_days(year::max()/max_day));
    r.offset = i[-1].info->offset;
    r.save = i[-1].info->is_dst ? minutes{1} : minutes{0};
    r.abbrev = i[-1].info->abbrev;
    return r;
}

sys_info
time_zone::get_info_impl(sys_seconds tp) const
{
    using namespace std;
    init();
    return load_sys_info(upper_bound(transitions_.begin(), transitions_.end(), tp,
                                     [](const sys_seconds& x, const transition& t)
                                     {
                                         return x < t.timepoint;
                                     }));
}

local_info
time_zone::get_info_impl(local_seconds tp) const
{
    using namespace std::chrono;
    init();
    local_info i;
    i.result = local_info::unique;
    auto tr = upper_bound(transitions_.begin(), transitions_.end(), tp,
                          [](const local_seconds& x, const transition& t)
                          {
                              return sys_seconds{x.time_since_epoch()} -
                                                         t.info->offset < t.timepoint;
                          });
    i.first = load_sys_info(tr);
    auto tps = sys_seconds{(tp - i.first.offset).time_since_epoch()};
    if (tps < i.first.begin + days{1} && tr != transitions_.begin())
    {
        i.second = load_sys_info(--tr);
        tps = sys_seconds{(tp - i.second.offset).time_since_epoch()};
        if (tps < i.second.end)
        {
           i.result = local_info::ambiguous;
           std::swap(i.first, i.second);
        }
        else
        {
            i.second = {};
        }
    }
    else if (tps >= i.first.end && tr != transitions_.end())
    {
        i.second = load_sys_info(++tr);
        tps = sys_seconds{(tp - i.second.offset).time_since_epoch()};
        if (tps < i.second.begin)
            i.result = local_info::nonexistent;
        else
            i.second = {};
    }
    return i;
}

std::ostream&
operator<<(std::ostream& os, const time_zone& z)
{
    using namespace std::chrono;
    z.init();
    os << z.name_ << '\n';
    os << "Initially:           ";
    auto const& t = z.transitions_.front();
    if (t.info->offset >= seconds{0})
        os << '+';
    os << make_time(t.info->offset);
    if (t.info->is_dst > 0)
        os << " daylight ";
    else
        os << " standard ";
    os << t.info->abbrev << '\n';
    for (auto i = std::next(z.transitions_.cbegin()); i < z.transitions_.cend(); ++i)
        os << *i << '\n';
    return os;
}

#if !MISSING_LEAP_SECONDS

leap::leap(const sys_seconds& s, detail::undocumented)
    : date_(s)
{
}

#endif  // !MISSING_LEAP_SECONDS

#else  // !USE_OS_TZDB

time_zone::time_zone(const std::string& s, detail::undocumented)
    : adjusted_(new std::once_flag{})
{
    try
    {
        using namespace date;
        std::istringstream in(s);
        in.exceptions(std::ios::failbit | std::ios::badbit);
        std::string word;
        in >> word >> name_;
        parse_info(in);
    }
    catch (...)
    {
        std::cerr << s << '\n';
        std::cerr << *this << '\n';
        zonelets_.pop_back();
        throw;
    }
}

sys_info
time_zone::get_info_impl(sys_seconds tp) const
{
    return get_info_impl(tp, static_cast<int>(tz::utc));
}

local_info
time_zone::get_info_impl(local_seconds tp) const
{
    using namespace std::chrono;
    local_info i{};
    i.first = get_info_impl(sys_seconds{tp.time_since_epoch()}, static_cast<int>(tz::local));
    auto tps = sys_seconds{(tp - i.first.offset).time_since_epoch()};
    if (tps < i.first.begin)
    {
        i.second = std::move(i.first);
        i.first = get_info_impl(i.second.begin - seconds{1}, static_cast<int>(tz::utc));
        i.result = local_info::nonexistent;
    }
    else if (i.first.end - tps <= days{1})
    {
        i.second = get_info_impl(i.first.end, static_cast<int>(tz::utc));
        tps = sys_seconds{(tp - i.second.offset).time_since_epoch()};
        if (tps >= i.second.begin)
            i.result = local_info::ambiguous;
        else
            i.second = {};
    }
    return i;
}

void
time_zone::add(const std::string& s)
{
    try
    {
        std::istringstream in(s);
        in.exceptions(std::ios::failbit | std::ios::badbit);
        ws(in);
        if (!in.eof() && in.peek() != '#')
            parse_info(in);
    }
    catch (...)
    {
        std::cerr << s << '\n';
        std::cerr << *this << '\n';
        zonelets_.pop_back();
        throw;
    }
}

void
time_zone::parse_info(std::istream& in)
{
    using namespace date;
    using namespace std::chrono;
    zonelets_.emplace_back();
    auto& zonelet = zonelets_.back();
    zonelet.gmtoff_ = parse_signed_time(in);
    in >> zonelet.u.rule_;
    if (zonelet.u.rule_ == "-")
        zonelet.u.rule_.clear();
    in >> zonelet.format_;
    if (!in.eof())
        ws(in);
    if (in.eof() || in.peek() == '#')
    {
        zonelet.until_year_ = year::max();
        zonelet.until_date_ = MonthDayTime(max_day, tz::utc);
    }
    else
    {
        int y;
        in >> y;
        zonelet.until_year_ = year{y};
        in >> zonelet.until_date_;
        zonelet.until_date_.canonicalize(zonelet.until_year_);
    }
    if ((zonelet.until_year_ < min_year) ||
            (zonelets_.size() > 1 && zonelets_.end()[-2].until_year_ > max_year))
        zonelets_.pop_back();
}

void
time_zone::adjust_infos(const std::vector<Rule>& rules)
{
    using namespace std::chrono;
    using namespace date;
    const zonelet* prev_zonelet = nullptr;
    for (auto& z : zonelets_)
    {
        std::pair<const Rule*, const Rule*> eqr{};
        std::istringstream in;
        in.exceptions(std::ios::failbit | std::ios::badbit);
        // Classify info as rule-based, has save, or neither
        if (!z.u.rule_.empty())
        {
            // Find out if this zonelet has a rule or a save
            eqr = std::equal_range(rules.data(), rules.data() + rules.size(), z.u.rule_);
            if (eqr.first == eqr.second)
            {
                // The rule doesn't exist.  Assume this is a save
                try
                {
                    using namespace std::chrono;
                    using string = std::string;
                    in.str(z.u.rule_);
                    auto tmp = duration_cast<minutes>(parse_signed_time(in));
#if !defined(_MSC_VER) || (_MSC_VER >= 1900)
                    z.u.rule_.~string();
                    z.tag_ = zonelet::has_save;
                    ::new(&z.u.save_) minutes(tmp);
#else
                    z.u.rule_.clear();
                    z.tag_ = zonelet::has_save;
                    z.u.save_ = tmp;
#endif
                }
                catch (...)
                {
                    std::cerr << name_ << " : " << z.u.rule_ << '\n';
                    throw;
                }
            }
        }
        else
        {
            // This zone::zonelet has no rule and no save
            z.tag_ = zonelet::is_empty;
        }

        minutes final_save{0};
        if (z.tag_ == zonelet::has_save)
        {
            final_save = z.u.save_;
        }
        else if (z.tag_ == zonelet::has_rule)
        {
            z.last_rule_ = find_rule_for_zone(eqr, z.until_year_, z.gmtoff_,
                                              z.until_date_);
            if (z.last_rule_.first != nullptr)
                final_save = z.last_rule_.first->save();
        }
        z.until_utc_ = z.until_date_.to_sys(z.until_year_, z.gmtoff_, final_save);
        z.until_std_ = local_seconds{z.until_utc_.time_since_epoch()} + z.gmtoff_;
        z.until_loc_ = z.until_std_ + final_save;

        if (z.tag_ == zonelet::has_rule)
        {
            if (prev_zonelet != nullptr)
            {
                z.first_rule_ = find_rule_for_zone(eqr, prev_zonelet->until_utc_,
                                                        prev_zonelet->until_std_,
                                                        prev_zonelet->until_loc_);
                if (z.first_rule_.first != nullptr)
                {
                    z.initial_save_ = z.first_rule_.first->save();
                    z.initial_abbrev_ = z.first_rule_.first->abbrev();
                    if (z.first_rule_ != z.last_rule_)
                    {
                        z.first_rule_ = find_next_rule(eqr.first, eqr.second,
                                                       z.first_rule_.first,
                                                       z.first_rule_.second);
                    }
                    else
                    {
                        z.first_rule_ = std::make_pair(nullptr, year::min());
                        z.last_rule_ = std::make_pair(nullptr, year::max());
                    }
                }
            }
            if (z.first_rule_.first == nullptr && z.last_rule_.first != nullptr)
            {
                z.first_rule_ = std::make_pair(eqr.first, eqr.first->starting_year());
                z.initial_abbrev_ = find_first_std_rule(eqr)->abbrev();
            }
        }

#ifndef NDEBUG
        if (z.first_rule_.first == nullptr)
        {
            assert(z.first_rule_.second == year::min());
            assert(z.last_rule_.first == nullptr);
            assert(z.last_rule_.second == year::max());
        }
        else
        {
            assert(z.last_rule_.first != nullptr);
        }
#endif
        prev_zonelet = &z;
    }
}

static
std::string
format_abbrev(std::string format, const std::string& variable, std::chrono::seconds off,
                                                               std::chrono::minutes save)
{
    using namespace std::chrono;
    auto k = format.find("%s");
    if (k != std::string::npos)
    {
        format.replace(k, 2, variable);
    }
    else
    {
        k = format.find('/');
        if (k != std::string::npos)
        {
            if (save == minutes{0})
                format.erase(k);
            else
                format.erase(0, k+1);
        }
        else
        {
            k = format.find("%z");
            if (k != std::string::npos)
            {
                std::string temp;
                if (off < seconds{0})
                {
                    temp = '-';
                    off = -off;
                }
                else
                    temp = '+';
                auto h = date::floor<hours>(off);
                off -= h;
                if (h < hours{10})
                    temp += '0';
                temp += std::to_string(h.count());
                if (off > seconds{0})
                {
                    auto m = date::floor<minutes>(off);
                    off -= m;
                    if (m < minutes{10})
                        temp += '0';
                    temp += std::to_string(m.count());
                    if (off > seconds{0})
                    {
                        if (off < seconds{10})
                            temp += '0';
                        temp += std::to_string(off.count());
                    }
                }
                format.replace(k, 2, temp);
            }
        }
    }
    return format;
}

sys_info
time_zone::get_info_impl(sys_seconds tp, int tz_int) const
{
    using namespace std::chrono;
    using namespace date;
    tz timezone = static_cast<tz>(tz_int);
    assert(timezone != tz::standard);
    auto y = year_month_day(floor<days>(tp)).year();
    if (y < min_year || y > max_year)
        throw std::runtime_error("The year " + std::to_string(static_cast<int>(y)) +
            " is out of range:[" + std::to_string(static_cast<int>(min_year)) + ", "
                                 + std::to_string(static_cast<int>(max_year)) + "]");
    std::call_once(*adjusted_,
                   [this]()
                   {
                       const_cast<time_zone*>(this)->adjust_infos(get_tzdb().rules);
                   });
    auto i = std::upper_bound(zonelets_.begin(), zonelets_.end(), tp,
        [timezone](sys_seconds t, const zonelet& zl)
        {
            return timezone == tz::utc ? t < zl.until_utc_ :
                                         t < sys_seconds{zl.until_loc_.time_since_epoch()};
        });

    sys_info r{};
    if (i != zonelets_.end())
    {
        if (i->tag_ == zonelet::has_save)
        {
            if (i != zonelets_.begin())
                r.begin = i[-1].until_utc_;
            else
                r.begin = sys_days(year::min()/min_day);
            r.end = i->until_utc_;
            r.offset = i->gmtoff_ + i->u.save_;
            r.save = i->u.save_;
        }
        else if (i->u.rule_.empty())
        {
            if (i != zonelets_.begin())
                r.begin = i[-1].until_utc_;
            else
                r.begin = sys_days(year::min()/min_day);
            r.end = i->until_utc_;
            r.offset = i->gmtoff_;
        }
        else
        {
            r = find_rule(i->first_rule_, i->last_rule_, y, i->gmtoff_,
                          MonthDayTime(local_seconds{tp.time_since_epoch()}, timezone),
                          i->initial_save_, i->initial_abbrev_);
            r.offset = i->gmtoff_ + r.save;
            if (i != zonelets_.begin() && r.begin < i[-1].until_utc_)
                r.begin = i[-1].until_utc_;
            if (r.end > i->until_utc_)
                r.end = i->until_utc_;
        }
        r.abbrev = format_abbrev(i->format_, r.abbrev, r.offset, r.save);
        assert(r.begin < r.end);
    }
    return r;
}

std::ostream&
operator<<(std::ostream& os, const time_zone& z)
{
    using namespace date;
    using namespace std::chrono;
    detail::save_stream<char> _(os);
    os.fill(' ');
    os.flags(std::ios::dec | std::ios::left);
    std::call_once(*z.adjusted_,
                   [&z]()
                   {
                       const_cast<time_zone&>(z).adjust_infos(get_tzdb().rules);
                   });
    os.width(35);
    os << z.name_;
    std::string indent;
    for (auto const& s : z.zonelets_)
    {
        os << indent;
        if (s.gmtoff_ >= seconds{0})
            os << ' ';
        os << make_time(s.gmtoff_) << "   ";
        os.width(15);
        if (s.tag_ != zonelet::has_save)
            os << s.u.rule_;
        else
        {
            std::ostringstream tmp;
            tmp << make_time(s.u.save_);
            os <<  tmp.str();
        }
        os.width(8);
        os << s.format_ << "   ";
        os << s.until_year_ << ' ' << s.until_date_;
        os << "   " << s.until_utc_ << " UTC";
        os << "   " << s.until_std_ << " STD";
        os << "   " << s.until_loc_;
        os << "   " << make_time(s.initial_save_);
        os << "   " << s.initial_abbrev_;
        if (s.first_rule_.first != nullptr)
            os << "   {" << *s.first_rule_.first << ", " << s.first_rule_.second << '}';
        else
            os << "   {" << "nullptr" << ", " << s.first_rule_.second << '}';
        if (s.last_rule_.first != nullptr)
            os << "   {" << *s.last_rule_.first << ", " << s.last_rule_.second << '}';
        else
            os << "   {" << "nullptr" << ", " << s.last_rule_.second << '}';
        os << '\n';
        if (indent.empty())
            indent = std::string(35, ' ');
    }
    return os;
}

#endif  // !USE_OS_TZDB

#if !MISSING_LEAP_SECONDS

std::ostream&
operator<<(std::ostream& os, const leap& x)
{
    using namespace date;
    return os << x.date_ << "  +";
}

#endif  // !MISSING_LEAP_SECONDS

#if USE_OS_TZDB

# ifdef __APPLE__
static
std::string
get_version()
{
    using namespace std;
    auto path = get_tz_dir() + string("/+VERSION");
    ifstream in{path};
    string version;
    in >> version;
    if (in.fail())
        throw std::runtime_error("Unable to get Timezone database version from " + path);
    return version;
}
# endif

static
std::unique_ptr<tzdb>
init_tzdb()
{
    std::unique_ptr<tzdb> db(new tzdb);

    //Iterate through folders
    std::queue<std::string> subfolders;
    subfolders.emplace(get_tz_dir());
    struct dirent* d;
    struct stat s;
    while (!subfolders.empty())
    {
        auto dirname = std::move(subfolders.front());
        subfolders.pop();
        auto dir = opendir(dirname.c_str());
        if (!dir)
            continue;
        while ((d = readdir(dir)) != nullptr)
        {
            // Ignore these files:
            if (d->d_name[0]                      == '.'    || // curdir, prevdir, hidden
                memcmp(d->d_name, "posix", 5)     == 0      || // starts with posix
                strcmp(d->d_name, "Factory")      == 0      ||
                strcmp(d->d_name, "iso3166.tab")  == 0      ||
                strcmp(d->d_name, "right")        == 0      ||
                strcmp(d->d_name, "+VERSION")     == 0      ||
                strcmp(d->d_name, "zone.tab")     == 0      ||
                strcmp(d->d_name, "zone1970.tab") == 0      ||
                strcmp(d->d_name, "leap-seconds.list") == 0   )
                continue;
            auto subname = dirname + folder_delimiter + d->d_name;
            if(stat(subname.c_str(), &s) == 0)
            {
                if(S_ISDIR(s.st_mode))
                {
                    if(!S_ISLNK(s.st_mode))
                    {
                        subfolders.push(subname);
                    }
                }
                else
                {
                    db->zones.emplace_back(subname.substr(get_tz_dir().size()+1),
                                           detail::undocumented{});
                }
            }
        }
        closedir(dir);
    }
    db->zones.shrink_to_fit();
    std::sort(db->zones.begin(), db->zones.end());
#  if !MISSING_LEAP_SECONDS
    std::ifstream in(get_tz_dir() + std::string(1, folder_delimiter) + "right/UTC",
                     std::ios_base::binary);
    if (in)
    {
        in.exceptions(std::ios::failbit | std::ios::badbit);
        db->leaps = load_just_leaps(in);
    }
    else
    {
        in.clear();
        in.open(get_tz_dir() + std::string(1, folder_delimiter) +
                "UTC", std::ios_base::binary);
        if (!in)
            throw std::runtime_error("Unable to extract leap second information");
        in.exceptions(std::ios::failbit | std::ios::badbit);
        db->leaps = load_just_leaps(in);
    }
#  endif  // !MISSING_LEAP_SECONDS
#  ifdef __APPLE__
    db->version = get_version();
#  endif
    return db;
}

#else  // !USE_OS_TZDB

// link

link::link(const std::string& s)
{
    using namespace date;
    std::istringstream in(s);
    in.exceptions(std::ios::failbit | std::ios::badbit);
    std::string word;
    in >> word >> target_ >> name_;
}

std::ostream&
operator<<(std::ostream& os, const link& x)
{
    using namespace date;
    detail::save_stream<char> _(os);
    os.fill(' ');
    os.flags(std::ios::dec | std::ios::left);
    os.width(35);
    return os << x.name_ << " --> " << x.target_;
}

// leap

leap::leap(const std::string& s, detail::undocumented)
{
    using namespace date;
    std::istringstream in(s);
    in.exceptions(std::ios::failbit | std::ios::badbit);
    std::string word;
    int y;
    MonthDayTime date;
    in >> word >> y >> date;
    date_ = date.to_time_point(year(y));
}

static
bool
file_exists(const std::string& filename)
{
#ifdef _WIN32
    return ::_access(filename.c_str(), 0) == 0;
#else
    return ::access(filename.c_str(), F_OK) == 0;
#endif
}

#if HAS_REMOTE_API

// CURL tools

static
int
curl_global()
{
    if (::curl_global_init(CURL_GLOBAL_DEFAULT) != 0)
        throw std::runtime_error("CURL global initialization failed");
    return 0;
}

namespace
{

struct curl_deleter
{
    void operator()(CURL* p) const
    {
        ::curl_easy_cleanup(p);
    }
};

}  // unnamed namespace

static
std::unique_ptr<CURL, curl_deleter>
curl_init()
{
    static const auto curl_is_now_initiailized = curl_global();
    (void)curl_is_now_initiailized;
    return std::unique_ptr<CURL, curl_deleter>{::curl_easy_init()};
}

static
bool
download_to_string(const std::string& url, std::string& str)
{
    str.clear();
    auto curl = curl_init();
    if (!curl)
        return false;
    std::string version;
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    curl_write_callback write_cb = [](char* contents, std::size_t size, std::size_t nmemb,
                                      void* userp) -> std::size_t
    {
        auto& userstr = *static_cast<std::string*>(userp);
        auto realsize = size * nmemb;
        userstr.append(contents, realsize);
        return realsize;
    };
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &str);
    curl_easy_setopt(curl.get(), CURLOPT_SSL_VERIFYPEER, false);
    auto res = curl_easy_perform(curl.get());
    return (res == CURLE_OK);
}

namespace
{
    enum class download_file_options { binary, text };
}

static
bool
download_to_file(const std::string& url, const std::string& local_filename,
                 download_file_options opts)
{
    auto curl = curl_init();
    if (!curl)
        return false;
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_SSL_VERIFYPEER, false);
    curl_write_callback write_cb = [](char* contents, std::size_t size, std::size_t nmemb,
                                      void* userp) -> std::size_t
    {
        auto& of = *static_cast<std::ofstream*>(userp);
        auto realsize = size * nmemb;
        of.write(contents, static_cast<std::streamsize>(realsize));
        return realsize;
    };
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, write_cb);
    decltype(curl_easy_perform(curl.get())) res;
    {
        std::ofstream of(local_filename,
                         opts == download_file_options::binary ?
                             std::ofstream::out | std::ofstream::binary :
                             std::ofstream::out);
        of.exceptions(std::ios::badbit);
        curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &of);
        res = curl_easy_perform(curl.get());
    }
    return res == CURLE_OK;
}

std::string
remote_version()
{
    std::string version;
    std::string str;
    if (download_to_string("https://www.iana.org/time-zones", str))
    {
        CONSTDATA char db[] = "/time-zones/releases/tzdata";
        CONSTDATA auto db_size = sizeof(db) - 1;
        auto p = str.find(db, 0, db_size);
        const int ver_str_len = 5;
        if (p != std::string::npos && p + (db_size + ver_str_len) <= str.size())
            version = str.substr(p + db_size, ver_str_len);
    }
    return version;
}


// TODO! Using system() create a process and a console window.
// This is useful to see what errors may occur but is slow and distracting.
// Consider implementing this functionality more directly, such as
// using _mkdir and CreateProcess etc.
// But use the current means now as matches Unix implementations and while
// in proof of concept / testing phase.
// TODO! Use <filesystem> eventually.
static
bool
remove_folder_and_subfolders(const std::string& folder)
{
#  ifdef _WIN32
#    if USE_SHELL_API
    // Delete the folder contents by deleting the folder.
    std::string cmd = "rd /s /q \"";
    cmd += folder;
    cmd += '\"';
    return std::system(cmd.c_str()) == EXIT_SUCCESS;
#    else  // !USE_SHELL_API
    // Create a buffer containing the path to delete. It must be terminated
    // by two nuls. Who designs these API's...
    std::vector<char> from;
    from.assign(folder.begin(), folder.end());
    from.push_back('\0');
    from.push_back('\0');
    SHFILEOPSTRUCT fo{}; // Zero initialize.
    fo.wFunc = FO_DELETE;
    fo.pFrom = from.data();
    fo.fFlags = FOF_NO_UI;
    int ret = SHFileOperation(&fo);
    if (ret == 0 && !fo.fAnyOperationsAborted)
        return true;
    return false;
#    endif  // !USE_SHELL_API
#  else   // !_WIN32
#    if USE_SHELL_API
    return std::system(("rm -R " + folder).c_str()) == EXIT_SUCCESS;
#    else // !USE_SHELL_API
    struct dir_deleter {
        dir_deleter() {}
        void operator()(DIR* d) const
        {
            if (d != nullptr)
            {
                int result = closedir(d);
                assert(result == 0);
            }
        }
    };
    using closedir_ptr = std::unique_ptr<DIR, dir_deleter>;

    std::string filename;
    struct stat statbuf;
    std::size_t folder_len = folder.length();
    struct dirent* p = nullptr;

    closedir_ptr d(opendir(folder.c_str()));
    bool r = d.get() != nullptr;
    while (r && (p=readdir(d.get())) != nullptr)
    {
        if (strcmp(p->d_name, ".") == 0 || strcmp(p->d_name, "..") == 0)
           continue;

        // + 2 for path delimiter and nul terminator.
        std::size_t buf_len = folder_len + strlen(p->d_name) + 2;
        filename.resize(buf_len);
        std::size_t path_len = static_cast<std::size_t>(
            snprintf(&filename[0], buf_len, "%s/%s", folder.c_str(), p->d_name));
        assert(path_len == buf_len - 1);
        filename.resize(path_len);

        if (stat(filename.c_str(), &statbuf) == 0)
            r = S_ISDIR(statbuf.st_mode)
              ? remove_folder_and_subfolders(filename)
              : unlink(filename.c_str()) == 0;
    }
    d.reset();

    if (r)
        r = rmdir(folder.c_str()) == 0;

    return r;
#    endif // !USE_SHELL_API
#  endif  // !_WIN32
}

static
bool
make_directory(const std::string& folder)
{
#  ifdef _WIN32
#    if USE_SHELL_API
    // Re-create the folder.
    std::string cmd = "mkdir \"";
    cmd += folder;
    cmd += '\"';
    return std::system(cmd.c_str()) == EXIT_SUCCESS;
#    else  // !USE_SHELL_API
    return _mkdir(folder.c_str()) == 0;
#    endif // !USE_SHELL_API
#  else  // !_WIN32
#    if USE_SHELL_API
    return std::system(("mkdir " + folder).c_str()) == EXIT_SUCCESS;
#    else  // !USE_SHELL_API
    return mkdir(folder.c_str(), 0777) == 0;
#    endif  // !USE_SHELL_API
#  endif  // !_WIN32
}

static
bool
delete_file(const std::string& file)
{
#  ifdef _WIN32
#    if USE_SHELL_API
    std::string cmd = "del \"";
    cmd += file;
    cmd += '\"';
    return std::system(cmd.c_str()) == 0;
#    else  // !USE_SHELL_API
    return _unlink(file.c_str()) == 0;
#    endif // !USE_SHELL_API
#  else  // !_WIN32
#    if USE_SHELL_API
    return std::system(("rm " + file).c_str()) == EXIT_SUCCESS;
#    else // !USE_SHELL_API
    return unlink(file.c_str()) == 0;
#    endif // !USE_SHELL_API
#  endif  // !_WIN32
}

#  ifdef _WIN32

static
bool
move_file(const std::string& from, const std::string& to)
{
#    if USE_SHELL_API
    std::string cmd = "move \"";
    cmd += from;
    cmd += "\" \"";
    cmd += to;
    cmd += '\"';
    return std::system(cmd.c_str()) == EXIT_SUCCESS;
#    else  // !USE_SHELL_API
    return !!::MoveFile(from.c_str(), to.c_str());
#    endif // !USE_SHELL_API
}

// Usually something like "c:\Program Files".
static
std::string
get_program_folder()
{
    return get_known_folder(FOLDERID_ProgramFiles);
}

// Note folder can and usually does contain spaces.
static
std::string
get_unzip_program()
{
    std::string path;

    // 7-Zip appears to note its location in the registry.
    // If that doesn't work, fall through and take a guess, but it will likely be wrong.
    HKEY hKey = nullptr;
    if (RegOpenKeyExA(HKEY_LOCAL_MACHINE, "SOFTWARE\\7-Zip", 0, KEY_READ, &hKey) == ERROR_SUCCESS)
    {
        char value_buffer[MAX_PATH + 1]; // fyi 260 at time of writing.
        // in/out parameter. Documentation say that size is a count of bytes not chars.
        DWORD size = sizeof(value_buffer) - sizeof(value_buffer[0]);
        DWORD tzi_type = REG_SZ;
        // Testing shows Path key value is "C:\Program Files\7-Zip\" i.e. always with trailing \.
        bool got_value = (RegQueryValueExA(hKey, "Path", nullptr, &tzi_type,
            reinterpret_cast<LPBYTE>(value_buffer), &size) == ERROR_SUCCESS);
        RegCloseKey(hKey); // Close now incase of throw later.
        if (got_value)
        {
            // Function does not guarantee to null terminate.
            value_buffer[size / sizeof(value_buffer[0])] = '\0';
            path = value_buffer;
            if (!path.empty())
            {
                path += "7z.exe";
                return path;
            }
        }
    }
    path += get_program_folder();
    path += folder_delimiter;
    path += "7-Zip\\7z.exe";
    return path;
}

#    if !USE_SHELL_API
static
int
run_program(const std::string& command)
{
    STARTUPINFO si{};
    si.cb = sizeof(si);
    PROCESS_INFORMATION pi{};

    // Allegedly CreateProcess overwrites the command line. Ugh.
    std::string mutable_command(command);
    if (CreateProcess(nullptr, &mutable_command[0],
        nullptr, nullptr, FALSE, CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi))
    {
        WaitForSingleObject(pi.hProcess, INFINITE);
        DWORD exit_code;
        bool got_exit_code = !!GetExitCodeProcess(pi.hProcess, &exit_code);
        CloseHandle(pi.hProcess);
        CloseHandle(pi.hThread);
        // Not 100% sure about this still active thing is correct,
        // but I'm going with it because I *think* WaitForSingleObject might
        // return in some cases without INFINITE-ly waiting.
        // But why/wouldn't GetExitCodeProcess return false in that case?
        if (got_exit_code && exit_code != STILL_ACTIVE)
            return static_cast<int>(exit_code);
    }
    return EXIT_FAILURE;
}
#    endif // !USE_SHELL_API

static
std::string
get_download_tar_file(const std::string& version)
{
    auto file = get_install();
    file += folder_delimiter;
    file += "tzdata";
    file += version;
    file += ".tar";
    return file;
}

static
bool
extract_gz_file(const std::string& version, const std::string& gz_file,
                const std::string& dest_folder)
{
    auto unzip_prog = get_unzip_program();
    bool unzip_result = false;
    // Use the unzip program to extract the tar file from the archive.

    // Aim to create a string like:
    // "C:\Program Files\7-Zip\7z.exe" x "C:\Users\SomeUser\Downloads\tzdata2016d.tar.gz"
    //     -o"C:\Users\SomeUser\Downloads\tzdata"
    std::string cmd;
    cmd = '\"';
    cmd += unzip_prog;
    cmd += "\" x \"";
    cmd += gz_file;
    cmd += "\" -o\"";
    cmd += dest_folder;
    cmd += '\"';

#    if USE_SHELL_API
    // When using shelling out with std::system() extra quotes are required around the
    // whole command. It's weird but necessary it seems, see:
    // http://stackoverflow.com/q/27975969/576911

    cmd = "\"" + cmd + "\"";
    if (std::system(cmd.c_str()) == EXIT_SUCCESS)
        unzip_result = true;
#    else  // !USE_SHELL_API
    if (run_program(cmd) == EXIT_SUCCESS)
        unzip_result = true;
#    endif // !USE_SHELL_API
    if (unzip_result)
        delete_file(gz_file);

    // Use the unzip program extract the data from the tar file that was
    // just extracted from the archive.
    auto tar_file = get_download_tar_file(version);
    cmd = '\"';
    cmd += unzip_prog;
    cmd += "\" x \"";
    cmd += tar_file;
    cmd += "\" -o\"";
    cmd += get_install();
    cmd += '\"';
#    if USE_SHELL_API
    cmd = "\"" + cmd + "\"";
    if (std::system(cmd.c_str()) == EXIT_SUCCESS)
        unzip_result = true;
#    else  // !USE_SHELL_API
    if (run_program(cmd) == EXIT_SUCCESS)
        unzip_result = true;
#    endif // !USE_SHELL_API

    if (unzip_result)
        delete_file(tar_file);

    return unzip_result;
}

static
std::string
get_download_mapping_file(const std::string& version)
{
    auto file = get_install() + version + "windowsZones.xml";
    return file;
}

#  else  // !_WIN32

#    if !USE_SHELL_API
static
int
run_program(const char* prog, const char*const args[])
{
    pid_t pid = fork();
    if (pid == -1) // Child failed to start.
        return EXIT_FAILURE;

    if (pid != 0)
    {
        // We are in the parent. Child started. Wait for it.
        pid_t ret;
        int status;
        while ((ret = waitpid(pid, &status, 0)) == -1)
        {
            if (errno != EINTR)
                break;
        }
        if (ret != -1)
        {
            if (WIFEXITED(status))
                return WEXITSTATUS(status);
        }
        printf("Child issues!\n");

        return EXIT_FAILURE; // Not sure what status of child is.
    }
    else // We are in the child process. Start the program the parent wants to run.
    {

        if (execv(prog, const_cast<char**>(args)) == -1) // Does not return.
        {
            perror("unreachable 0\n");
            _Exit(127);
        }
        printf("unreachable 2\n");
    }
    printf("unreachable 2\n");
    // Unreachable.
    assert(false);
    exit(EXIT_FAILURE);
    return EXIT_FAILURE;
}
#    endif // !USE_SHELL_API

static
bool
extract_gz_file(const std::string&, const std::string& gz_file, const std::string&)
{
#    if USE_SHELL_API
    bool unzipped = std::system(("tar -xzf " + gz_file + " -C " + get_install()).c_str()) == EXIT_SUCCESS;
#    else  // !USE_SHELL_API
    const char prog[] = {"/usr/bin/tar"};
    const char*const args[] =
    {
        prog, "-xzf", gz_file.c_str(), "-C", get_install().c_str(), nullptr
    };
    bool unzipped = (run_program(prog, args) == EXIT_SUCCESS);
#    endif // !USE_SHELL_API
    if (unzipped)
    {
        delete_file(gz_file);
        return true;
    }
    return false;
}

#  endif // !_WIN32

bool
remote_download(const std::string& version)
{
    assert(!version.empty());

#  ifdef _WIN32
    // Download folder should be always available for Windows
#  else  // !_WIN32
    // Create download folder if it does not exist on UNIX system
    auto download_folder = get_download_folder();
    if (!file_exists(download_folder))
    {
        make_directory(download_folder);
    }
#  endif  // _WIN32

    auto url = "https://data.iana.org/time-zones/releases/tzdata" + version +
               ".tar.gz";
    bool result = download_to_file(url, get_download_gz_file(version),
                                   download_file_options::binary);
#  ifdef _WIN32
    if (result)
    {
        auto mapping_file = get_download_mapping_file(version);
        result = download_to_file("http://unicode.org/repos/cldr/trunk/common/"
                                  "supplemental/windowsZones.xml",
            mapping_file, download_file_options::text);
    }
#  endif  // _WIN32
    return result;
}

bool
remote_install(const std::string& version)
{
    auto success = false;
    assert(!version.empty());

    std::string install = get_install();
    auto gz_file = get_download_gz_file(version);
    if (file_exists(gz_file))
    {
        if (file_exists(install))
            remove_folder_and_subfolders(install);
        if (make_directory(install))
        {
            if (extract_gz_file(version, gz_file, install))
                success = true;
#  ifdef _WIN32
            auto mapping_file_source = get_download_mapping_file(version);
            auto mapping_file_dest = get_install();
            mapping_file_dest += folder_delimiter;
            mapping_file_dest += "windowsZones.xml";
            if (!move_file(mapping_file_source, mapping_file_dest))
                success = false;
#  endif  // _WIN32
        }
    }
    return success;
}

#endif  // HAS_REMOTE_API

static
std::string
get_version(const std::string& path)
{
    std::string version;
    std::ifstream infile(path + "version");
    if (infile.is_open())
    {
        infile >> version;
        if (!infile.fail())
            return version;
    }
    else
    {
        infile.open(path + "NEWS");
        while (infile)
        {
            infile >> version;
            if (version == "Release")
            {
                infile >> version;
                return version;
            }
        }
    }
    throw std::runtime_error("Unable to get Timezone database version from " + path);
}

static
std::unique_ptr<tzdb>
init_tzdb()
{
    using namespace date;
    const std::string install = get_install();
    const std::string path = install + folder_delimiter;
    std::string line;
    bool continue_zone = false;
    std::unique_ptr<tzdb> db(new tzdb);

#if AUTO_DOWNLOAD
    if (!file_exists(install))
    {
        auto rv = remote_version();
        if (!rv.empty() && remote_download(rv))
        {
            if (!remote_install(rv))
            {
                std::string msg = "Timezone database version \"";
                msg += rv;
                msg += "\" did not install correctly to \"";
                msg += install;
                msg += "\"";
                throw std::runtime_error(msg);
            }
        }
        if (!file_exists(install))
        {
            std::string msg = "Timezone database not found at \"";
            msg += install;
            msg += "\"";
            throw std::runtime_error(msg);
        }
        db->version = get_version(path);
    }
    else
    {
        db->version = get_version(path);
        auto rv = remote_version();
        if (!rv.empty() && db->version != rv)
        {
            if (remote_download(rv))
            {
                remote_install(rv);
                db->version = get_version(path);
            }
        }
    }
#else  // !AUTO_DOWNLOAD
    if (!file_exists(install))
    {
        std::string msg = "Timezone database not found at \"";
        msg += install;
        msg += "\"";
        throw std::runtime_error(msg);
    }
    db->version = get_version(path);
#endif  // !AUTO_DOWNLOAD

    CONSTDATA char*const files[] =
    {
        "africa", "antarctica", "asia", "australasia", "backward", "etcetera", "europe",
        "pacificnew", "northamerica", "southamerica", "systemv", "leapseconds"
    };

    for (const auto& filename : files)
    {
        std::ifstream infile(path + filename);
        while (infile)
        {
            std::getline(infile, line);
            if (!line.empty() && line[0] != '#')
            {
                std::istringstream in(line);
                std::string word;
                in >> word;
                if (word == "Rule")
                {
                    db->rules.push_back(Rule(line));
                    continue_zone = false;
                }
                else if (word == "Link")
                {
                    db->links.push_back(link(line));
                    continue_zone = false;
                }
                else if (word == "Leap")
                {
                    db->leaps.push_back(leap(line, detail::undocumented{}));
                    continue_zone = false;
                }
                else if (word == "Zone")
                {
                    db->zones.push_back(time_zone(line, detail::undocumented{}));
                    continue_zone = true;
                }
                else if (line[0] == '\t' && continue_zone)
                {
                    db->zones.back().add(line);
                }
                else
                {
                    std::cerr << line << '\n';
                }
            }
        }
    }
    std::sort(db->rules.begin(), db->rules.end());
    Rule::split_overlaps(db->rules);
    std::sort(db->zones.begin(), db->zones.end());
    db->zones.shrink_to_fit();
    std::sort(db->links.begin(), db->links.end());
    db->links.shrink_to_fit();
    std::sort(db->leaps.begin(), db->leaps.end());
    db->leaps.shrink_to_fit();

#ifdef _WIN32
    std::string mapping_file = get_install() + folder_delimiter + "windowsZones.xml";
    db->mappings = load_timezone_mappings_from_xml_file(mapping_file);
    sort_zone_mappings(db->mappings);
#endif // _WIN32

    return db;
}

const tzdb&
reload_tzdb()
{
#if AUTO_DOWNLOAD
    auto const& v = get_tzdb_list().front().version;
    if (!v.empty() && v == remote_version())
        return get_tzdb_list().front();
#endif  // AUTO_DOWNLOAD
    tzdb_list::undocumented_helper::push_front(get_tzdb_list(), init_tzdb().release());
    return get_tzdb_list().front();
}

#endif  // !USE_OS_TZDB

const tzdb&
get_tzdb()
{
    return get_tzdb_list().front();
}

const time_zone*
#if HAS_STRING_VIEW
tzdb::locate_zone(std::string_view tz_name) const
#else
tzdb::locate_zone(const std::string& tz_name) const
#endif
{
    auto zi = std::lower_bound(zones.begin(), zones.end(), tz_name,
#if HAS_STRING_VIEW
        [](const time_zone& z, const std::string_view& nm)
#else
        [](const time_zone& z, const std::string& nm)
#endif
        {
            return z.name() < nm;
        });
    if (zi == zones.end() || zi->name() != tz_name)
    {
#if !USE_OS_TZDB
        auto li = std::lower_bound(links.begin(), links.end(), tz_name,
#if HAS_STRING_VIEW
        [](const link& z, const std::string_view& nm)
#else
        [](const link& z, const std::string& nm)
#endif
        {
            return z.name() < nm;
        });
        if (li != links.end() && li->name() == tz_name)
        {
            zi = std::lower_bound(zones.begin(), zones.end(), li->target(),
                [](const time_zone& z, const std::string& nm)
                {
                    return z.name() < nm;
                });
            if (zi != zones.end() && zi->name() == li->target())
                return &*zi;
        }
#endif  // !USE_OS_TZDB
        throw std::runtime_error(std::string(tz_name) + " not found in timezone database");
    }
    return &*zi;
}

const time_zone*
#if HAS_STRING_VIEW
locate_zone(std::string_view tz_name)
#else
locate_zone(const std::string& tz_name)
#endif
{
    return get_tzdb().locate_zone(tz_name);
}

#if USE_OS_TZDB

std::ostream&
operator<<(std::ostream& os, const tzdb& db)
{
    os << "Version: " << db.version << "\n\n";
    for (const auto& x : db.zones)
        os << x << '\n';
#if !MISSING_LEAP_SECONDS
    os << '\n';
    for (const auto& x : db.leaps)
        os << x << '\n';
#endif  // !MISSING_LEAP_SECONDS
    return os;
}

#else  // !USE_OS_TZDB

std::ostream&
operator<<(std::ostream& os, const tzdb& db)
{
    os << "Version: " << db.version << '\n';
    std::string title("--------------------------------------------"
                      "--------------------------------------------\n"
                      "Name           ""Start Y ""End Y   "
                      "Beginning                              ""Offset  "
                      "Designator\n"
                      "--------------------------------------------"
                      "--------------------------------------------\n");
    int count = 0;
    for (const auto& x : db.rules)
    {
        if (count++ % 50 == 0)
            os << title;
        os << x << '\n';
    }
    os << '\n';
    title = std::string("---------------------------------------------------------"
                        "--------------------------------------------------------\n"
                        "Name                               ""Offset      "
                        "Rule           ""Abrev      ""Until\n"
                        "---------------------------------------------------------"
                        "--------------------------------------------------------\n");
    count = 0;
    for (const auto& x : db.zones)
    {
        if (count++ % 10 == 0)
            os << title;
        os << x << '\n';
    }
    os << '\n';
    title = std::string("---------------------------------------------------------"
                        "--------------------------------------------------------\n"
                        "Alias                                   ""To\n"
                        "---------------------------------------------------------"
                        "--------------------------------------------------------\n");
    count = 0;
    for (const auto& x : db.links)
    {
        if (count++ % 45 == 0)
            os << title;
        os << x << '\n';
    }
    os << '\n';
    title = std::string("---------------------------------------------------------"
                        "--------------------------------------------------------\n"
                        "Leap second on\n"
                        "---------------------------------------------------------"
                        "--------------------------------------------------------\n");
    os << title;
    for (const auto& x : db.leaps)
        os << x << '\n';
    return os;
}

#endif  // !USE_OS_TZDB

// -----------------------

#ifdef _WIN32

static
std::string
getTimeZoneKeyName()
{
    DYNAMIC_TIME_ZONE_INFORMATION dtzi{};
    auto result = GetDynamicTimeZoneInformation(&dtzi);
    if (result == TIME_ZONE_ID_INVALID)
        throw std::runtime_error("current_zone(): GetDynamicTimeZoneInformation()"
                                 " reported TIME_ZONE_ID_INVALID.");
    auto wlen = wcslen(dtzi.TimeZoneKeyName);
    char buf[128] = {};
    assert(sizeof(buf) >= wlen+1);
    wcstombs(buf, dtzi.TimeZoneKeyName, wlen);
    if (strcmp(buf, "Coordinated Universal Time") == 0)
        return "UTC";
    return buf;
}

const time_zone*
tzdb::current_zone() const
{
    std::string win_tzid = getTimeZoneKeyName();
    std::string standard_tzid;
    if (!native_to_standard_timezone_name(win_tzid, standard_tzid))
    {
        std::string msg;
        msg = "current_zone() failed: A mapping from the Windows Time Zone id \"";
        msg += win_tzid;
        msg += "\" was not found in the time zone mapping database.";
        throw std::runtime_error(msg);
    }
    return locate_zone(standard_tzid);
}

#else  // !_WIN32

const time_zone*
tzdb::current_zone() const
{
    // On some OS's a file called /etc/localtime may
    // exist and it may be either a real file
    // containing time zone details or a symlink to such a file.
    // On MacOS and BSD Unix if this file is a symlink it
    // might resolve to a path like this:
    // "/usr/share/zoneinfo/America/Los_Angeles"
    // If it does, we try to determine the current
    // timezone from the remainder of the path by removing the prefix
    // and hoping the rest resolves to a valid timezone.
    // It may not always work though. If it doesn't then an
    // exception will be thrown by local_timezone.
    // The path may also take a relative form:
    // "../usr/share/zoneinfo/America/Los_Angeles".
    {
        struct stat sb;
        CONSTDATA auto timezone = "/etc/localtime";
        if (lstat(timezone, &sb) == 0 && S_ISLNK(sb.st_mode) && sb.st_size > 0) {
            using namespace std;
            string result;
            char rp[PATH_MAX+1] = {};
            if (readlink(timezone, rp, sizeof(rp)-1) > 0)
                result = string(rp);
            else
                throw system_error(errno, system_category(), "readlink() failed");

            const size_t pos = result.find(get_tz_dir());
            if (pos != result.npos)
                result.erase(0, get_tz_dir().size() + 1 + pos);
            return locate_zone(result);
        }
    }
    // On embedded systems e.g. buildroot with uclibc the timezone is linked
    // into /etc/TZ which is a symlink to path like this:
    // "/usr/share/zoneinfo/uclibc/America/Los_Angeles"
    // If it does, we try to determine the current
    // timezone from the remainder of the path by removing the prefix
    // and hoping the rest resolves to valid timezone.
    // It may not always work though. If it doesn't then an
    // exception will be thrown by local_timezone.
    // The path may also take a relative form:
    // "../usr/share/zoneinfo/uclibc/America/Los_Angeles".
    {
        struct stat sb;
        CONSTDATA auto timezone = "/etc/TZ";
        if (lstat(timezone, &sb) == 0 && S_ISLNK(sb.st_mode) && sb.st_size > 0) {
            using namespace std;
            string result;
            char rp[PATH_MAX+1] = {};
            if (readlink(timezone, rp, sizeof(rp)-1) > 0)
                result = string(rp);
            else
                throw system_error(errno, system_category(), "readlink() failed");

            const size_t pos = result.find(get_tz_dir());
            if (pos != result.npos)
                result.erase(0, get_tz_dir().size() + 1 + pos);
            return locate_zone(result);
        }
    }
    {
    // On some versions of some linux distro's (e.g. Ubuntu),
    // the current timezone might be in the first line of
    // the /etc/timezone file.
        std::ifstream timezone_file("/etc/timezone");
        if (timezone_file.is_open())
        {
            std::string result;
            std::getline(timezone_file, result);
            if (!result.empty())
                return locate_zone(result);
        }
        // Fall through to try other means.
    }
    {
    // On some versions of some bsd distro's (e.g. FreeBSD),
    // the current timezone might be in the first line of
    // the /var/db/zoneinfo file.
        std::ifstream timezone_file("/var/db/zoneinfo");
        if (timezone_file.is_open())
        {
            std::string result;
            std::getline(timezone_file, result);
            if (!result.empty())
                return locate_zone(result);
        }
        // Fall through to try other means.
    }
    {
    // On some versions of some bsd distro's (e.g. iOS),
    // it is not possible to use file based approach,
    // we switch to system API, calling functions in
    // CoreFoundation framework.
#if TARGET_OS_IPHONE
        std::string result = date::iOSUtils::get_current_timezone();
        if (!result.empty())
            return locate_zone(result);
#endif
    // Fall through to try other means.
    }
    {
    // On some versions of some linux distro's (e.g. Red Hat),
    // the current timezone might be in the first line of
    // the /etc/sysconfig/clock file as:
    // ZONE="US/Eastern"
        std::ifstream timezone_file("/etc/sysconfig/clock");
        std::string result;
        while (timezone_file)
        {
            std::getline(timezone_file, result);
            auto p = result.find("ZONE=\"");
            if (p != std::string::npos)
            {
                result.erase(p, p+6);
                result.erase(result.rfind('"'));
                return locate_zone(result);
            }
        }
        // Fall through to try other means.
    }
    throw std::runtime_error("Could not get current timezone");
}

#endif  // !_WIN32

const time_zone*
current_zone()
{
    return get_tzdb().current_zone();
}

}  // namespace date
}  // namespace util
}  // namespace arrow

#if defined(__GNUC__) && __GNUC__ < 5
# pragma GCC diagnostic pop
#endif
