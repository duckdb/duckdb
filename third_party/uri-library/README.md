# URIs for Modern C++ #
A header-only URI library in modern C++. Just plug and play!

## URI support ##
The driving reason for this library is the general lack of URI-parsing libraries
that are generic to all forms of URIs, and the amount of bad URI parsing in the
world. To address both of those cases, this library provides access to the
content component (called `hier-part` in the spec, even for non-hierarchical
URIs) for non-hierarchical URIs, and provides access to the parsed components of
the content component (username, password, host, port, path) for hierarchical
URIs. Note that accessing hierarchical URI components for a non-hierarchical URI
is invalid and will throw a `std::domain_error`, while accessing the content
component of a hierarchical URI is invalid and will likewise throw a
`std::domain_error`. For additional notes, see the following API documentation.

## URI Library API ##

### Constructors ###
* `uri(char const *uri_text, scheme_category category =
  scheme_category::Hierarchical, query_argument_separator separator =
  query_argument_separator::ampersand)` and `uri(std::string const &uri_text,
  scheme_category category = scheme_category::Hierarchical,
  query_argument_separator separator = query_argument_separator::ampersand)`:
  constructs a `uri` object and throws an exception for any invalid component.
* `uri(uri const &other)` and `uri &operator=(uri const &other)`: copy
  constructor and copy assignment operator. Creates a duplicate of the supplied
  uri.
* `uri(uri const &other, std::map<component, std::string> const &replacements)`:
  Constructs a new URI with the supplied URI, with components replaced as per
  the replacements dictionary. This constructor cannot change if the path is
  rooted or not, and it cannot change from a hierarchical URI to a
  non-hierarchical URI.
* `uri(std::map<component, std::string> const &components, scheme_category
  category, bool rooted_path, query_argument_separator separator =
  query_argument_separator::ampersand)`: Constructs a new URI from the
  components given. Note that currently it is possible to build very invalid
  URIs with this setup, as no validation is performed (as of right now.) This
  constructor is a wee bit experimental.

### Accessors ###
* `std::string const &get_scheme() const`: get the scheme component.
* `scheme_category get_scheme_category() const`: get the scheme category, either
  Hierarchical or NonHierarchical.
* `std::string const &get_content() const`: get the content component of a
  non-hierarchical URI. Throws when called on a hierarchical URI.
* `std::string const &get_username() const`: get the username component of the
  URI (or return an empty string if none was present in the source string.) This
  method will most likely be marked deprecated shortly (as username/password
  handling in URIs is deprecated.) Throws when called on a non-hierarchical
  URI.
* `std::string const &get_password() const`: get the password component of the
  URI (or return an empty string if none was present in the source string.) This
  method will most likely be marked deprecated shortly (as username/password
  handling in URIs is deprecated.) Throws when called on a non-hierarchical
  URI.
* `std::string const &get_host() const`: get the host component of the
  URI. Returns an empty string if the host component was empty or not
  supplied. Throws when called on a non-hierarchical URI.
* `unsigned long get_port() const`: get the port component (parsed into an
  `unsigned long`) of the URI. If no port was supplied, returns 0. Throws when
  called on a non-hierarchical URI.
* `std::string const &get_path() const`: get the path component of the
  URI. Returns an empty string if the path component was empty. Throws when
  called on a non-hierarchical URI.
* `std::string const &get_query() const`: get the query component of the URI, as
  a string. Returns an empty string if no query was supplied.
* `std::map<std::string, std::string> const &get_query_dictionary() const`: get
  the parsed contents of the query component, as a key-value dictionary. This
  operation uses the separator declared at the creation of the URI, which as a
  default uses an ampersand. If your URI uses semicolons to separate arguments,
  set the optional arguments in the constructor.
* `std::string const &get_fragment() const`: get the fragment component of the
  URI. Returns an empty string if no fragment was supplied.
* `std::string to_string() const`: get the normalized form of the URI; any
  empty components included in the initial URI string will be stripped from this
  form. Currently does not normalize on capitalization, but do not rely on the
  case of the returned URI matching the supplied case in the future. If no
  authority component was present in a hierarchical URI, this method will
  preserve the rootless state of the path component, i.e. for the URN
  `urn:ietf:rfc:2141`, the path is `ietf:rfc:2141` with no root (initial `/`
  character) and as such, the normalized string form also will not have a root
  character.

## Building tests ##
This library comes with a basic set of tests, which (as of this writing) mostly
confirm that a few example URIs work well, and should confirm the operation of
the library for various cases.

### ... with GCC or Clang++ ###
With GCC or Clang++, the instructions are fairly straightforward; run the
following in this directory, substituting `clang++` for `g++` (assuming your
installation has C++11 support):

    g++ -std=c++11 test.cc -o uri_test
    ./uri_test

### ... with MSVC 2015 or newer ###
With MSVC, note that I have only tested with 2015, and I expect later versions
will be similar. Using the developer command prompt, navigate to this directory
and run the following:

    cl.exe test.cc
    .\test.exe

## Current issues ##
The map-based instantiation is very weak currently, as it does absolutely no
validation. Similarly, the IPv6 parsing is only on structure, it doesn't
validate that the address is a valid format. That's up for consideration for
future work. Even though the documentation currently states that this library
normalizes URIs, it does not presently normalize the case of a URI, and since
the path handling is generic, rationalization of a path involving relative
sections (`.` or `..`) has to be application specific.

## Future work ##
One thing I'd like to do with this library is add specializations for specific
types of non-hierarchical URIs; `data:`, `mailto:`, and `geo:` are all strong
candidates for subclasses to support their formats, since they're very
well-defined. Since the current class structure supports hierarchical URIs very
well as-is, no further extensions seem necessary in that direction.

Once C++14 support is more widespread (or I can find an appropriate value to
check for, `get_username()` and `get_password()` will be marked as
`[[deprecated]]`, in order to appropriately warn on their use. Please heed this
warning, and don't use `get_username()` or `get_password()`, they're horribly
unsafe and only included for completeness. At some point in the future, calling
them may result in an exception being thrown at runtime.

As an optional future task, I might consider supporting sniffing query argument
separators from context; I'm not too interested in that right now, however,
since guessing as a parser is generally a good excuse to create a bug farm.