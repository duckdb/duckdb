// Copyright (C) 2015 Ben Lewis <benjf5+github@gmail.com>
// Licensed under the MIT license.

#pragma once
#include <cctype>
#include <map>
#include <string>
#include <stdexcept>
#include <utility>

namespace duckdb {

class uri
{
  /* URIs are broadly divided into two categories: hierarchical and
   * non-hierarchical. Both hierarchical URIs and non-hierarchical URIs have a
   * few elements in common; all URIs have a scheme of one or more alphanumeric
   * characters followed by a colon, and they all may optionally have a query
   * component preceded by a question mark, and a fragment component preceded by
   * an octothorpe (hash mark: '#'). The query consists of stanzas separated by
   * either ampersands ('&') or semicolons (';') (but only one or the other),
   * and each stanza consists of a key and an optional value; if the value
   * exists, the key and value must be divided by an equals sign.
   *
   * The following is an example from Wikipedia of a hierarchical URI:
   * scheme:[//[user:password@]domain[:port]][/]path[?query][#fragment]
   */

public:

  enum class scheme_category
  {
    Hierarchical,
    NonHierarchical
  };

  enum class component
  {
    Scheme,
    Content,
    Username,
    Password,
    Host,
    Port,
    Path,
    Query,
    Fragment
  };
  
  enum class query_argument_separator
  {
    ampersand,
    semicolon
  };

  uri(char const *uri_text, scheme_category category = scheme_category::Hierarchical,
      query_argument_separator separator = query_argument_separator::ampersand) :
    m_category(category),
    m_port(0),
    m_path_is_rooted(false),
    m_separator(separator)
  {
    setup(std::string(uri_text), category);
  };

  uri(std::string const &uri_text, scheme_category category = scheme_category::Hierarchical,
      query_argument_separator separator = query_argument_separator::ampersand) : 
    m_category(category),
    m_port(0),
    m_path_is_rooted(false),
    m_separator(separator)
  {
    setup(uri_text, category);
  };

  uri(std::map<component, std::string> const &components,
      scheme_category category,
      bool rooted_path,
      query_argument_separator separator = query_argument_separator::ampersand) :
    m_category(category),
    m_path_is_rooted(rooted_path),
    m_separator(separator)
  {
    if (components.count(component::Scheme))
    {
      if (components.at(component::Scheme).length() == 0)
      {
	throw std::invalid_argument("Scheme cannot be empty.");
      }
      m_scheme = components.at(component::Scheme);
    }
    else
    {
      throw std::invalid_argument("A URI must have a scheme.");
    }

    if (category == scheme_category::Hierarchical)
    {
      if (components.count(component::Content))
      {
	throw std::invalid_argument("The content component is only for use in non-hierarchical URIs.");
      }

      bool has_username = components.count(component::Username);
      bool has_password = components.count(component::Password);
      if (has_username && has_password)
      {
	m_username = components.at(component::Username);
	m_password = components.at(component::Password);
      }
      else if ((has_username && !has_password) || (!has_username && has_password))
      {
	throw std::invalid_argument("If a username or password is supplied, both must be provided.");
      }

      if (components.count(component::Host))
      {
	m_host = components.at(component::Host);
      }

      if (components.count(component::Port))
      {
	m_port = std::stoul(components.at(component::Port));
      }

      if (components.count(component::Path))
      {
	m_path = components.at(component::Path);
      }
      else
      {
	throw std::invalid_argument("A path is required on a hierarchical URI, even an empty path.");
      }
    }
    else
    {
      if (components.count(component::Username)
	  || components.count(component::Password)
	  || components.count(component::Host)
	  || components.count(component::Port)
	  || components.count(component::Path))
      {
	throw std::invalid_argument("None of the hierarchical components are allowed in a non-hierarchical URI.");
      }

      if (components.count(component::Content))
      {
	m_content = components.at(component::Content);
      }
      else
      {
	throw std::invalid_argument("Content is a required component for a non-hierarchical URI, even an empty string.");
      }
    }

    if (components.count(component::Query))
    {
      m_query = components.at(component::Query);
    }

    if (components.count(component::Fragment))
    {
      m_fragment = components.at(component::Fragment);
    } 
  }    

  uri(uri const &other, std::map<component, std::string> const &replacements) :
    m_category(other.m_category),
    m_path_is_rooted(other.m_path_is_rooted),
    m_separator(other.m_separator)
  {
    m_scheme = (replacements.count(component::Scheme))
      ? replacements.at(component::Scheme) : other.m_scheme;

    if (m_category == scheme_category::Hierarchical)
    {
      m_username = (replacements.count(component::Username))
	? replacements.at(component::Username) : other.m_username;

      m_password = (replacements.count(component::Password))
	? replacements.at(component::Password) : other.m_password;

      m_host = (replacements.count(component::Host))
	? replacements.at(component::Host) : other.m_host;

      m_port = (replacements.count(component::Port))
	? std::stoul(replacements.at(component::Port)) : other.m_port;

      m_path = (replacements.count(component::Path))
	? replacements.at(component::Path) : other.m_path;
    }
    else
    {
      m_content = (replacements.count(component::Content))
	? replacements.at(component::Content) : other.m_content;
    }

    m_query = (replacements.count(component::Query))
      ? replacements.at(component::Query) : other.m_query;

    m_fragment = (replacements.count(component::Fragment))
      ? replacements.at(component::Fragment) : other.m_fragment;
  }

  // Copy constructor; just use the copy assignment operator internally.
  uri(uri const &other)
  {
    *this = other;
  };

  // Copy assignment operator
  uri &operator=(uri const &other)
  {
    if (this != &other)
    {
      m_scheme = other.m_scheme;
      m_content = other.m_content;
      m_username = other.m_username;
      m_password = other.m_password;
      m_host = other.m_host;
      m_path = other.m_path;
      m_query = other.m_query;
      m_fragment = other.m_fragment;
      m_query_dict = other.m_query_dict;
      m_category = other.m_category;
      m_port = other.m_port;
      m_path_is_rooted = other.m_path_is_rooted;
      m_separator = other.m_separator;
    }
    return *this;
  }

  ~uri() { };

  std::string const &get_scheme() const
  {
    return m_scheme;
  };

  scheme_category get_scheme_category() const
  {
    return m_category;
  };

  std::string const &get_content() const
  {
    if (m_category != scheme_category::NonHierarchical)
    {
      throw std::domain_error("The content component is only valid for non-hierarchical URIs.");
    }
    return m_content;
  };

  std::string const &get_username() const
  {
    if (m_category != scheme_category::Hierarchical)
    {
      throw std::domain_error("The username component is only valid for hierarchical URIs.");
    }
    return m_username;
  };

  std::string const &get_password() const
  {
    if (m_category != scheme_category::Hierarchical)
    {
      throw std::domain_error("The password component is only valid for hierarchical URIs.");
    }
    return m_password;
  };
  
  std::string const &get_host() const
  {
    if (m_category != scheme_category::Hierarchical)
    {
      throw std::domain_error("The host component is only valid for hierarchical URIs.");
    }
    return m_host;
  };

  unsigned long get_port() const
  {
    if (m_category != scheme_category::Hierarchical)
    {
      throw std::domain_error("The port component is only valid for hierarchical URIs.");
    }
    return m_port;
  };

  std::string const &get_path() const
  {
    if (m_category != scheme_category::Hierarchical)
    {
      throw std::domain_error("The path component is only valid for hierarchical URIs.");
    }
    return m_path;
  };

  std::string const &get_query() const
  {
    return m_query;
  };

  std::map<std::string, std::string> const &get_query_dictionary() const
  {
    return m_query_dict;
  };

  std::string const &get_fragment() const
  {
    return m_fragment;
  };

  std::string to_string() const
  {
    std::string full_uri;
    full_uri.append(m_scheme);
    full_uri.append(":");

    if (m_category == scheme_category::Hierarchical)
    {
      full_uri.append("//");
      if (!(m_username.empty() || m_password.empty()))
      {
        full_uri.append(m_username);
        full_uri.append(":");
        full_uri.append(m_password);
        full_uri.append("@");
      }

      full_uri.append(m_host);

      if (m_port != 0)
      {
        full_uri.append(":");
        full_uri.append(std::to_string(m_port));
      }
    }

    if (m_path_is_rooted)
    {
      full_uri.append("/");
    }
    full_uri.append(m_path);

    if (!m_query.empty())
    {
      full_uri.append("?");
      full_uri.append(m_query);
    }

    if (!m_fragment.empty())
    {
      full_uri.append("#");
      full_uri.append(m_fragment);
    }

    return full_uri;
  };
  
private:

  void setup(std::string const &uri_text, scheme_category category)
  {
    size_t const uri_length = uri_text.length();

    if (uri_length == 0)
    {
      throw std::invalid_argument("URIs cannot be of zero length.");
    }

    std::string::const_iterator cursor = parse_scheme(uri_text, 
                                                      uri_text.begin());
    // After calling parse_scheme, *cursor == ':'; none of the following parsers
    // expect a separator character, so we advance the cursor upon calling them.
    cursor = parse_content(uri_text, (cursor + 1));

    if ((cursor != uri_text.end()) && (*cursor == '?'))
    {
      cursor = parse_query(uri_text, (cursor + 1));
    }

    if ((cursor != uri_text.end()) && (*cursor == '#'))
    {
      cursor = parse_fragment(uri_text, (cursor + 1));
    }

    init_query_dictionary(); // If the query string is empty, this will be empty too.

  };

  std::string::const_iterator parse_scheme(std::string const &uri_text,
					   std::string::const_iterator scheme_start)
  {
    std::string::const_iterator scheme_end = scheme_start;
    while ((scheme_end != uri_text.end()) && (*scheme_end != ':'))
    {
      if (!(std::isalnum(*scheme_end) || (*scheme_end == '-')
	    || (*scheme_end == '+') || (*scheme_end == '.')))
      {
	throw std::invalid_argument("Invalid character found in the scheme component. Supplied URI was: \""
				    + uri_text + "\".");
      }
      ++scheme_end;
    }

    if (scheme_end == uri_text.end())
    {
      throw std::invalid_argument("End of URI found while parsing the scheme. Supplied URI was: \""
				  + uri_text + "\".");
    }

    if (scheme_start == scheme_end)
    {
      throw std::invalid_argument("Scheme component cannot be zero-length. Supplied URI was: \""
				  + uri_text + "\".");
    }

    m_scheme = std::string(scheme_start, scheme_end);
    return scheme_end;
  };

  std::string::const_iterator parse_content(std::string const &uri_text,
					    std::string::const_iterator content_start)
  {
    std::string::const_iterator content_end = content_start;
    while ((content_end != uri_text.end()) && (*content_end != '?') && (*content_end != '#'))
    {
      ++content_end;
    }

    m_content = std::string(content_start, content_end);

    if ((m_category == scheme_category::Hierarchical) && (m_content.length() > 0))
    {
      // If it's a hierarchical URI, the content should be parsed for the hierarchical components.
      std::string::const_iterator path_start = m_content.begin();
      std::string::const_iterator path_end = m_content.end();
      if (!m_content.compare(0, 2, "//"))
      {
	// In this case an authority component is present.
	std::string::const_iterator authority_cursor = (m_content.begin() + 2);
	if (m_content.find_first_of('@') != std::string::npos)
	{
	  std::string::const_iterator userpass_divider = parse_username(uri_text,
									m_content,
									authority_cursor);
	  authority_cursor = parse_password(uri_text, m_content, (userpass_divider + 1));
	  // After this call, *authority_cursor == '@', so we skip over it.
	  ++authority_cursor;
	}

	authority_cursor = parse_host(uri_text, m_content, authority_cursor);

	if ((authority_cursor != m_content.end()) && (*authority_cursor == ':'))
	{
	  authority_cursor = parse_port(uri_text, m_content, (authority_cursor + 1));
	}

	if ((authority_cursor != m_content.end()) && (*authority_cursor == '/' ))
	{
	  // Then the path is rooted, and we should note this.
	  m_path_is_rooted = true;
	  path_start = authority_cursor + 1;
	}
	
	// If we've reached the end and no path is present then set path_start
        // to the end.
        if (authority_cursor == m_content.end()) {
          path_start = m_content.end();
        }
      }
      else if (!m_content.compare(0, 1, "/"))
      {
	m_path_is_rooted = true;
	++path_start;
      }

      // We can now build the path based on what remains in the content string,
      // since that's all that exists after the host and optional port component.
      m_path = std::string(path_start, path_end);
    }
    return content_end;
  };

  std::string::const_iterator parse_username(std::string const &uri_text,
					     std::string const &content,
					     std::string::const_iterator username_start)
  {
    std::string::const_iterator username_end = username_start;
    // Since this is only reachable when '@' was in the content string, we can
    // ignore the end-of-string case.
    while (*username_end != ':')
    {
      if (*username_end == '@')
      {
	throw std::invalid_argument("Username must be followed by a password. Supplied URI was: \""
				    + uri_text + "\".");
      }
      ++username_end;
    }
    m_username = std::string(username_start, username_end);
    return username_end;
  };

  std::string::const_iterator parse_password(std::string const &uri_text,
					     std::string const &content,
					     std::string::const_iterator password_start)
  {
    std::string::const_iterator password_end = password_start;
    while (*password_end != '@')
    {
      ++password_end;
    }

    m_password = std::string(password_start, password_end);
    return password_end;
  };

  std::string::const_iterator parse_host(std::string const &uri_text,
					 std::string const &content,
					 std::string::const_iterator host_start)
  {
    std::string::const_iterator host_end = host_start;
    // So, the host can contain a few things. It can be a domain, it can be an
    // IPv4 address, it can be an IPv6 address, or an IPvFuture literal. In the
    // case of those last two, it's of the form [...] where what's between the
    // brackets is a matter of which IPv?? version it is.
    while (host_end != content.end())
    {
      if (*host_end == '[')
      {
	// We're parsing an IPv6 or IPvFuture address, so we should handle that
	// instead of the normal procedure.
	while ((host_end != content.end()) && (*host_end != ']'))
	{
	  ++host_end;
	}

	if (host_end == content.end())
	{
	  throw std::invalid_argument("End of content component encountered "
				      "while parsing the host component. "
				      "Supplied URI was: \""
				      + uri_text + "\".");
	}

	++host_end;
	break;
	// We can stop looping, we found the end of the IP literal, which is the
	// whole of the host component when one's in use.
      }
      else if ((*host_end == ':') || (*host_end == '/'))
      {
	break;
      }
      else
      {
	++host_end;
      }
    }

    m_host = std::string(host_start, host_end);
    return host_end;
  };

  std::string::const_iterator parse_port(std::string const &uri_text,
					std::string const &content,
					std::string::const_iterator port_start)
  {
    std::string::const_iterator port_end = port_start;
    while ((port_end != content.end()) && (*port_end != '/'))
    {
      if (!std::isdigit(*port_end))
      {
	throw std::invalid_argument("Invalid character while parsing the port. "
				    "Supplied URI was: \"" + uri_text + "\".");
      }

      ++port_end;
    }

    m_port = std::stoul(std::string(port_start, port_end));
    return port_end;
  };

  std::string::const_iterator parse_query(std::string const &uri_text,
                                          std::string::const_iterator query_start)
  {
    std::string::const_iterator query_end = query_start;
    while ((query_end != uri_text.end()) && (*query_end != '#'))
    {
      // Queries can contain almost any character except hash, which is reserved
      // for the start of the fragment.
      ++query_end;
    }
    m_query = std::string(query_start, query_end);
    return query_end;
  };

  std::string::const_iterator parse_fragment(std::string const &uri_text,
                                             std::string::const_iterator fragment_start)
  {
    m_fragment = std::string(fragment_start, uri_text.end());
    return uri_text.end();
  };  
  
  void init_query_dictionary()
  {
    if (!m_query.empty())
    {
      // Loop over the query string looking for '&'s, then check each one for
      // an '=' to find keys and values; if there's not an '=' then the key
      // will have an empty value in the map.
      char separator = (m_separator == query_argument_separator::ampersand) ? '&' : ';';
      size_t carat = 0;
      size_t stanza_end = m_query.find_first_of(separator);
      do
      {
	std::string stanza = m_query.substr(carat, ((stanza_end != std::string::npos) ? (stanza_end - carat) : std::string::npos));
	size_t key_value_divider = stanza.find_first_of('=');
	std::string key = stanza.substr(0, key_value_divider);
	std::string value;
	if (key_value_divider != std::string::npos)
	{
	  value = stanza.substr((key_value_divider + 1));
	}

	if (m_query_dict.count(key) != 0)
	{
	  throw std::invalid_argument("Bad key in the query string!");
	}

	m_query_dict.emplace(key, value);
	carat = ((stanza_end != std::string::npos) ? (stanza_end + 1)
		 : std::string::npos);
	stanza_end = m_query.find_first_of(separator, carat);
      }
      while ((stanza_end != std::string::npos) 
	     || (carat != std::string::npos));
    }
  }

  std::string m_scheme;
  std::string m_content;
  std::string m_username;
  std::string m_password;
  std::string m_host;
  std::string m_path;
  std::string m_query;
  std::string m_fragment;

  std::map<std::string, std::string> m_query_dict;

  scheme_category m_category;
  unsigned long m_port;
  bool m_path_is_rooted;
  query_argument_separator m_separator;
};

} // namespace duckdb
