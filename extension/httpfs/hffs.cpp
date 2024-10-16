#include "hffs.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "http_state.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

#include <chrono>
#include <string>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include <map>

namespace duckdb {

HuggingFaceFileSystem::~HuggingFaceFileSystem() {
}

static string ParseNextUrlFromLinkHeader(const string &link_header_content) {
	auto split_outer = StringUtil::Split(link_header_content, ',');
	for (auto &split : split_outer) {
		auto split_inner = StringUtil::Split(split, ';');
		if (split_inner.size() != 2) {
			throw IOException("Unexpected link header for huggingface pagination: %s", link_header_content);
		}

		StringUtil::Trim(split_inner[1]);
		if (split_inner[1] == "rel=\"next\"") {
			StringUtil::Trim(split_inner[0]);

			if (!StringUtil::StartsWith(split_inner[0], "<") || !StringUtil::EndsWith(split_inner[0], ">")) {
				throw IOException("Unexpected link header for huggingface pagination: %s", link_header_content);
			}

			return split_inner[0].substr(1, split_inner[0].size() - 2);
		}
	}

	throw IOException("Failed to parse Link header for paginated response, pagination support");
}

HFFileHandle::~HFFileHandle() {};

unique_ptr<duckdb_httplib_openssl::Client> HFFileHandle::CreateClient(optional_ptr<ClientContext> client_context) {
	return HTTPFileSystem::GetClient(this->http_params, parsed_url.endpoint.c_str(), this);
}

string HuggingFaceFileSystem::ListHFRequest(ParsedHFUrl &url, HTTPParams &http_params, string &next_page_url,
                                            optional_ptr<HTTPState> state) {
	HeaderMap header_map;
	auto headers = HTTPFileSystem::InitializeHeaders(header_map, http_params);
	string link_header_result;

	auto client = HTTPFileSystem::GetClient(http_params, url.endpoint.c_str(), nullptr);
	std::stringstream response;

	std::function<duckdb_httplib_openssl::Result(void)> request([&]() {
		if (state) {
			state->get_count++;
		}

		return client->Get(
		    next_page_url.c_str(), *headers,
		    [&](const duckdb_httplib_openssl::Response &response) {
			    if (response.status >= 400) {
				    throw HTTPException(response, "HTTP GET error on '%s' (HTTP %d)", next_page_url, response.status);
			    }
			    auto link_res = response.headers.find("Link");
			    if (link_res != response.headers.end()) {
				    link_header_result = link_res->second;
			    }
			    return true;
		    },
		    [&](const char *data, size_t data_length) {
			    if (state) {
				    state->total_bytes_received += data_length;
			    }
			    response << string(data, data_length);
			    return true;
		    });
	});

	auto res = RunRequestWithRetry(request, next_page_url, "GET", http_params, nullptr);

	if (res->code != 200) {
		throw IOException(res->error + " error for HTTP GET to '" + next_page_url + "'");
	}

	if (!link_header_result.empty()) {
		next_page_url = ParseNextUrlFromLinkHeader(link_header_result);
	} else {
		next_page_url = "";
	}

	return response.str();
}

static bool Match(vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                  vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end) {

	while (key != key_end && pattern != pattern_end) {
		if (*pattern == "**") {
			if (std::next(pattern) == pattern_end) {
				return true;
			}
			while (key != key_end) {
				if (Match(key, key_end, std::next(pattern), pattern_end)) {
					return true;
				}
				key++;
			}
			return false;
		}
		if (!LikeFun::Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
			return false;
		}
		key++;
		pattern++;
	}
	return key == key_end && pattern == pattern_end;
}

void ParseListResult(string &input, vector<string> &files, vector<string> &directories) {
	enum parse_entry { FILE, DIR, UNKNOWN };
	idx_t idx = 0;
	idx_t nested = 0;
	bool found_path;
	parse_entry type;
	string current_string;
base:
	found_path = false;
	type = parse_entry::UNKNOWN;
	for (; idx < input.size(); idx++) {
		if (input[idx] == '{') {
			idx++;
			goto entry;
		}
	}
	goto end;
entry:
	while (idx < input.size()) {
		if (input[idx] == '}') {
			if (nested) {
				idx++;
				nested--;
				continue;
			} else if (!found_path || type == parse_entry::UNKNOWN) {
				throw IOException("Failed to parse list result");
			} else if (type == parse_entry::FILE) {
				files.push_back("/" + current_string);
			} else {
				directories.push_back("/" + current_string);
			}
			current_string = "";
			idx++;
			goto base;
		} else if (input[idx] == '{') {
			nested++;
			idx++;
		} else if (strncmp(input.c_str() + idx, "\"type\":\"directory\"", 18) == 0) {
			type = parse_entry::DIR;
			idx += 18;
		} else if (strncmp(input.c_str() + idx, "\"type\":\"file\"", 13) == 0) {
			type = parse_entry::FILE;
			idx += 13;
		} else if (strncmp(input.c_str() + idx, "\"path\":\"", 8) == 0) {
			idx += 8;
			found_path = true;
			goto pathname;
		} else {
			idx++;
		}
	}
	goto end;
pathname:
	while (idx < input.size()) {
		// Handle escaped quote in url
		if (input[idx] == '\\' && idx + 1 < input.size() && input[idx] == '\"') {
			current_string += '\"';
			idx += 2;
		} else if (input[idx] == '\"') {
			idx++;
			goto entry;
		} else {
			current_string += input[idx];
			idx++;
		}
	}
end:
	return;
}

// Some valid example Urls:
// - hf://datasets/lhoestq/demo1/default/train/0000.parquet
// - hf://datasets/lhoestq/demo1/default/train/*.parquet
// - hf://datasets/lhoestq/demo1/*/train/file_[abc].parquet
// - hf://datasets/lhoestq/demo1/**/train/*.parquet
vector<string> HuggingFaceFileSystem::Glob(const string &path, FileOpener *opener) {
	// Ensure the glob pattern is a valid HF url
	auto parsed_glob_url = HFUrlParse(path);
	auto first_wildcard_pos = parsed_glob_url.path.find_first_of("*[\\");

	if (first_wildcard_pos == string::npos) {
		return {path};
	}

	string shared_path = parsed_glob_url.path.substr(0, first_wildcard_pos);
	auto last_path_slash = shared_path.find_last_of('/', first_wildcard_pos);

	// trim the final
	if (last_path_slash == string::npos) {
		// Root path
		shared_path = "";
	} else {
		shared_path = shared_path.substr(0, last_path_slash);
	}

	FileOpenerInfo info;
	info.file_path = path;
	auto http_params = HTTPParams::ReadFrom(opener, info);
	SetParams(http_params, path, opener);
	auto http_state = HTTPState::TryGetState(opener).get();

	ParsedHFUrl curr_hf_path = parsed_glob_url;
	curr_hf_path.path = shared_path;

	vector<string> files;
	vector<string> dirs = {shared_path};
	string next_page_url = "";

	// Loop over the paths and paginated responses for each path
	while (true) {
		if (next_page_url.empty() && !dirs.empty()) {
			// Done with previous dir, load the next one
			curr_hf_path.path = dirs.back();
			dirs.pop_back();
			next_page_url = HuggingFaceFileSystem::GetTreeUrl(curr_hf_path, http_params.hf_max_per_page);
		} else if (next_page_url.empty()) {
			// No more pages to read, also no more dirs
			break;
		}

		auto response_str = ListHFRequest(curr_hf_path, http_params, next_page_url, http_state);
		ParseListResult(response_str, files, dirs);
	}

	vector<string> pattern_splits = StringUtil::Split(parsed_glob_url.path, "/");
	vector<string> result;
	for (const auto &file : files) {

		vector<string> file_splits = StringUtil::Split(file, "/");
		bool is_match = Match(file_splits.begin(), file_splits.end(), pattern_splits.begin(), pattern_splits.end());

		if (is_match) {
			curr_hf_path.path = file;
			result.push_back(GetHFUrl(curr_hf_path));
		}
	}

	// Prune files using match
	return result;
}

unique_ptr<ResponseWrapper> HuggingFaceFileSystem::HeadRequest(FileHandle &handle, string hf_url,
                                                               HeaderMap header_map) {
	auto &hf_handle = handle.Cast<HFFileHandle>();
	auto http_url = HuggingFaceFileSystem::GetFileUrl(hf_handle.parsed_url);
	return HTTPFileSystem::HeadRequest(handle, http_url, header_map);
}

unique_ptr<ResponseWrapper> HuggingFaceFileSystem::GetRequest(FileHandle &handle, string s3_url, HeaderMap header_map) {
	auto &hf_handle = handle.Cast<HFFileHandle>();
	auto http_url = HuggingFaceFileSystem::GetFileUrl(hf_handle.parsed_url);
	return HTTPFileSystem::GetRequest(handle, http_url, header_map);
}

unique_ptr<ResponseWrapper> HuggingFaceFileSystem::GetRangeRequest(FileHandle &handle, string s3_url,
                                                                   HeaderMap header_map, idx_t file_offset,
                                                                   char *buffer_out, idx_t buffer_out_len) {
	auto &hf_handle = handle.Cast<HFFileHandle>();
	auto http_url = HuggingFaceFileSystem::GetFileUrl(hf_handle.parsed_url);
	return HTTPFileSystem::GetRangeRequest(handle, http_url, header_map, file_offset, buffer_out, buffer_out_len);
}

unique_ptr<HTTPFileHandle> HuggingFaceFileSystem::CreateHandle(const string &path, FileOpenFlags flags,
                                                               optional_ptr<FileOpener> opener) {
	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	auto parsed_url = HFUrlParse(path);

	FileOpenerInfo info;
	info.file_path = path;

	auto params = HTTPParams::ReadFrom(opener, info);
	SetParams(params, path, opener);

	return duckdb::make_uniq<HFFileHandle>(*this, std::move(parsed_url), path, flags, params);
}

void HuggingFaceFileSystem::SetParams(HTTPParams &params, const string &path, optional_ptr<FileOpener> opener) {
	auto secret_manager = FileOpener::TryGetSecretManager(opener);
	auto transaction = FileOpener::TryGetCatalogTransaction(opener);
	if (secret_manager && transaction) {
		auto secret_match = secret_manager->LookupSecret(*transaction, path, "huggingface");

		if (secret_match.HasMatch()) {
			const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);
			params.bearer_token = kv_secret.TryGetValue("token", true).ToString();
		}
	}
}

static void ThrowParseError(const string &url) {
	throw IOException(
	    "Failed to parse '%s'. Please format url like: 'hf://datasets/my-username/my-dataset/path/to/file.parquet'",
	    url);
}

ParsedHFUrl HuggingFaceFileSystem::HFUrlParse(const string &url) {
	ParsedHFUrl result;

	if (!StringUtil::StartsWith(url, "hf://")) {
		throw InternalException("Not an hf url");
	}

	size_t last_delim = 5;
	size_t curr_delim;

	// Parse Repository type
	curr_delim = url.find('/', last_delim);
	if (curr_delim == string::npos) {
		ThrowParseError(url);
	}
	result.repo_type = url.substr(last_delim, curr_delim - last_delim);
	if (result.repo_type != "datasets" && result.repo_type != "spaces") {
		throw IOException(
		    "Failed to parse: '%s'. Currently DuckDB only supports querying datasets or spaces, so the url should "
		    "start with 'hf://datasets' or 'hf://spaces'",
		    url);
	}

	last_delim = curr_delim;

	// Parse repository and revision
	auto repo_delim = url.find('/', last_delim + 1);
	if (repo_delim == string::npos) {
		ThrowParseError(url);
	}

	auto next_at = url.find('@', repo_delim + 1);
	auto next_slash = url.find('/', repo_delim + 1);

	if (next_slash == string::npos) {
		ThrowParseError(url);
	}

	if (next_at != string::npos && next_at < next_slash) {
		result.repository = url.substr(last_delim + 1, next_at - last_delim - 1);
		result.revision = url.substr(next_at + 1, next_slash - next_at - 1);
	} else {
		result.repository = url.substr(last_delim + 1, next_slash - last_delim - 1);
	}
	last_delim = next_slash;

	// The remainder is the path
	result.path = url.substr(last_delim);

	return result;
}

string HuggingFaceFileSystem::GetHFUrl(const ParsedHFUrl &url) {
	if (url.revision == "main") {
		return "hf://" + url.repo_type + "/" + url.repository + url.path;
	} else {
		return "hf://" + url.repo_type + "/" + url.repository + "@" + url.revision + url.path;
	}
}

string HuggingFaceFileSystem::GetTreeUrl(const ParsedHFUrl &url, idx_t limit) {
	//! Url format {endpoint}/api/{repo_type}/{repository}/tree/{revision}{encoded_path_in_repo}
	string http_url = url.endpoint;

	http_url = JoinPath(http_url, "api");
	http_url = JoinPath(http_url, url.repo_type);
	http_url = JoinPath(http_url, url.repository);
	http_url = JoinPath(http_url, "tree");
	http_url = JoinPath(http_url, url.revision);
	http_url += url.path;

	if (limit > 0) {
		http_url += "?limit=" + to_string(limit);
	}

	return http_url;
}

string HuggingFaceFileSystem::GetFileUrl(const ParsedHFUrl &url) {
	//! Url format {endpoint}/{repo_type}[/{repository}/{revision}{encoded_path_in_repo}
	string http_url = url.endpoint;
	http_url = JoinPath(http_url, url.repo_type);
	http_url = JoinPath(http_url, url.repository);
	http_url = JoinPath(http_url, "resolve");
	http_url = JoinPath(http_url, url.revision);
	http_url += url.path;

	return http_url;
}

} // namespace duckdb
