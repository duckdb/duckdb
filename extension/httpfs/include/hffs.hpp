#pragma once

#include "httpfs.hpp"

namespace duckdb {

struct ParsedHFUrl {
	//! Path within the
	string path;
	//! Name of the repo (i presume)
	string repository;

	//! Endpoint, defaults to HF
	string endpoint = "https://huggingface.co";
	//! Which revision/branch/tag to use
	string revision = "main";
	//! For DuckDB this may be a sensible default?
	string repo_type = "datasets";
};

class HuggingFaceFileSystem : public HTTPFileSystem {
public:
	~HuggingFaceFileSystem() override;

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;

	duckdb::unique_ptr<ResponseWrapper> HeadRequest(FileHandle &handle, string hf_url, HeaderMap header_map) override;
	duckdb::unique_ptr<ResponseWrapper> GetRequest(FileHandle &handle, string hf_url, HeaderMap header_map) override;
	duckdb::unique_ptr<ResponseWrapper> GetRangeRequest(FileHandle &handle, string hf_url, HeaderMap header_map,
	                                                    idx_t file_offset, char *buffer_out,
	                                                    idx_t buffer_out_len) override;

	bool CanHandleFile(const string &fpath) override {
		return fpath.rfind("hf://", 0) == 0;
	};

	string GetName() const override {
		return "HuggingFaceFileSystem";
	}
	static ParsedHFUrl HFUrlParse(const string &url);
	string GetHFUrl(const ParsedHFUrl &url);
	string GetTreeUrl(const ParsedHFUrl &url, idx_t limit);
	string GetFileUrl(const ParsedHFUrl &url);

	static void SetParams(HTTPParams &params, const string &path, optional_ptr<FileOpener> opener);

protected:
	duckdb::unique_ptr<HTTPFileHandle> CreateHandle(const string &path, FileOpenFlags flags,
	                                                optional_ptr<FileOpener> opener) override;

	string ListHFRequest(ParsedHFUrl &url, HTTPParams &http_params, string &next_page_url,
	                     optional_ptr<HTTPState> state);
};

class HFFileHandle : public HTTPFileHandle {
	friend class HuggingFaceFileSystem;

public:
	HFFileHandle(FileSystem &fs, ParsedHFUrl hf_url, string http_url, FileOpenFlags flags,
	             const HTTPParams &http_params)
	    : HTTPFileHandle(fs, std::move(http_url), flags, http_params), parsed_url(std::move(hf_url)) {
	}
	~HFFileHandle() override;

	unique_ptr<duckdb_httplib_openssl::Client> CreateClient(optional_ptr<ClientContext> client_context) override;

protected:
	ParsedHFUrl parsed_url;
};

} // namespace duckdb
