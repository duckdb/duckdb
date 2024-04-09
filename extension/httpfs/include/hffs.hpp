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

	bool CanHandleFile(const string &fpath) override {
		return fpath.rfind("hf://", 0) == 0;
	};

	string GetName() const override {
		return "HuggingFaceFileSystem";
	}
	static ParsedHFUrl HFUrlParse(const string &url);
	string GetHFUrl(const ParsedHFUrl &url);
	string GetTreeUrl(const ParsedHFUrl &url);
	string GetFileUrl(const ParsedHFUrl &url);

protected:
	duckdb::unique_ptr<HTTPFileHandle> CreateHandle(const string &path, FileOpenFlags flags,
	                                                        optional_ptr<FileOpener> opener) override;

	string ListHFRequest(ParsedHFUrl &url, HTTPParams &http_params, string &next_page_url, optional_ptr<HTTPState> state);
};

} // namespace duckdb
