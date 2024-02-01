#include "duckdb/common/common.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_storage.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {

SecretMatch SecretStorage::SelectBestMatch(SecretEntry &secret_entry, const string &path, SecretMatch &current_best) {
	// Get secret match score
	auto match_score = secret_entry.secret->MatchScore(path);

	// On no match
	if (match_score == NumericLimits<int64_t>::Minimum()) {
		return current_best;
	}

	// The number of characters that match, where 0 means matching the catchall of "*"
	D_ASSERT(match_score >= 0);

	// Apply storage tie-break offset
	match_score = OffsetMatchScore(match_score);

	// Choose the best matching score, tie-breaking on secret name when necessary
	if (match_score > current_best.score) {
		return SecretMatch(secret_entry, match_score);
	} else if (match_score == current_best.score &&
	           secret_entry.secret->GetName() < current_best.GetSecret().GetName()) {
		return SecretMatch(secret_entry, match_score);
	} else {
		return current_best;
	}
}

optional_ptr<SecretEntry> CatalogSetSecretStorage::StoreSecret(unique_ptr<const BaseSecret> secret,
                                                               OnCreateConflict on_conflict,
                                                               optional_ptr<CatalogTransaction> transaction) {
	if (secrets->GetEntry(GetTransactionOrDefault(transaction), secret->GetName())) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			string persist_string = persistent ? "Persistent" : "Temporary";
			string storage_string = persistent ? " in secret storage '" + storage_name + "'" : "";
			throw InvalidInputException("%s secret with name '%s' already exists%s!", persist_string, secret->GetName(),
			                            storage_string);
		} else if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		} else if (on_conflict == OnCreateConflict::ALTER_ON_CONFLICT) {
			throw InternalException("unknown OnCreateConflict found while registering secret");
		} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			secrets->DropEntry(GetTransactionOrDefault(transaction), secret->GetName(), true, true);
		}
	}

	// Call write function
	WriteSecret(*secret, on_conflict);

	auto secret_name = secret->GetName();
	auto secret_entry = make_uniq<SecretCatalogEntry>(std::move(secret), Catalog::GetSystemCatalog(db));
	secret_entry->temporary = !persistent;
	secret_entry->secret->storage_mode = storage_name;
	secret_entry->secret->persist_type = persistent ? SecretPersistType::PERSISTENT : SecretPersistType::TEMPORARY;
	DependencyList l;
	secrets->CreateEntry(GetTransactionOrDefault(transaction), secret_name, std::move(secret_entry), l);

	auto secret_catalog_entry =
	    &secrets->GetEntry(GetTransactionOrDefault(transaction), secret_name)->Cast<SecretCatalogEntry>();
	return secret_catalog_entry->secret;
}

vector<reference<SecretEntry>> CatalogSetSecretStorage::AllSecrets(optional_ptr<CatalogTransaction> transaction) {
	vector<reference<SecretEntry>> ret_value;
	const std::function<void(CatalogEntry &)> callback = [&](CatalogEntry &entry) {
		auto &cast_entry = entry.Cast<SecretCatalogEntry>();
		ret_value.push_back(*cast_entry.secret);
	};
	secrets->Scan(GetTransactionOrDefault(transaction), callback);
	return ret_value;
}

void CatalogSetSecretStorage::DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
                                               optional_ptr<CatalogTransaction> transaction) {
	auto entry = secrets->GetEntry(GetTransactionOrDefault(transaction), name);
	if (!entry && on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		string persist_string = persistent ? "persistent" : "temporary";
		string storage_string = persistent ? " in secret storage '" + storage_name + "'" : "";
		throw InvalidInputException("Failed to remove non-existent %s secret '%s'%s", persist_string, name,
		                            storage_string);
	}

	secrets->DropEntry(GetTransactionOrDefault(transaction), name, true, true);
	RemoveSecret(name, on_entry_not_found);
}

SecretMatch CatalogSetSecretStorage::LookupSecret(const string &path, const string &type,
                                                  optional_ptr<CatalogTransaction> transaction) {
	auto best_match = SecretMatch();

	const std::function<void(CatalogEntry &)> callback = [&](CatalogEntry &entry) {
		auto &cast_entry = entry.Cast<SecretCatalogEntry>();
		if (cast_entry.secret->secret->GetType() == type) {
			best_match = SelectBestMatch(*cast_entry.secret, path, best_match);
		}
	};
	secrets->Scan(GetTransactionOrDefault(transaction), callback);

	if (best_match.HasMatch()) {
		return best_match;
	}

	return SecretMatch();
}

optional_ptr<SecretEntry> CatalogSetSecretStorage::GetSecretByName(const string &name,
                                                                   optional_ptr<CatalogTransaction> transaction) {
	auto res = secrets->GetEntry(GetTransactionOrDefault(transaction), name);

	if (res) {
		auto &cast_entry = res->Cast<SecretCatalogEntry>();
		return cast_entry.secret;
	}

	return nullptr;
}

LocalFileSecretStorage::LocalFileSecretStorage(SecretManager &manager, DatabaseInstance &db_p, const string &name_p,
                                               const string &secret_path)
    : CatalogSetSecretStorage(db_p, name_p), secret_path(secret_path) {
	persistent = true;

	LocalFileSystem fs;

	if (!fs.DirectoryExists(secret_path)) {
		fs.CreateDirectory(secret_path);
	}

	if (persistent_secrets.empty()) {
		fs.ListFiles(secret_path, [&](const string &fname, bool is_dir) {
			string full_path = fs.JoinPath(secret_path, fname);

			if (StringUtil::EndsWith(full_path, ".duckdb_secret")) {
				string secret_name = fname.substr(0, fname.size() - 14); // size of file ext
				persistent_secrets.insert(secret_name);
			}
		});
	}

	auto &catalog = Catalog::GetSystemCatalog(db);
	secrets = make_uniq<CatalogSet>(Catalog::GetSystemCatalog(db),
	                                make_uniq<DefaultSecretGenerator>(catalog, manager, persistent_secrets));
}

void CatalogSetSecretStorage::WriteSecret(const BaseSecret &secret, OnCreateConflict on_conflict) {
	// By default, this writes nothing
}
void CatalogSetSecretStorage::RemoveSecret(const string &name, OnEntryNotFound on_entry_not_found) {
	// By default, this writes nothing
}

CatalogTransaction CatalogSetSecretStorage::GetTransactionOrDefault(optional_ptr<CatalogTransaction> transaction) {
	if (transaction) {
		return *transaction;
	}
	return CatalogTransaction::GetSystemTransaction(db);
}

void LocalFileSecretStorage::WriteSecret(const BaseSecret &secret, OnCreateConflict on_conflict) {
	LocalFileSystem fs;
	auto file_path = fs.JoinPath(secret_path, secret.GetName() + ".duckdb_secret");

	if (fs.FileExists(file_path)) {
		fs.RemoveFile(file_path);
	}

	auto file_writer = BufferedFileWriter(fs, file_path);

	auto serializer = BinarySerializer(file_writer);
	serializer.Begin();
	secret.Serialize(serializer);
	serializer.End();

	file_writer.Flush();
}

void LocalFileSecretStorage::RemoveSecret(const string &secret, OnEntryNotFound on_entry_not_found) {
	LocalFileSystem fs;
	string file = fs.JoinPath(secret_path, secret + ".duckdb_secret");
	persistent_secrets.erase(secret);
	try {
		fs.RemoveFile(file);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::IO) {
			throw IOException("Failed to remove secret file '%s', the file may have been removed by another duckdb "
			                  "instance. (original error: '%s')",
			                  file, error.RawMessage());
		}
	}
}

} // namespace duckdb
