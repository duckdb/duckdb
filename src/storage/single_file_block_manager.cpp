#include "duckdb/storage/single_file_block_manager.hpp"

#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/common/encryption_key_manager.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "mbedtls_wrapper.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

const char MainHeader::MAGIC_BYTES[] = "DUCK";
const char MainHeader::CANARY[] = "DUCKKEY";

void SerializeVersionNumber(WriteStream &ser, const string &version_str) {
	data_t version[MainHeader::MAX_VERSION_SIZE];
	memset(version, 0, MainHeader::MAX_VERSION_SIZE);
	memcpy(version, version_str.c_str(), MinValue<idx_t>(version_str.size(), MainHeader::MAX_VERSION_SIZE));
	ser.WriteData(version, MainHeader::MAX_VERSION_SIZE);
}

void SerializeDBIdentifier(WriteStream &ser, data_ptr_t db_identifier_p) {
	data_t db_identifier[MainHeader::DB_IDENTIFIER_LEN];
	memset(db_identifier, 0, MainHeader::DB_IDENTIFIER_LEN);
	memcpy(db_identifier, db_identifier_p, MainHeader::DB_IDENTIFIER_LEN);
	ser.WriteData(db_identifier, MainHeader::DB_IDENTIFIER_LEN);
}

void SerializeEncryptionMetadata(WriteStream &ser, data_ptr_t metadata_p, const bool encrypted) {
	// Zero-initialize.
	data_t metadata[MainHeader::ENCRYPTION_METADATA_LEN];
	memset(metadata, 0, MainHeader::ENCRYPTION_METADATA_LEN);

	// Write metadata, if encrypted.
	if (encrypted) {
		memcpy(metadata, metadata_p, MainHeader::ENCRYPTION_METADATA_LEN);
	}
	ser.WriteData(metadata, MainHeader::ENCRYPTION_METADATA_LEN);
}

void DeserializeVersionNumber(ReadStream &stream, data_t *dest) {
	memset(dest, 0, MainHeader::MAX_VERSION_SIZE);
	stream.ReadData(dest, MainHeader::MAX_VERSION_SIZE);
}

void DeserializeEncryptionData(ReadStream &stream, data_t *dest, idx_t size) {
	memset(dest, 0, size);
	stream.ReadData(dest, size);
}

void GenerateDBIdentifier(uint8_t *db_identifier) {
	memset(db_identifier, 0, MainHeader::DB_IDENTIFIER_LEN);
	RandomEngine engine;
	engine.RandomData(db_identifier, MainHeader::DB_IDENTIFIER_LEN);
}

void EncryptCanary(MainHeader &main_header, const shared_ptr<EncryptionState> &encryption_state,
                   const_data_ptr_t derived_key) {
	uint8_t canary_buffer[MainHeader::CANARY_BYTE_SIZE];

	// we zero-out the iv and the (not yet) encrypted canary
	uint8_t iv[MainHeader::AES_IV_LEN];
	memset(iv, 0, MainHeader::AES_IV_LEN);
	memset(canary_buffer, 0, MainHeader::CANARY_BYTE_SIZE);

	encryption_state->InitializeEncryption(iv, MainHeader::AES_IV_LEN, derived_key,
	                                       MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	encryption_state->Process(reinterpret_cast<const_data_ptr_t>(MainHeader::CANARY), MainHeader::CANARY_BYTE_SIZE,
	                          canary_buffer, MainHeader::CANARY_BYTE_SIZE);

	main_header.SetEncryptedCanary(canary_buffer);
}

bool DecryptCanary(MainHeader &main_header, const shared_ptr<EncryptionState> &encryption_state,
                   data_ptr_t derived_key) {
	// just zero-out the iv
	uint8_t iv[MainHeader::AES_IV_LEN];
	memset(iv, 0, MainHeader::AES_IV_LEN);

	//! allocate a buffer for the decrypted canary
	data_t decrypted_canary[MainHeader::CANARY_BYTE_SIZE];
	memset(decrypted_canary, 0, MainHeader::CANARY_BYTE_SIZE);

	//! Decrypt the canary
	encryption_state->InitializeDecryption(iv, MainHeader::AES_IV_LEN, derived_key,
	                                       MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	encryption_state->Process(main_header.GetEncryptedCanary(), MainHeader::CANARY_BYTE_SIZE, decrypted_canary,
	                          MainHeader::CANARY_BYTE_SIZE);

	//! compare if the decrypted canary is correct
	if (memcmp(decrypted_canary, MainHeader::CANARY, MainHeader::CANARY_BYTE_SIZE) != 0) {
		return false;
	}

	return true;
}

void MainHeader::Write(WriteStream &ser) {
	ser.WriteData(const_data_ptr_cast(MAGIC_BYTES), MAGIC_BYTE_SIZE);
	ser.Write<uint64_t>(version_number);
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		ser.Write<uint64_t>(flags[i]);
	}

	SerializeVersionNumber(ser, DuckDB::LibraryVersion());
	SerializeVersionNumber(ser, DuckDB::SourceID());

	// We always serialize, and write zeros, if not set.
	auto encryption_enabled = IsEncrypted();
	SerializeEncryptionMetadata(ser, encryption_metadata, encryption_enabled);
	SerializeDBIdentifier(ser, db_identifier);
	SerializeEncryptionMetadata(ser, encrypted_canary, encryption_enabled);
}

void MainHeader::CheckMagicBytes(QueryContext context, FileHandle &handle) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];
	if (handle.GetFileSize() < MainHeader::MAGIC_BYTE_SIZE + MainHeader::MAGIC_BYTE_OFFSET) {
		throw IOException("The file \"%s\" exists, but it is not a valid DuckDB database file!", handle.path);
	}
	handle.Read(context, magic_bytes, MainHeader::MAGIC_BYTE_SIZE, MainHeader::MAGIC_BYTE_OFFSET);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file \"%s\" exists, but it is not a valid DuckDB database file!", handle.path);
	}
}

MainHeader MainHeader::Read(ReadStream &source) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];

	MainHeader header;
	source.ReadData(magic_bytes, MainHeader::MAGIC_BYTE_SIZE);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file is not a valid DuckDB database file!");
	}

	header.version_number = source.Read<uint64_t>();

	// Check the version number to determine if we can read this file.
	if (header.version_number < VERSION_NUMBER_LOWER || header.version_number > VERSION_NUMBER_UPPER) {
		auto version = GetDuckDBVersions(header.version_number);
		string version_text;
		if (!version.empty()) {
			// Known version.
			version_text = "DuckDB version " + string(version);
		} else {
			version_text = string("an ") +
			               (VERSION_NUMBER_UPPER > header.version_number ? "older development" : "newer") +
			               string(" version of DuckDB");
		}
		throw IOException(
		    "Trying to read a database file with version number %lld, but we can only read versions between %lld and "
		    "%lld.\n"
		    "The database file was created with %s.\n\n"
		    "Newer DuckDB version might introduce backward incompatible changes (possibly guarded by compatibility "
		    "settings).\n"
		    "See the storage page for migration strategy and more information: https://duckdb.org/internals/storage",
		    header.version_number, VERSION_NUMBER_LOWER, VERSION_NUMBER_UPPER, version_text);
	}

	// Read the flags.
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		header.flags[i] = source.Read<uint64_t>();
	}

	DeserializeVersionNumber(source, header.library_git_desc);
	DeserializeVersionNumber(source, header.library_git_hash);

	// We always deserialize, and read zeros, if not set.
	DeserializeEncryptionData(source, header.encryption_metadata, MainHeader::ENCRYPTION_METADATA_LEN);
	DeserializeEncryptionData(source, header.db_identifier, MainHeader::DB_IDENTIFIER_LEN);
	DeserializeEncryptionData(source, header.encrypted_canary, MainHeader::CANARY_BYTE_SIZE);

	return header;
}

void DatabaseHeader::Write(WriteStream &ser) {
	ser.Write<uint64_t>(iteration);
	ser.Write<idx_t>(meta_block);
	ser.Write<idx_t>(free_list);
	ser.Write<uint64_t>(block_count);
	ser.Write<idx_t>(block_alloc_size);
	ser.Write<idx_t>(vector_size);
	ser.Write<idx_t>(serialization_compatibility);
}

DatabaseHeader DatabaseHeader::Read(const MainHeader &main_header, ReadStream &source) {
	DatabaseHeader header;
	header.iteration = source.Read<uint64_t>();
	header.meta_block = source.Read<idx_t>();
	header.free_list = source.Read<idx_t>();
	header.block_count = source.Read<uint64_t>();
	header.block_alloc_size = source.Read<idx_t>();

	// backwards compatibility
	if (!header.block_alloc_size) {
		header.block_alloc_size = DEFAULT_BLOCK_ALLOC_SIZE;
	}

	header.vector_size = source.Read<idx_t>();
	if (!header.vector_size) {
		// backwards compatibility
		header.vector_size = DEFAULT_STANDARD_VECTOR_SIZE;
	}
	if (header.vector_size != STANDARD_VECTOR_SIZE) {
		throw IOException("Cannot read database file: DuckDB's compiled vector size is %llu bytes, but the file has a "
		                  "vector size of %llu bytes.",
		                  STANDARD_VECTOR_SIZE, header.vector_size);
	}

	// Default to 1 for version 64, else read from file.
	header.serialization_compatibility = main_header.version_number == 64 ? 1 : source.Read<idx_t>();

	return header;
}

template <class T>
void SerializeHeaderStructure(T header, data_ptr_t ptr) {
	MemoryStream ser(ptr, Storage::FILE_HEADER_SIZE);
	header.Write(ser);
}

MainHeader DeserializeMainHeader(data_ptr_t ptr) {
	MemoryStream source(ptr, Storage::FILE_HEADER_SIZE);
	return MainHeader::Read(source);
}

DatabaseHeader DeserializeDatabaseHeader(const MainHeader &main_header, data_ptr_t ptr) {
	MemoryStream source(ptr, Storage::FILE_HEADER_SIZE);
	return DatabaseHeader::Read(main_header, source);
}

SingleFileBlockManager::SingleFileBlockManager(AttachedDatabase &db_p, const string &path_p,
                                               const StorageManagerOptions &options)
    : BlockManager(BufferManager::GetBufferManager(db_p), options.block_alloc_size, options.block_header_size),
      db(db_p), path(path_p), header_buffer(BlockAllocator::Get(db_p), FileBufferType::MANAGED_BUFFER,
                                            Storage::FILE_HEADER_SIZE - options.block_header_size.GetIndex(),
                                            options.block_header_size.GetIndex()),
      iteration_count(0), options(options) {
}

FileOpenFlags SingleFileBlockManager::GetFileFlags(bool create_new) const {
	FileOpenFlags result;
	if (options.read_only) {
		D_ASSERT(!create_new);
		result = FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS | FileLockType::READ_LOCK;
	} else {
		result = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ | FileLockType::WRITE_LOCK;
		if (create_new) {
			result |= FileFlags::FILE_FLAGS_FILE_CREATE;
		}
	}
	if (options.use_direct_io) {
		result |= FileFlags::FILE_FLAGS_DIRECT_IO;
	}
	// database files can be read from in parallel
	result |= FileFlags::FILE_FLAGS_PARALLEL_ACCESS;
	result |= FileFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS;
	return result;
}

void SingleFileBlockManager::AddStorageVersionTag() {
	db.tags["storage_version"] = GetStorageVersionName(options.storage_version.GetIndex(), true);
}

uint64_t SingleFileBlockManager::GetVersionNumber() const {
	auto storage_version = options.storage_version.GetIndex();
	if (storage_version < 4) {
		return VERSION_NUMBER;
	}
	// Look up the matching version number.
	auto version_name = GetStorageVersionName(storage_version, false);
	return GetStorageVersion(version_name.c_str()).GetIndex();
}

MainHeader ConstructMainHeader(idx_t version_number) {
	MainHeader header;
	header.version_number = version_number;
	memset(header.flags, 0, sizeof(uint64_t) * MainHeader::FLAG_COUNT);
	return header;
}

void SingleFileBlockManager::StoreEncryptedCanary(AttachedDatabase &db, MainHeader &main_header, const string &key_id) {
	const_data_ptr_t key = EncryptionEngine::GetKeyFromCache(db.GetDatabase(), key_id);
	// Encrypt canary with the derived key
	auto encryption_state = db.GetDatabase().GetEncryptionUtil()->CreateEncryptionState(
	    main_header.GetEncryptionCipher(), MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	EncryptCanary(main_header, encryption_state, key);
}

void SingleFileBlockManager::StoreDBIdentifier(MainHeader &main_header, data_ptr_t db_identifier) {
	main_header.SetDBIdentifier(db_identifier);
}

void SingleFileBlockManager::StoreEncryptionMetadata(MainHeader &main_header) const {
	// The first byte is the key derivation function (kdf).
	// The second byte is for the usage of AAD.
	// The third byte is for the cipher.
	// The subsequent byte is empty.
	// The last 4 bytes are the key length.

	uint8_t metadata[MainHeader::ENCRYPTION_METADATA_LEN];
	memset(metadata, 0, MainHeader::ENCRYPTION_METADATA_LEN);
	data_ptr_t offset = metadata;

	Store<uint8_t>(options.encryption_options.kdf, offset);
	offset++;
	Store<uint8_t>(options.encryption_options.additional_authenticated_data, offset);
	offset++;
	Store<uint8_t>(db.GetStorageManager().GetCipher(), offset);
	offset += 2;
	Store<uint32_t>(options.encryption_options.key_length, offset);

	main_header.SetEncryptionMetadata(metadata);
}

void SingleFileBlockManager::CheckAndAddEncryptionKey(MainHeader &main_header, string &user_key) {
	//! Get the database identifier.
	uint8_t db_identifier[MainHeader::DB_IDENTIFIER_LEN];
	memset(db_identifier, 0, MainHeader::DB_IDENTIFIER_LEN);
	memcpy(db_identifier, main_header.GetDBIdentifier(), MainHeader::DB_IDENTIFIER_LEN);

	//! Check if the correct key is used to decrypt the database
	// Derive the encryption key and add it to cache
	data_t derived_key[MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH];
	EncryptionKeyManager::DeriveKey(user_key, db_identifier, derived_key);

	auto encryption_state = db.GetDatabase().GetEncryptionUtil()->CreateEncryptionState(
	    main_header.GetEncryptionCipher(), MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	if (!DecryptCanary(main_header, encryption_state, derived_key)) {
		throw IOException("Wrong encryption key used to open the database file");
	}

	options.encryption_options.derived_key_id = EncryptionEngine::AddKeyToCache(db.GetDatabase(), derived_key);
	auto &catalog = db.GetCatalog().Cast<DuckCatalog>();
	catalog.SetEncryptionKeyId(options.encryption_options.derived_key_id);
	catalog.SetIsEncrypted();

	std::fill(user_key.begin(), user_key.end(), 0);
	user_key.clear();
}

void SingleFileBlockManager::CheckAndAddEncryptionKey(MainHeader &main_header) {
	return CheckAndAddEncryptionKey(main_header, *options.encryption_options.user_key);
}

void SingleFileBlockManager::CreateNewDatabase(QueryContext context) {
	auto flags = GetFileFlags(true);

	auto encryption_enabled = options.encryption_options.encryption_enabled;
	if (encryption_enabled) {
		if (!db.GetDatabase().GetEncryptionUtil()->SupportsEncryption() && !options.read_only) {
			throw InvalidConfigurationException(
			    "The database was opened with encryption enabled, but DuckDB currently has a read-only crypto module "
			    "loaded. Please re-open using READONLY, or ensure httpfs is loaded using `LOAD httpfs`.");
		}
	}

	// open the RDBMS handle
	auto &fs = FileSystem::Get(db);
	handle = fs.OpenFile(path, flags);
	header_buffer.Clear();

	options.version_number = GetVersionNumber();
	db.GetStorageManager().SetStorageVersion(options.storage_version.GetIndex());
	AddStorageVersionTag();

	MainHeader main_header = ConstructMainHeader(options.version_number.GetIndex());

	// Derive the encryption key and add it to the cache.
	// Not used for plain databases.
	data_t derived_key[MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH];

	// We need the unique database identifier, if the storage version is new enough.
	// If encryption is enabled, we also use it as the salt.
	memset(options.db_identifier, 0, MainHeader::DB_IDENTIFIER_LEN);
	if (encryption_enabled || options.version_number.GetIndex() >= 67) {
		GenerateDBIdentifier(options.db_identifier);
	}

	if (encryption_enabled) {
		// The key is given via ATTACH.
		EncryptionKeyManager::DeriveKey(*options.encryption_options.user_key, options.db_identifier, derived_key);
		options.encryption_options.user_key = nullptr;

		// if no encryption cipher is specified, use GCM
		if (db.GetStorageManager().GetCipher() == EncryptionTypes::INVALID) {
			db.GetStorageManager().SetCipher(EncryptionTypes::GCM);
		}

		// Set the encrypted DB bit to 1.
		main_header.SetEncrypted();

		// The derived key is wiped in AddKeyToCache.
		options.encryption_options.derived_key_id = EncryptionEngine::AddKeyToCache(db.GetDatabase(), derived_key);
		auto &catalog = db.GetCatalog().Cast<DuckCatalog>();
		catalog.SetEncryptionKeyId(options.encryption_options.derived_key_id);
		catalog.SetIsEncrypted();
	}

	// Store all metadata in the main header.
	if (encryption_enabled) {
		StoreEncryptionMetadata(main_header);
	}
	// Always store the database identifier.
	StoreDBIdentifier(main_header, options.db_identifier);
	if (encryption_enabled) {
		StoreEncryptedCanary(db, main_header, options.encryption_options.derived_key_id);
	}

	// Write the main database header.
	SerializeHeaderStructure<MainHeader>(main_header, header_buffer.buffer);
	ChecksumAndWrite(context, header_buffer, 0, true);

	// write the database headers
	// initialize meta_block and free_list to INVALID_BLOCK because the database file does not contain any actual
	// content yet
	DatabaseHeader h1;
	// header 1
	h1.iteration = 0;
	h1.meta_block = idx_t(INVALID_BLOCK);
	h1.free_list = idx_t(INVALID_BLOCK);
	h1.block_count = 0;
	// We create the SingleFileBlockManager with the desired block allocation size before calling CreateNewDatabase.
	h1.block_alloc_size = GetBlockAllocSize();
	h1.vector_size = STANDARD_VECTOR_SIZE;
	h1.serialization_compatibility = options.storage_version.GetIndex();
	SerializeHeaderStructure<DatabaseHeader>(h1, header_buffer.buffer);
	ChecksumAndWrite(context, header_buffer, Storage::FILE_HEADER_SIZE);

	// header 2
	DatabaseHeader h2;
	h2.iteration = 0;
	h2.meta_block = idx_t(INVALID_BLOCK);
	h2.free_list = idx_t(INVALID_BLOCK);
	h2.block_count = 0;
	// We create the SingleFileBlockManager with the desired block allocation size before calling CreateNewDatabase.
	h2.block_alloc_size = GetBlockAllocSize();
	h2.vector_size = STANDARD_VECTOR_SIZE;
	h2.serialization_compatibility = options.storage_version.GetIndex();
	SerializeHeaderStructure<DatabaseHeader>(h2, header_buffer.buffer);
	ChecksumAndWrite(context, header_buffer, Storage::FILE_HEADER_SIZE * 2ULL);

	// ensure that writing to disk is completed before returning
	handle->Sync();
	// we start with h2 as active_header, this way our initial write will be in h1
	iteration_count = 0;
	active_header = 1;
	max_block = 0;
}

void SingleFileBlockManager::LoadExistingDatabase(QueryContext context) {
	auto flags = GetFileFlags(false);

	// open the RDBMS handle
	auto &fs = FileSystem::Get(db);
	handle = fs.OpenFile(path, flags);
	if (!handle) {
		// this can only happen in read-only mode - as that is when we set FILE_FLAGS_NULL_IF_NOT_EXISTS
		throw IOException("Cannot open database \"%s\" in read-only mode: database does not exist", path);
	}

	MainHeader::CheckMagicBytes(context, *handle);
	// otherwise, we check the metadata of the file
	ReadAndChecksum(context, header_buffer, 0, true);

	uint64_t delta = 0;
	if (GetBlockHeaderSize() > DEFAULT_BLOCK_HEADER_STORAGE_SIZE) {
		delta = GetBlockHeaderSize() - DEFAULT_BLOCK_HEADER_STORAGE_SIZE;
	}

	MainHeader main_header = DeserializeMainHeader(header_buffer.buffer - delta);
	memcpy(options.db_identifier, main_header.GetDBIdentifier(), MainHeader::DB_IDENTIFIER_LEN);

	if (!main_header.IsEncrypted() && options.encryption_options.encryption_enabled) {
		throw CatalogException("A key is explicitly specified, but database \"%s\" is not encrypted", path);
		// database is not encrypted, but is tried to be opened with a key
	}

	if (main_header.IsEncrypted()) {
		if (options.encryption_options.encryption_enabled) {
			//! Encryption is set

			//! Check if our encryption module can write, if not, we should throw here
			if (!db.GetDatabase().GetEncryptionUtil()->SupportsEncryption() && !options.read_only) {
				throw InvalidConfigurationException(
				    "The database is encrypted, but DuckDB currently has a read-only crypto module loaded. Either "
				    "re-open the database using `ATTACH '..' (READONLY)`, or ensure httpfs is loaded using `LOAD "
				    "httpfs`.");
			}

			//! Check if the given key upon attach is correct
			// Derive the encryption key and add it to cache
			CheckAndAddEncryptionKey(main_header);
			// delete user key ptr
			options.encryption_options.user_key = nullptr;
		} else {
			// if encrypted, but no encryption key given
			throw CatalogException("Cannot open encrypted database \"%s\" without a key", path);
		}

		// if a cipher was provided, check if it is the same as in the config
		auto stored_cipher = main_header.GetEncryptionCipher();
		auto config_cipher = db.GetStorageManager().GetCipher();
		if (config_cipher != EncryptionTypes::INVALID && config_cipher != stored_cipher) {
			throw CatalogException("Cannot open encrypted database \"%s\" with a different cipher (%s) than the one "
			                       "used to create it (%s)",
			                       path, EncryptionTypes::CipherToString(config_cipher),
			                       EncryptionTypes::CipherToString(stored_cipher));
		}

		// This avoids the cipher from being downgrades by an attacker FIXME: we likely want to have a propervalidation
		// of the cipher used instead of this trick to avoid downgrades
		if (stored_cipher != EncryptionTypes::GCM) {
			if (config_cipher == EncryptionTypes::INVALID) {
				throw CatalogException(
				    "Cannot open encrypted database \"%s\" without explicitly specifying the "
				    "encryption cipher for security reasons. Please make sure you understand the security implications "
				    "and re-attach the database specifying the desired cipher.",
				    path);
			}
		}

		// this is ugly, but the storage manager does not know the cipher type before
		db.GetStorageManager().SetCipher(stored_cipher);
	}

	options.version_number = main_header.version_number;

	// read the database headers from disk
	DatabaseHeader h1;
	ReadAndChecksum(context, header_buffer, Storage::FILE_HEADER_SIZE);
	h1 = DeserializeDatabaseHeader(main_header, header_buffer.buffer);

	DatabaseHeader h2;
	ReadAndChecksum(context, header_buffer, Storage::FILE_HEADER_SIZE * 2ULL);
	h2 = DeserializeDatabaseHeader(main_header, header_buffer.buffer);

	// check the header with the highest iteration count
	if (h1.iteration > h2.iteration) {
		// h1 is active header
		active_header = 0;
		Initialize(h1, GetOptionalBlockAllocSize());
	} else {
		// h2 is active header
		active_header = 1;
		Initialize(h2, GetOptionalBlockAllocSize());
	}
	AddStorageVersionTag();
	LoadFreeList(context);
}

void SingleFileBlockManager::CheckChecksum(data_ptr_t start_ptr, uint64_t delta, bool skip_block_header) const {
	uint64_t stored_checksum;
	uint64_t computed_checksum;

	if (skip_block_header && delta > 0) {
		//! Even with encryption enabled, the main header should be plaintext
		stored_checksum = Load<uint64_t>(start_ptr);
		computed_checksum = Checksum(start_ptr + DEFAULT_BLOCK_HEADER_STORAGE_SIZE, GetBlockSize() + delta);
	} else {
		//! We do have to decrypt other headers
		stored_checksum = Load<uint64_t>(start_ptr + delta);
		computed_checksum = Checksum(start_ptr + GetBlockHeaderSize(), GetBlockSize());
	}

	// verify the checksum
	if (stored_checksum != computed_checksum) {
		throw IOException("Corrupt database file: computed checksum %llu does not match stored checksum %llu in block "
		                  "at location %llu",
		                  computed_checksum, stored_checksum, start_ptr);
	}
}

void SingleFileBlockManager::CheckChecksum(FileBuffer &block, uint64_t location, uint64_t delta,
                                           bool skip_block_header) const {
	uint64_t stored_checksum;
	uint64_t computed_checksum;

	if (skip_block_header && delta > 0) {
		//! Even with encryption enabled, the main header should be plaintext
		stored_checksum = Load<uint64_t>(block.InternalBuffer());
		computed_checksum = Checksum(block.buffer - delta, block.Size() + delta);
	} else {
		//! We do have to decrypt other headers
		stored_checksum = Load<uint64_t>(block.InternalBuffer() + delta);
		computed_checksum = Checksum(block.buffer, block.Size());
	}

	// verify the checksum
	if (stored_checksum != computed_checksum) {
		throw IOException("Corrupt database file: computed checksum %llu does not match stored checksum %llu in block "
		                  "at location %llu",
		                  computed_checksum, stored_checksum, location);
	}
}

void SingleFileBlockManager::ReadAndChecksum(QueryContext context, FileBuffer &block, uint64_t location,
                                             bool skip_block_header) const {
	// read the buffer from disk
	block.Read(context, *handle, location);

	//! calculate delta header bytes (if any)
	uint64_t delta = GetBlockHeaderSize() - Storage::DEFAULT_BLOCK_HEADER_SIZE;

	if (options.encryption_options.encryption_enabled && !skip_block_header) {
		auto key_id = options.encryption_options.derived_key_id;
		EncryptionEngine::DecryptBlock(db, key_id, block.InternalBuffer(), block.Size(), delta);
	}

	CheckChecksum(block, location, delta, skip_block_header);
}

void SingleFileBlockManager::ChecksumAndWrite(QueryContext context, FileBuffer &block, uint64_t location,
                                              bool skip_block_header) const {
	auto delta = GetBlockHeaderSize() - Storage::DEFAULT_BLOCK_HEADER_SIZE;
	uint64_t checksum;

	if (skip_block_header && delta > 0) {
		//! This happens only for the main database header
		//! We do not encrypt the main database header
		memmove(block.InternalBuffer() + Storage::DEFAULT_BLOCK_HEADER_SIZE, block.buffer, block.Size());
		//! zero out the last bytes of the block
		memset(block.InternalBuffer() + block.Size() + Storage::DEFAULT_BLOCK_HEADER_SIZE, 0, delta);
		checksum = Checksum(block.buffer - delta, block.Size() + delta);
		delta = 0;
	} else {
		checksum = Checksum(block.buffer, block.Size());
	}

	Store<uint64_t>(checksum, block.InternalBuffer() + delta);

	// encrypt if required
	unique_ptr<FileBuffer> temp_buffer_manager;
	if (options.encryption_options.encryption_enabled && !skip_block_header) {
		auto key_id = options.encryption_options.derived_key_id;
		temp_buffer_manager =
		    make_uniq<FileBuffer>(BlockAllocator::Get(db), block.GetBufferType(), block.Size(), GetBlockHeaderSize());
		EncryptionEngine::EncryptBlock(db, key_id, block, *temp_buffer_manager, delta);
		temp_buffer_manager->Write(context, *handle, location);
	} else {
		block.Write(context, *handle, location);
	}
}

void SingleFileBlockManager::Initialize(const DatabaseHeader &header, const optional_idx block_alloc_size) {
	free_list_id = header.free_list;
	meta_block = header.meta_block;
	iteration_count = header.iteration;
	max_block = NumericCast<block_id_t>(header.block_count);
	if (options.storage_version.IsValid()) {
		// storage version specified explicity - use requested storage version
		auto requested_compat_version = options.storage_version.GetIndex();
		if (requested_compat_version < header.serialization_compatibility) {
			throw InvalidInputException(
			    "Error opening \"%s\": cannot initialize database with storage version %d - which is lower than what "
			    "the database itself uses (%d). The storage version of an existing database cannot be lowered.",
			    path, requested_compat_version, header.serialization_compatibility);
		}
	} else {
		// load storage version from header
		options.storage_version = header.serialization_compatibility;
	}
	if (header.serialization_compatibility > SerializationCompatibility::Latest().serialization_version) {
		throw InvalidInputException(
		    "Error opening \"%s\": file was written with a storage version greater than the latest version supported "
		    "by this DuckDB instance. Try opening the file with a newer version of DuckDB.",
		    path);
	}

	db.GetStorageManager().SetStorageVersion(options.storage_version.GetIndex());

	if (block_alloc_size.IsValid() && block_alloc_size.GetIndex() != header.block_alloc_size) {
		throw InvalidInputException(
		    "Error opening \"%s\": cannot initialize the same database with a different block size: provided block "
		    "size: %llu, file block size: %llu",
		    path, GetBlockAllocSize(), header.block_alloc_size);
	}

	SetBlockAllocSize(header.block_alloc_size);
}

void SingleFileBlockManager::LoadFreeList(QueryContext context) {
	MetaBlockPointer free_pointer(free_list_id, 0);
	if (!free_pointer.IsValid()) {
		// no free list
		return;
	}
	MetadataReader reader(GetMetadataManager(), free_pointer, nullptr, BlockReaderType::REGISTER_BLOCKS);
	auto free_list_count = reader.Read<uint64_t>(context);
	free_list.clear();
	for (idx_t i = 0; i < free_list_count; i++) {
		auto block = reader.Read<block_id_t>(context);
		free_list.insert(block);
		newly_freed_list.insert(block);
	}
	auto multi_use_blocks_count = reader.Read<uint64_t>(context);
	multi_use_blocks.clear();
	for (idx_t i = 0; i < multi_use_blocks_count; i++) {
		auto block_id = reader.Read<block_id_t>(context);
		auto usage_count = reader.Read<uint32_t>(context);
		multi_use_blocks[block_id] = usage_count;
	}
	GetMetadataManager().Read(reader);
	GetMetadataManager().MarkBlocksAsModified();
}

bool SingleFileBlockManager::IsRootBlock(MetaBlockPointer root) {
	return root.block_pointer == meta_block;
}

block_id_t SingleFileBlockManager::GetFreeBlockId() {
	lock_guard<mutex> lock(block_lock);
	block_id_t block;
	if (!free_list.empty()) {
		// The free list is not empty, so we take its first element.
		block = *free_list.begin();
		// erase the entry from the free list again
		free_list.erase(free_list.begin());
		newly_freed_list.erase(block);
	} else {
		block = max_block++;
	}
	return block;
}

block_id_t SingleFileBlockManager::PeekFreeBlockId() {
	lock_guard<mutex> lock(block_lock);
	if (!free_list.empty()) {
		return *free_list.begin();
	} else {
		return max_block;
	}
}

void SingleFileBlockManager::MarkBlockAsFree(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	D_ASSERT(block_id >= 0);
	D_ASSERT(block_id < max_block);
	if (free_list.find(block_id) != free_list.end()) {
		throw InternalException("MarkBlockAsFree called but block %llu was already freed!", block_id);
	}
	multi_use_blocks.erase(block_id);
	free_list.insert(block_id);
	newly_freed_list.insert(block_id);
}

void SingleFileBlockManager::MarkBlockAsUsed(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	D_ASSERT(block_id >= 0);
	if (max_block <= block_id) {
		// the block is past the current max_block
		// in this case we need to increment  "max_block" to "block_id"
		// any blocks in the middle are added to the free list
		// i.e. if max_block = 0, and block_id = 3, we need to add blocks 1 and 2 to the free list
		while (max_block < block_id) {
			free_list.insert(max_block);
			max_block++;
		}
		max_block++;
	} else if (free_list.find(block_id) != free_list.end()) {
		// block is currently in the free list - erase
		free_list.erase(block_id);
		newly_freed_list.erase(block_id);
	} else {
		// block is already in use - increase reference count
		IncreaseBlockReferenceCountInternal(block_id);
	}
}

void SingleFileBlockManager::MarkBlockAsModified(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	D_ASSERT(block_id >= 0);
	D_ASSERT(block_id < max_block);

	// check if the block is a multi-use block
	auto entry = multi_use_blocks.find(block_id);
	if (entry != multi_use_blocks.end()) {
		// it is! reduce the reference count of the block
		entry->second--;
		// check the reference count: is the block still a multi-use block?
		if (entry->second <= 1) {
			// no longer a multi-use block!
			multi_use_blocks.erase(entry);
		}
		return;
	}
	// Check for multi-free
	// TODO: Fix the bug that causes this assert to fire, then uncomment it.
	// D_ASSERT(modified_blocks.find(block_id) == modified_blocks.end());
	D_ASSERT(free_list.find(block_id) == free_list.end());
	modified_blocks.insert(block_id);
}

void SingleFileBlockManager::IncreaseBlockReferenceCountInternal(block_id_t block_id) {
	D_ASSERT(block_id >= 0);
	D_ASSERT(block_id < max_block);
	D_ASSERT(free_list.find(block_id) == free_list.end());
	auto entry = multi_use_blocks.find(block_id);
	if (entry != multi_use_blocks.end()) {
		entry->second++;
	} else {
		multi_use_blocks[block_id] = 2;
	}
}

void SingleFileBlockManager::VerifyBlocks(const unordered_map<block_id_t, idx_t> &block_usage_count) {
	// probably don't need this?
	lock_guard<mutex> lock(block_lock);
	// all blocks should be accounted for - either in the block_usage_count, or in the free list
	set<block_id_t> referenced_blocks;
	for (auto &block : block_usage_count) {
		if (block.first == INVALID_BLOCK) {
			continue;
		}
		if (block.first >= max_block) {
			throw InternalException("Block %lld is used, but it is bigger than the max block %d", block.first,
			                        max_block);
		}
		referenced_blocks.insert(block.first);
		if (block.second > 1) {
			// multi-use block
			auto entry = multi_use_blocks.find(block.first);
			if (entry == multi_use_blocks.end()) {
				throw InternalException("Block %lld was used %llu times, but not present in multi_use_blocks",
				                        block.first, block.second);
			}
			if (entry->second != block.second) {
				throw InternalException(
				    "Block %lld was used %llu times, but multi_use_blocks says it is used %llu times", block.first,
				    block.second, entry->second);
			}
		} else {
			D_ASSERT(block.second > 0);
			auto entry = free_list.find(block.first);
			if (entry != free_list.end()) {
				throw InternalException("Block %lld was used, but it is present in the free list", block.first);
			}
		}
	}
	for (auto &free_block : free_list) {
		referenced_blocks.insert(free_block);
	}
	if (referenced_blocks.size() != NumericCast<idx_t>(max_block)) {
		// not all blocks are accounted for
		string missing_blocks;
		for (block_id_t i = 0; i < max_block; i++) {
			if (referenced_blocks.find(i) == referenced_blocks.end()) {
				if (!missing_blocks.empty()) {
					missing_blocks += ", ";
				}
				missing_blocks += to_string(i);
			}
		}
		throw InternalException(
		    "Blocks %s were neither present in the free list or in the block_usage_count (max block %lld)",
		    missing_blocks, max_block);
	}
}

void SingleFileBlockManager::IncreaseBlockReferenceCount(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	IncreaseBlockReferenceCountInternal(block_id);
}

idx_t SingleFileBlockManager::GetMetaBlock() {
	return meta_block;
}

idx_t SingleFileBlockManager::TotalBlocks() {
	lock_guard<mutex> lock(block_lock);
	return NumericCast<idx_t>(max_block);
}

idx_t SingleFileBlockManager::FreeBlocks() {
	lock_guard<mutex> lock(block_lock);
	return free_list.size();
}

bool SingleFileBlockManager::IsRemote() {
	return !handle->OnDiskFile();
}

bool SingleFileBlockManager::Prefetch() {
	switch (DBConfig::GetSetting<StorageBlockPrefetchSetting>(db.GetDatabase())) {
	case StorageBlockPrefetch::NEVER:
		return false;
	case StorageBlockPrefetch::DEBUG_FORCE_ALWAYS:
	case StorageBlockPrefetch::ALWAYS_PREFETCH:
		return !InMemory();
	case StorageBlockPrefetch::REMOTE_ONLY:
		return IsRemote();
	default:
		throw InternalException("Unknown StorageBlockPrefetch type");
	}
}

unique_ptr<Block> SingleFileBlockManager::ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) {
	D_ASSERT(source_buffer.AllocSize() == GetBlockAllocSize());
	// FIXME; maybe we should pass the block header size explicitly
	return make_uniq<Block>(source_buffer, block_id, GetBlockHeaderSize());
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock(block_id_t block_id, FileBuffer *source_buffer) {
	// FIXME; maybe we should pass the block header size explicitly
	unique_ptr<Block> result;
	if (source_buffer) {
		result = ConvertBlock(block_id, *source_buffer);
	} else {
		result = make_uniq<Block>(BlockAllocator::Get(db), block_id, *this);
	}
	result->Initialize(options.debug_initialize);
	return result;
}

idx_t SingleFileBlockManager::GetBlockLocation(block_id_t block_id) const {
	return BLOCK_START + NumericCast<idx_t>(block_id) * GetBlockAllocSize();
}

void SingleFileBlockManager::ReadBlock(data_ptr_t internal_buffer, uint64_t block_size, bool skip_block_header) const {
	//! calculate delta header bytes (if any)
	uint64_t delta = GetBlockHeaderSize() - Storage::DEFAULT_BLOCK_HEADER_SIZE;

	if (options.encryption_options.encryption_enabled && !skip_block_header) {
		EncryptionEngine::DecryptBlock(db, options.encryption_options.derived_key_id, internal_buffer, block_size,
		                               delta);
	}

	CheckChecksum(internal_buffer, delta, skip_block_header);
}

void SingleFileBlockManager::ReadBlock(Block &block, bool skip_block_header) const {
	// read the buffer from disk
	auto location = GetBlockLocation(block.id);
	block.Read(QueryContext(), *handle, location);

	//! calculate delta header bytes (if any)
	uint64_t delta = GetBlockHeaderSize() - Storage::DEFAULT_BLOCK_HEADER_SIZE;

	if (options.encryption_options.encryption_enabled && !skip_block_header) {
		EncryptionEngine::DecryptBlock(db, options.encryption_options.derived_key_id, block.InternalBuffer(),
		                               block.Size(), delta);
	}

	CheckChecksum(block, location, delta, skip_block_header);
}

void SingleFileBlockManager::Read(QueryContext context, Block &block) {
	D_ASSERT(block.id >= 0);
	D_ASSERT(std::find(free_list.begin(), free_list.end(), block.id) == free_list.end());
	ReadAndChecksum(context, block, GetBlockLocation(block.id));
}

void SingleFileBlockManager::ReadBlocks(FileBuffer &buffer, block_id_t start_block, idx_t block_count) {
	D_ASSERT(start_block >= 0);
	D_ASSERT(block_count >= 1);

	// read the buffer from disk
	auto location = GetBlockLocation(start_block);
	buffer.Read(QueryContext(), *handle, location);

	// for each of the blocks - verify the checksum
	auto ptr = buffer.InternalBuffer();
	for (idx_t i = 0; i < block_count; i++) {
		auto start_ptr = ptr + i * GetBlockAllocSize();
		ReadBlock(start_ptr, GetBlockSize());
	}
}

void SingleFileBlockManager::Write(FileBuffer &buffer, block_id_t block_id) {
	Write(QueryContext(), buffer, block_id);
}

void SingleFileBlockManager::Write(QueryContext context, FileBuffer &buffer, block_id_t block_id) {
	D_ASSERT(block_id >= 0);
	ChecksumAndWrite(context, buffer, BLOCK_START + NumericCast<idx_t>(block_id) * GetBlockAllocSize());
}

void SingleFileBlockManager::Truncate() {
	BlockManager::Truncate();
	idx_t blocks_to_truncate = 0;
	// reverse iterate over the free-list
	for (auto entry = free_list.rbegin(); entry != free_list.rend(); entry++) {
		auto block_id = *entry;
		if (block_id + 1 != max_block) {
			break;
		}
		blocks_to_truncate++;
		max_block--;
	}
	if (blocks_to_truncate == 0) {
		// nothing to truncate
		return;
	}
	// truncate the file
	free_list.erase(free_list.lower_bound(max_block), free_list.end());
	newly_freed_list.erase(newly_freed_list.lower_bound(max_block), newly_freed_list.end());
	handle->Truncate(NumericCast<int64_t>(BLOCK_START + NumericCast<idx_t>(max_block) * GetBlockAllocSize()));
}

vector<MetadataHandle> SingleFileBlockManager::GetFreeListBlocks() {
	vector<MetadataHandle> free_list_blocks;
	auto &metadata_manager = GetMetadataManager();

	// reserve all blocks that we are going to write the free list to
	// since these blocks are no longer free we cannot just include them in the free list!
	auto block_size = metadata_manager.GetMetadataBlockSize() - sizeof(idx_t);
	idx_t allocated_size = 0;
	while (true) {
		auto free_list_size = sizeof(uint64_t) + sizeof(block_id_t) * (free_list.size() + modified_blocks.size());
		auto multi_use_blocks_size =
		    sizeof(uint64_t) + (sizeof(block_id_t) + sizeof(uint32_t)) * multi_use_blocks.size();
		auto metadata_blocks =
		    sizeof(uint64_t) + (sizeof(block_id_t) + sizeof(idx_t)) * GetMetadataManager().BlockCount();
		auto total_size = free_list_size + multi_use_blocks_size + metadata_blocks;
		if (total_size < allocated_size) {
			break;
		}
		auto free_list_handle = GetMetadataManager().AllocateHandle();
		free_list_blocks.push_back(std::move(free_list_handle));
		allocated_size += block_size;
	}

	return free_list_blocks;
}

class FreeListBlockWriter : public MetadataWriter {
public:
	FreeListBlockWriter(MetadataManager &manager, vector<MetadataHandle> free_list_blocks_p)
	    : MetadataWriter(manager), free_list_blocks(std::move(free_list_blocks_p)), index(0) {
	}

	vector<MetadataHandle> free_list_blocks;
	idx_t index;

protected:
	MetadataHandle NextHandle() override {
		if (index >= free_list_blocks.size()) {
			throw InternalException(
			    "Free List Block Writer ran out of blocks, this means not enough blocks were allocated up front");
		}
		return std::move(free_list_blocks[index++]);
	}
};

void SingleFileBlockManager::WriteHeader(QueryContext context, DatabaseHeader header) {
	auto free_list_blocks = GetFreeListBlocks();

	// now handle the free list
	auto &metadata_manager = GetMetadataManager();
	// add all modified blocks to the free list: they can now be written to again
	metadata_manager.MarkBlocksAsModified();

	lock_guard<mutex> lock(block_lock);
	// set the iteration count
	header.iteration = ++iteration_count;

	for (auto &block : modified_blocks) {
		free_list.insert(block);
		newly_freed_list.insert(block);
	}
	modified_blocks.clear();

	if (!free_list_blocks.empty()) {
		// there are blocks to write, either in the free_list or in the modified_blocks
		// we write these blocks specifically to the free_list_blocks
		// a normal MetadataWriter will fetch blocks to use from the free_list
		// but since we are WRITING the free_list, this behavior is sub-optimal
		FreeListBlockWriter writer(metadata_manager, std::move(free_list_blocks));

		auto ptr = writer.GetMetaBlockPointer();
		header.free_list = ptr.block_pointer;

		writer.Write<uint64_t>(free_list.size());
		for (auto &block_id : free_list) {
			writer.Write<block_id_t>(block_id);
		}
		writer.Write<uint64_t>(multi_use_blocks.size());
		for (auto &entry : multi_use_blocks) {
			writer.Write<block_id_t>(entry.first);
			writer.Write<uint32_t>(entry.second);
		}
		GetMetadataManager().Write(writer);
		writer.Flush();
	} else {
		// no blocks in the free list
		header.free_list = DConstants::INVALID_INDEX;
	}
	metadata_manager.Flush();
	header.block_count = NumericCast<idx_t>(max_block);
	header.serialization_compatibility = options.storage_version.GetIndex();

	auto debug_checkpoint_abort = DBConfig::GetSetting<DebugCheckpointAbortSetting>(db.GetDatabase());
	if (debug_checkpoint_abort == CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE) {
		throw FatalException("Checkpoint aborted after free list write because of PRAGMA checkpoint_abort flag");
	}

	// We need to fsync BEFORE we write the header to ensure that all the previous blocks are written as well
	handle->Sync();

	header_buffer.Clear();
	// if we are upgrading the database from version 64 -> version 65, we need to re-write the main header
	if (options.version_number.GetIndex() == 64 && options.storage_version.GetIndex() >= 4) {
		// rewrite the main header
		options.version_number = 65;
		MainHeader main_header = ConstructMainHeader(options.version_number.GetIndex());
		SerializeHeaderStructure<MainHeader>(main_header, header_buffer.buffer);
		// now write the header to the file
		ChecksumAndWrite(context, header_buffer, 0);
		header_buffer.Clear();
	}

	// set the header inside the buffer
	MemoryStream serializer(Allocator::Get(db));
	header.Write(serializer);
	memcpy(header_buffer.buffer, serializer.GetData(), serializer.GetPosition());
	// now write the header to the file, active_header determines whether we write to h1 or h2
	// note that if active_header is h1 we write to h2, and vice versa
	auto location = active_header == 1 ? Storage::FILE_HEADER_SIZE : Storage::FILE_HEADER_SIZE * 2;
	ChecksumAndWrite(context, header_buffer, location);
	// switch active header to the other header
	active_header = 1 - active_header;
	//! Ensure the header write ends up on disk
	handle->Sync();
	// Release the free blocks to the filesystem.
	TrimFreeBlocks();
}

void SingleFileBlockManager::FileSync() {
	handle->Sync();
}

void SingleFileBlockManager::TrimFreeBlocks() {
	if (DBConfig::Get(db).options.trim_free_blocks) {
		for (auto itr = newly_freed_list.begin(); itr != newly_freed_list.end(); ++itr) {
			block_id_t first = *itr;
			block_id_t last = first;
			// Find end of contiguous range.
			for (++itr; itr != newly_freed_list.end() && (*itr == last + 1); ++itr) {
				last = *itr;
			}
			// We are now one too far.
			--itr;
			// Trim the range.
			handle->Trim(BLOCK_START + (NumericCast<idx_t>(first) * GetBlockAllocSize()),
			             NumericCast<idx_t>(last + 1 - first) * GetBlockAllocSize());
		}
	}
	newly_freed_list.clear();
}

} // namespace duckdb
