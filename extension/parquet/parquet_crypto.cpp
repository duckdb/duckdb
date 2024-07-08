#include "parquet_crypto.hpp"

#include "mbedtls_wrapper.hpp"
#include "thrift_tools.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#endif

namespace duckdb {

ParquetKeys &ParquetKeys::Get(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	if (!cache.Get<ParquetKeys>(ParquetKeys::ObjectType())) {
		cache.Put(ParquetKeys::ObjectType(), make_shared_ptr<ParquetKeys>());
	}
	return *cache.Get<ParquetKeys>(ParquetKeys::ObjectType());
}

void ParquetKeys::AddKey(const string &key_name, const string &key) {
	keys[key_name] = key;
}

bool ParquetKeys::HasKey(const string &key_name) const {
	return keys.find(key_name) != keys.end();
}

const string &ParquetKeys::GetKey(const string &key_name) const {
	D_ASSERT(HasKey(key_name));
	return keys.at(key_name);
}

string ParquetKeys::ObjectType() {
	return "parquet_keys";
}

string ParquetKeys::GetObjectType() {
	return ObjectType();
}

ParquetEncryptionConfig::ParquetEncryptionConfig(ClientContext &context_p) : context(context_p) {
}

ParquetEncryptionConfig::ParquetEncryptionConfig(ClientContext &context_p, const Value &arg)
    : ParquetEncryptionConfig(context_p) {
	if (arg.type().id() != LogicalTypeId::STRUCT) {
		throw BinderException("Parquet encryption_config must be of type STRUCT");
	}
	const auto &child_types = StructType::GetChildTypes(arg.type());
	auto &children = StructValue::GetChildren(arg);
	const auto &keys = ParquetKeys::Get(context);
	for (idx_t i = 0; i < StructType::GetChildCount(arg.type()); i++) {
		auto &struct_key = child_types[i].first;
		if (StringUtil::Lower(struct_key) == "footer_key") {
			const auto footer_key_name = StringValue::Get(children[i].DefaultCastAs(LogicalType::VARCHAR));
			if (!keys.HasKey(footer_key_name)) {
				throw BinderException(
				    "No key with name \"%s\" exists. Add it with PRAGMA add_parquet_key('<key_name>','<key>');",
				    footer_key_name);
			}
			footer_key = footer_key_name;
		} else if (StringUtil::Lower(struct_key) == "column_keys") {
			throw NotImplementedException("Parquet encryption_config column_keys not yet implemented");
		} else {
			throw BinderException("Unknown key in encryption_config \"%s\"", struct_key);
		}
	}
}

shared_ptr<ParquetEncryptionConfig> ParquetEncryptionConfig::Create(ClientContext &context, const Value &arg) {
	return shared_ptr<ParquetEncryptionConfig>(new ParquetEncryptionConfig(context, arg));
}

const string &ParquetEncryptionConfig::GetFooterKey() const {
	const auto &keys = ParquetKeys::Get(context);
	D_ASSERT(!footer_key.empty());
	D_ASSERT(keys.HasKey(footer_key));
	return keys.GetKey(footer_key);
}

using duckdb_apache::thrift::transport::TTransport;
using AESGCMState = duckdb_mbedtls::MbedTlsWrapper::AESGCMState;
using duckdb_apache::thrift::protocol::TCompactProtocolFactoryT;

static void GenerateNonce(const data_ptr_t nonce) {
	duckdb_mbedtls::MbedTlsWrapper::GenerateRandomData(nonce, ParquetCrypto::NONCE_BYTES);
}

//! Encryption wrapper for a transport protocol
class EncryptionTransport : public TTransport {
public:
	EncryptionTransport(TProtocol &prot_p, const string &key)
	    : prot(prot_p), trans(*prot.getTransport()), aes(key),
	      allocator(Allocator::DefaultAllocator(), ParquetCrypto::CRYPTO_BLOCK_SIZE) {
		Initialize();
	}

	bool isOpen() const override {
		return trans.isOpen();
	}

	void open() override {
		trans.open();
	}

	void close() override {
		trans.close();
	}

	void write_virt(const uint8_t *buf, uint32_t len) override {
		memcpy(allocator.Allocate(len), buf, len);
	}

	uint32_t Finalize() {
		// Write length
		const auto ciphertext_length = allocator.SizeInBytes();
		const uint32_t total_length = ParquetCrypto::NONCE_BYTES + ciphertext_length + ParquetCrypto::TAG_BYTES;
		trans.write(const_data_ptr_cast(&total_length), ParquetCrypto::LENGTH_BYTES);

		// Write nonce
		trans.write(nonce, ParquetCrypto::NONCE_BYTES);

		// Encrypt and write data
		data_t aes_buffer[ParquetCrypto::CRYPTO_BLOCK_SIZE];
		auto current = allocator.GetTail();
		while (current != nullptr) {
			for (idx_t pos = 0; pos < current->current_position; pos += ParquetCrypto::CRYPTO_BLOCK_SIZE) {
				auto next = MinValue<idx_t>(current->current_position - pos, ParquetCrypto::CRYPTO_BLOCK_SIZE);
				auto write_size =
				    aes.Process(current->data.get() + pos, next, aes_buffer, ParquetCrypto::CRYPTO_BLOCK_SIZE);
				trans.write(aes_buffer, write_size);
			}
			current = current->prev;
		}

		// Finalize the last encrypted data and write tag
		data_t tag[ParquetCrypto::TAG_BYTES];
		auto write_size = aes.Finalize(aes_buffer, ParquetCrypto::CRYPTO_BLOCK_SIZE, tag, ParquetCrypto::TAG_BYTES);
		trans.write(aes_buffer, write_size);
		trans.write(tag, ParquetCrypto::TAG_BYTES);

		return ParquetCrypto::LENGTH_BYTES + total_length;
	}

private:
	void Initialize() {
		// Generate nonce and initialize AES
		GenerateNonce(nonce);
		aes.InitializeEncryption(nonce, ParquetCrypto::NONCE_BYTES);
	}

private:
	//! Protocol and corresponding transport that we're wrapping
	TProtocol &prot;
	TTransport &trans;

	//! AES context
	AESGCMState aes;

	//! Nonce created by Initialize()
	data_t nonce[ParquetCrypto::NONCE_BYTES];

	//! Arena Allocator to fully materialize in memory before encrypting
	ArenaAllocator allocator;
};

//! Decryption wrapper for a transport protocol
class DecryptionTransport : public TTransport {
public:
	DecryptionTransport(TProtocol &prot_p, const string &key)
	    : prot(prot_p), trans(*prot.getTransport()), aes(key), read_buffer_size(0), read_buffer_offset(0) {
		Initialize();
	}

	uint32_t read_virt(uint8_t *buf, uint32_t len) override {
		const uint32_t result = len;

		if (len > transport_remaining - ParquetCrypto::TAG_BYTES + read_buffer_size - read_buffer_offset) {
			throw InvalidInputException("Too many bytes requested from crypto buffer");
		}

		while (len != 0) {
			if (read_buffer_offset == read_buffer_size) {
				ReadBlock();
			}
			const auto next = MinValue(read_buffer_size - read_buffer_offset, len);
			memcpy(buf, read_buffer + read_buffer_offset, next);
			read_buffer_offset += next;
			buf += next;
			len -= next;
		}

		return result;
	}

	uint32_t Finalize() {
		if (read_buffer_offset != read_buffer_size) {
			throw InternalException("DecryptionTransport::Finalize was called with bytes remaining in read buffer");
		}

		data_t computed_tag[ParquetCrypto::TAG_BYTES];
		if (aes.Finalize(read_buffer, AESGCMState::BLOCK_SIZE, computed_tag, ParquetCrypto::TAG_BYTES) != 0) {
			throw InternalException("DecryptionTransport::Finalize was called with bytes remaining in AES context");
		}

		data_t read_tag[ParquetCrypto::TAG_BYTES];
		transport_remaining -= trans.read(read_tag, ParquetCrypto::TAG_BYTES);
		if (memcmp(computed_tag, read_tag, ParquetCrypto::TAG_BYTES) != 0) {
			throw InvalidInputException("Computed AES tag differs from read AES tag, are you using the right key?");
		}

		if (transport_remaining != 0) {
			throw InvalidInputException("Encoded ciphertext length differs from actual ciphertext length");
		}

		return ParquetCrypto::LENGTH_BYTES + total_bytes;
	}

	AllocatedData ReadAll() {
		D_ASSERT(transport_remaining == total_bytes - ParquetCrypto::NONCE_BYTES);
		auto result = Allocator::DefaultAllocator().Allocate(transport_remaining - ParquetCrypto::TAG_BYTES);
		read_virt(result.get(), transport_remaining - ParquetCrypto::TAG_BYTES);
		Finalize();
		return result;
	}

private:
	void Initialize() {
		// Read encoded length (don't add to read_bytes)
		data_t length_buf[ParquetCrypto::LENGTH_BYTES];
		trans.read(length_buf, ParquetCrypto::LENGTH_BYTES);
		total_bytes = Load<uint32_t>(length_buf);
		transport_remaining = total_bytes;

		// Read nonce and initialize AES
		transport_remaining -= trans.read(nonce, ParquetCrypto::NONCE_BYTES);
		aes.InitializeDecryption(nonce, ParquetCrypto::NONCE_BYTES);
	}

	void ReadBlock() {
		// Read from transport into read_buffer at one AES block size offset (up to the tag)
		read_buffer_size = MinValue(ParquetCrypto::CRYPTO_BLOCK_SIZE, transport_remaining - ParquetCrypto::TAG_BYTES);
		transport_remaining -= trans.read(read_buffer + AESGCMState::BLOCK_SIZE, read_buffer_size);

		// Decrypt from read_buffer + block size into read_buffer start (decryption can trail behind in same buffer)
#ifdef DEBUG
		auto size = aes.Process(read_buffer + AESGCMState::BLOCK_SIZE, read_buffer_size, read_buffer,
		                        ParquetCrypto::CRYPTO_BLOCK_SIZE + AESGCMState::BLOCK_SIZE);
		D_ASSERT(size == read_buffer_size);
#else
		aes.Process(read_buffer + AESGCMState::BLOCK_SIZE, read_buffer_size, read_buffer,
		            ParquetCrypto::CRYPTO_BLOCK_SIZE + AESGCMState::BLOCK_SIZE);
#endif
		read_buffer_offset = 0;
	}

private:
	//! Protocol and corresponding transport that we're wrapping
	TProtocol &prot;
	TTransport &trans;

	//! AES context and buffers
	AESGCMState aes;

	//! We read/decrypt big blocks at a time
	data_t read_buffer[ParquetCrypto::CRYPTO_BLOCK_SIZE + AESGCMState::BLOCK_SIZE];
	uint32_t read_buffer_size;
	uint32_t read_buffer_offset;

	//! Remaining bytes to read, set by Initialize(), decremented by ReadBlock()
	uint32_t total_bytes;
	uint32_t transport_remaining;
	//! Nonce read by Initialize()
	data_t nonce[ParquetCrypto::NONCE_BYTES];
};

class SimpleReadTransport : public TTransport {
public:
	explicit SimpleReadTransport(data_ptr_t read_buffer_p, uint32_t read_buffer_size_p)
	    : read_buffer(read_buffer_p), read_buffer_size(read_buffer_size_p), read_buffer_offset(0) {
	}

	uint32_t read_virt(uint8_t *buf, uint32_t len) override {
		const auto remaining = read_buffer_size - read_buffer_offset;
		if (len > remaining) {
			return remaining;
		}
		memcpy(buf, read_buffer + read_buffer_offset, len);
		read_buffer_offset += len;
		return len;
	}

private:
	const data_ptr_t read_buffer;
	const uint32_t read_buffer_size;
	uint32_t read_buffer_offset;
};

uint32_t ParquetCrypto::Read(TBase &object, TProtocol &iprot, const string &key) {
	// Create decryption protocol
	TCompactProtocolFactoryT<DecryptionTransport> tproto_factory;
	auto dprot = tproto_factory.getProtocol(std::make_shared<DecryptionTransport>(iprot, key));
	auto &dtrans = reinterpret_cast<DecryptionTransport &>(*dprot->getTransport());

	// We have to read the whole thing otherwise thrift throws an error before we realize we're decryption is wrong
	auto all = dtrans.ReadAll();
	TCompactProtocolFactoryT<SimpleReadTransport> tsimple_proto_factory;
	auto simple_prot =
	    tsimple_proto_factory.getProtocol(std::make_shared<SimpleReadTransport>(all.get(), all.GetSize()));

	// Read the object
	object.read(simple_prot.get());

	return ParquetCrypto::LENGTH_BYTES + ParquetCrypto::NONCE_BYTES + all.GetSize() + ParquetCrypto::TAG_BYTES;
}

uint32_t ParquetCrypto::Write(const TBase &object, TProtocol &oprot, const string &key) {
	// Create encryption protocol
	TCompactProtocolFactoryT<EncryptionTransport> tproto_factory;
	auto eprot = tproto_factory.getProtocol(std::make_shared<EncryptionTransport>(oprot, key));
	auto &etrans = reinterpret_cast<EncryptionTransport &>(*eprot->getTransport());

	// Write the object in memory
	object.write(eprot.get());

	// Encrypt and write to oprot
	return etrans.Finalize();
}

uint32_t ParquetCrypto::ReadData(TProtocol &iprot, const data_ptr_t buffer, const uint32_t buffer_size,
                                 const string &key) {
	// Create decryption protocol
	TCompactProtocolFactoryT<DecryptionTransport> tproto_factory;
	auto dprot = tproto_factory.getProtocol(std::make_shared<DecryptionTransport>(iprot, key));
	auto &dtrans = reinterpret_cast<DecryptionTransport &>(*dprot->getTransport());

	// Read buffer
	dtrans.read(buffer, buffer_size);

	// Verify AES tag and read length
	return dtrans.Finalize();
}

uint32_t ParquetCrypto::WriteData(TProtocol &oprot, const const_data_ptr_t buffer, const uint32_t buffer_size,
                                  const string &key) {
	// FIXME: we know the size upfront so we could do a streaming write instead of this
	// Create encryption protocol
	TCompactProtocolFactoryT<EncryptionTransport> tproto_factory;
	auto eprot = tproto_factory.getProtocol(std::make_shared<EncryptionTransport>(oprot, key));
	auto &etrans = reinterpret_cast<EncryptionTransport &>(*eprot->getTransport());

	// Write the data in memory
	etrans.write(buffer, buffer_size);

	// Encrypt and write to oprot
	return etrans.Finalize();
}

string Base64Decode(const string &key) {
	auto result_size = Blob::FromBase64Size(key);
	auto output = duckdb::unique_ptr<unsigned char[]>(new unsigned char[result_size]);
	Blob::FromBase64(key, output.get(), result_size);
	string decoded_key(reinterpret_cast<const char *>(output.get()), result_size);
	return decoded_key;
}

void ParquetCrypto::AddKey(ClientContext &context, const FunctionParameters &parameters) {
	const auto &key_name = StringValue::Get(parameters.values[0]);
	const auto &key = StringValue::Get(parameters.values[1]);

	auto &keys = ParquetKeys::Get(context);
	if (AESGCMState::ValidKey(key)) {
		keys.AddKey(key_name, key);
	} else {
		string decoded_key;
		try {
			decoded_key = Base64Decode(key);
		} catch (const ConversionException &e) {
			throw InvalidInputException("Invalid AES key. Not a plain AES key NOR a base64 encoded string");
		}
		if (!AESGCMState::ValidKey(decoded_key)) {
			throw InvalidInputException(
			    "Invalid AES key. Must have a length of 128, 192, or 256 bits (16, 24, or 32 bytes)");
		}
		keys.AddKey(key_name, decoded_key);
	}
}

} // namespace duckdb
