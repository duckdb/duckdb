#include "parquet_crypto.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "mbedtls_wrapper.hpp"
#include "thrift/protocol/TCompactProtocol.h"

namespace duckdb {

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
	    : prot(prot_p), trans(*prot.getTransport()), aes(key), read_buffer_offset(0), read_buffer_size(0) {
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
			throw InvalidInputException("Computed AES tag differs from read AES tag");
		}

		if (transport_remaining != 0) {
			throw InvalidInputException("Encoded ciphertext length differs from actual ciphertext length");
		}

		return total_bytes;
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
	uint32_t read_buffer_offset;
	uint32_t read_buffer_size;

	//! Remaining bytes to read, set by Initialize(), decremented by ReadBlock()
	uint32_t total_bytes;
	uint32_t transport_remaining;
	//! Nonce read by Initialize()
	data_t nonce[ParquetCrypto::NONCE_BYTES];
};

uint32_t ParquetCrypto::Read(TBase &object, TProtocol &iprot, const string &key) {
	// Create decryption protocol
	TCompactProtocolFactoryT<DecryptionTransport> tproto_factory;
	auto dprot = tproto_factory.getProtocol(make_shared<DecryptionTransport>(iprot, key));
	auto &dtrans = reinterpret_cast<DecryptionTransport &>(*dprot->getTransport());

	// Read the object
	object.read(dprot.get());

	// Verify AES tag and read length
	return dtrans.Finalize();
}

uint32_t ParquetCrypto::Write(const TBase &object, TProtocol &oprot, const string &key) {
	// Create encryption protocol
	TCompactProtocolFactoryT<EncryptionTransport> tproto_factory;
	auto eprot = tproto_factory.getProtocol(make_shared<EncryptionTransport>(oprot, key));
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
	auto dprot = tproto_factory.getProtocol(make_shared<DecryptionTransport>(iprot, key));
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
	auto eprot = tproto_factory.getProtocol(make_shared<EncryptionTransport>(oprot, key));
	auto &etrans = reinterpret_cast<EncryptionTransport &>(*eprot->getTransport());

	// Write the data in memory
	etrans.write(buffer, buffer_size);

	// Encrypt and write to oprot
	return etrans.Finalize();
}

void ParquetCrypto::SetKey(ClientContext &context, const FunctionParameters &parameters) {
	auto &cache = ObjectCache::GetObjectCache(context);
	const auto &key = StringValue::Get(parameters.values[0]);
	if (!AESGCMState::ValidKey(key)) {
		throw InvalidInputException(
		    "Invalid AES key. Must have a length of 128, 192, or 256 bits (16, 24, or 32 bytes)");
	}
	cache.Put(ParquetKey::ObjectType(), make_shared<ParquetKey>(key));
}

bool ParquetCrypto::HasKey(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.Get<ParquetKey>(ParquetKey::ObjectType()).get();
}

string ParquetCrypto::GetKey(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	auto entry = cache.Get<ParquetKey>(ParquetKey::ObjectType());
	return entry ? entry->key : "";
}

} // namespace duckdb
