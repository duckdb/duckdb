#include "kafkafs.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/helper.hpp"

#include <iostream>
/**
 * @brief A fatal error has occurred, immediately exit the application.
 */
#define fatal(...)                                                                                                     \
	do {                                                                                                               \
		fprintf(stderr, "FATAL ERROR: ");                                                                              \
		fprintf(stderr, __VA_ARGS__);                                                                                  \
		fprintf(stderr, "\n");                                                                                         \
		exit(1);                                                                                                       \
	} while (0)

/**
 * @brief Same as fatal() but takes an rd_kafka_error_t object, prints its
 *        error message, destroys the object and then exits fatally.
 */
#define fatal_error(what, error)                                                                                       \
	do {                                                                                                               \
		fprintf(stderr, "FATAL ERROR: %s: %s: %s\n", what, rd_kafka_error_name(error), rd_kafka_error_string(error));  \
		rd_kafka_error_destroy(error);                                                                                 \
		exit(1);                                                                                                       \
	} while (0)

/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll(), rd_kafka_flush(),
 * rd_kafka_abort_transaction() and rd_kafka_commit_transaction() and
 * executes on the application's thread.
 *
 * The current transactional will enter the abortable state if any
 * message permanently fails delivery and the application must then
 * call rd_kafka_abort_transaction(). But it does not need to be done from
 * here, this state is checked by all the transactional APIs and it is better
 * to perform this error checking when calling
 * rd_kafka_send_offsets_to_transaction() and rd_kafka_commit_transaction().
 * In the case of transactional producing the delivery report callback is
 * mostly useful for logging the produce failures.
 */
static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
	if (rkmessage->err)
		fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));

	/* The rkmessage is destroyed automatically by librdkafka */
}
namespace duckdb {
KafkaFileHandle::KafkaFileHandle(FileSystem &fs, string bootstrap_server, string topic, uint8_t flags)
    : FileHandle(fs, bootstrap_server) {

	conf = rd_kafka_conf_new();
	char errstr[256];
	rd_kafka_error_t *error;

	if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_server.c_str(), errstr, sizeof(errstr)) !=
	    RD_KAFKA_CONF_OK)
		fatal("Failed to configure producer: %s", errstr);

	if (flags == FileFlags::FILE_FLAGS_READ) {
		if (rd_kafka_conf_set(conf, "group.id", "groupid", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
			fprintf(stderr, "%s\n", errstr);
			rd_kafka_conf_destroy(conf);
		}

		if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
			fprintf(stderr, "%s\n", errstr);
			rd_kafka_conf_destroy(conf);
		}

		rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
		if (!rk) {
			fprintf(stderr, "%s\n", errstr);
		}

		rd_kafka_poll_set_consumer(rk);

		const char *topics[1];
		topics[0] = topic.c_str();
		rd_kafka_topic_partition_list_t *subscription; /* Subscribed topics */
		subscription = rd_kafka_topic_partition_list_new(1);
		rd_kafka_topic_partition_list_add(subscription, topics[0],
		                                  /* the partition is ignored
		                                   * by subscribe() */
		                                  RD_KAFKA_PARTITION_UA);

		/* Subscribe to the list of topics */
		rd_kafka_resp_err_t err;
		err = rd_kafka_subscribe(rk, subscription);
		if (err) {
			rd_kafka_topic_partition_list_destroy(subscription);
			rd_kafka_destroy(rk);
		}
		rd_kafka_topic_partition_list_destroy(subscription);
	} else {
		string txn_id = "txn_id_";
		txn_id += topic;
		if (rd_kafka_conf_set(conf, "transactional.id", txn_id.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
			fatal("Failed to configure producer: %s", errstr);
		/* This callback will be called once per message to indicate
		 * final delivery status. */
		rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

		/* Create producer */
		rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
		if (!rk) {
			rd_kafka_conf_destroy(conf);
			fatal("Failed to create producer: %s", errstr);
		}

		/* Initialize transactions, this is only performed once
		 * per transactional producer to acquire its producer id, et.al. */
		error = rd_kafka_init_transactions(rk, -1);
		if (error)
			fatal_error("init_transactions()", error);
		else
			fprintf(stderr, "success init_kafka_txn\n");
	}
}

KafkaFileHandle::~KafkaFileHandle() = default;
bool KafkaFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind("kafka://", 0) == 0;
}

unique_ptr<FileHandle> KafkaFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                 FileCompressionType compression, FileOpener *opener) {
	D_ASSERT(compression == FileCompressionType::UNCOMPRESSED);
	if (path.rfind("kafka://", 0) != 0) {
		throw IOException("URL needs to start with kafka://");
	}
	auto slash_pos = path.find('/', 8);
	if (slash_pos == string::npos) {
		throw IOException("path needs to contain a '/' after the host");
	}

	bootstrap_server = path.substr(8, slash_pos);

	topic = path.substr(slash_pos + 1);

	auto writer_slash_pos = path.find('/', slash_pos + 1);

	is_writer = path.substr(writer_slash_pos + 1) == "writer";

	if (topic.empty()) {
		throw IOException("URL needs to contain a path");
	}

	auto handle = duckdb::make_uniq<KafkaFileHandle>(*this, bootstrap_server, topic, flags);
	// handle->Initialize(opener);
	return std::move(handle);
}

int64_t KafkaFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	if (!is_writer)
		return nr_bytes;

	std::cout << "Writing to topic " << topic << std::endl;
	rd_kafka_error_t *error;
	rd_kafka_resp_err_t err;

	rd_kafka_t *producer = ((KafkaFileHandle *)(&handle))->rk;
	char key[512];

	strcpy(key, "hello");
	/* Begin transaction and start waiting for messages */
	error = rd_kafka_begin_transaction(producer);

	if (error) {
	}

	err = rd_kafka_producev(
	    /* Producer handle */
	    producer,
	    /* Topic name */
	    (const char *)topic.c_str(), (void *)key, (size_t)5,
	    /* Value is the current sum of this
	     * transaction. */
	    /* Make a copy of the payload. */
	    (int)RD_KAFKA_MSG_F_COPY,
	    /* Message value and length */
	    (void *)buffer, (size_t)nr_bytes,
	    /* End sentinel */
	    RD_KAFKA_V_END);
	if (err) {
	}

	error = rd_kafka_commit_transaction(producer, -1);

	if (error) {

	} else {
		fprintf(stderr, "success\n");
	}
	return nr_bytes;
}

int64_t KafkaFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	rd_kafka_message_t *rkm;
	rd_kafka_t *rk = ((KafkaFileHandle *)(&handle))->rk;
	rkm = rd_kafka_consumer_poll(rk, 1000);
	if (!rkm)
		return 0;

	/* consumer_poll() will return either a proper message
	 * or a consumer error (rkm->err is set). */
	if (rkm->err) {
		/* Consumer errors are generally to be considered
		 * informational as the consumer will automatically
		 * try to recover from all types of errors. */
		//  fprintf(stderr, "%% Consumer error: %s\n",
		//      rd_kafka_message_errstr(rkm));
		rd_kafka_message_destroy(rkm);
		return 0;
	}

	/* Proper message. */
	memcpy(buffer, rkm->payload, rkm->len);
	fprintf(stderr, "success-read\n");
	int64_t ret = rkm->len;

	rd_kafka_message_destroy(rkm);

	return ret;
}
void KafkaFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
}
void KafkaFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
}
void KafkaFileSystem::FileSync(FileHandle &handle) {
}
int64_t KafkaFileSystem::GetFileSize(FileHandle &handle) {
	return -1;
}
void KafkaFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
}
} // namespace duckdb
