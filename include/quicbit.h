#ifndef QUICBIT_H
#define QUICBIT_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CHUNK_FRAME_FLAG 2147483648

#define CHUNK_FRAME_LEN_MASK 2147483647

#define CHUNK_HEADER_LEN ((((8 + 4) + 4) + 8) + 4)

/**
 * Maximum byte length for a topic name. The local SHM backend uses the same
 * cap for direct services and for composed `<identity>__<topic>` names.
 */
#define MAX_TOPIC_BYTES 200

/**
 * ASCII "QBR1" little-endian — pub/sub stream identifier.
 */
#define HANDSHAKE_MAGIC 827474513

/**
 * ASCII "QBR2" little-endian — req/res stream identifier.
 */
#define REQRESP_MAGIC 844251729

/**
 * ASCII "QBA1" little-endian — que/ans stream identifier.
 */
#define QUEANS_MAGIC 826360401

/**
 * ASCII "QBP1" little-endian — put/ack stream identifier.
 */
#define PUTACK_MAGIC 827343441

/**
 * ASCII "QBI1" little-endian — pip stream identifier.
 */
#define PIP_MAGIC 826884689

/**
 * Stream protocol version. Bump on any breaking wire change.
 *
 * History:
 * * `1` — type identity hashed from `std::any::type_name::<T>()`.
 *   Unstable across rustc versions; retired.
 * * `2` — type identity hashed from `(size_of::<T>(), align_of::<T>())`
 *   via [`crate::transport::wire_type_hash`]. Stable across
 *   toolchains; coarser (size+align collisions possible).
 */
#define HANDSHAKE_VERSION 2

/**
 * Pub/sub handshake version carrying data-agnostic topic QoS.
 *
 * This is only for `QBR1` pub/sub streams. Other stream families
 * continue using [`HANDSHAKE_VERSION`] until their own wire shape
 * changes.
 */
#define PUBSUB_HANDSHAKE_VERSION_QOS 3

/**
 * Item-stream handshake version carrying data-agnostic byte limits
 * (`max_message_bytes`, `max_inflight_bytes`, `chunk_bytes`) for the
 * req/res, que/ans, put/ack, and pip families. Appended to the v2 tail
 * before `topic_len`. Version 2 (legacy, single-frame, 64 MiB cap) is
 * still accepted on the read path. Enables chunked large messages.
 */
#define ITEM_HANDSHAKE_VERSION_CHUNKED 3

/**
 * Maximum topic name length on the wire.
 */
#define MAX_TOPIC_LEN 1024

/**
 * Maximum single-message payload (64 MiB). Cap exists to defend
 * against a malicious peer asking us to allocate huge buffers while
 * still allowing the 4K RGBA video demo (~32 MiB/frame) over iroh.
 */
#define MAX_PAYLOAD_LEN ((64 * 1024) * 1024)

/**
 * Delivery behavior for C QoS.
 */
typedef enum {
  QUICBIT_DELIVERY_POLICY_QUICBIT_DELIVERY_POLICY_RELIABLE = 0,
  QUICBIT_DELIVERY_POLICY_QUICBIT_DELIVERY_POLICY_LATEST = 1,
  QUICBIT_DELIVERY_POLICY_QUICBIT_DELIVERY_POLICY_BEST_EFFORT = 2,
} QuicbitDeliveryPolicy;

typedef struct QuicbitAckServer QuicbitAckServer;

/**
 * Passed to a que/ans handler so it can append answer items.
 */
typedef struct QuicbitAnsResponder QuicbitAnsResponder;

typedef struct QuicbitAnsServer QuicbitAnsServer;

typedef struct QuicbitDatapodPublisher QuicbitDatapodPublisher;

typedef struct QuicbitDatapodSample QuicbitDatapodSample;

typedef struct QuicbitDatapodSubscriber QuicbitDatapodSubscriber;

/**
 * An owned, received message (kind tag + payload bytes).
 */
typedef struct QuicbitMessage QuicbitMessage;

/**
 * Passed to put/ack or pip handlers so they can build the final reply list.
 */
typedef struct QuicbitMessageResponder QuicbitMessageResponder;

/**
 * Owned message list returned by que/ans and pip convenience calls.
 */
typedef struct QuicbitMessages QuicbitMessages;

typedef struct QuicbitNode QuicbitNode;

typedef struct QuicbitPendingPip QuicbitPendingPip;

typedef struct QuicbitPendingQue QuicbitPendingQue;

typedef struct QuicbitPendingReq QuicbitPendingReq;

typedef struct QuicbitPip QuicbitPip;

typedef struct QuicbitPipClient QuicbitPipClient;

typedef struct QuicbitPipServer QuicbitPipServer;

typedef struct QuicbitPublisher QuicbitPublisher;

typedef struct QuicbitPutClient QuicbitPutClient;

typedef struct QuicbitPutUpload QuicbitPutUpload;

typedef struct QuicbitPuts QuicbitPuts;

typedef struct QuicbitQueClient QuicbitQueClient;

typedef struct QuicbitReqClient QuicbitReqClient;

typedef struct QuicbitReqServer QuicbitReqServer;

/**
 * Passed to a request handler so it can set the response.
 */
typedef struct QuicbitResponder QuicbitResponder;

typedef struct QuicbitSample QuicbitSample;

typedef struct QuicbitSubscriber QuicbitSubscriber;

/**
 * Optional node construction settings. NULL string pointers mean "unset";
 * zero numeric limits mean "use the Rust default".
 */
typedef struct {
  const char *identity;
  bool no_relay;
  const char *system_did;
  uintptr_t max_payload_bytes;
  uint32_t history_depth;
  uint32_t subscriber_buffer;
} QuicbitNodeConfig;

typedef struct {
  uintptr_t publisher_topics;
  uintptr_t cached_peers;
} QuicbitNodeStats;

/**
 * C mirror of [`TopicQos`]. Zero fields are allowed; use
 * [`quicbit_topic_qos_reliable`], [`quicbit_topic_qos_latest`], or
 * [`quicbit_topic_qos_best_effort`] for canonical defaults.
 */
typedef struct {
  QuicbitDeliveryPolicy delivery;
  uintptr_t max_message_bytes;
  uintptr_t max_inflight_bytes;
  uintptr_t chunk_bytes;
  uintptr_t subscriber_queue;
  uint8_t priority;
} QuicbitTopicQos;

typedef struct {
  uint64_t published;
  uint64_t remote_dropped;
  uint64_t stale_dropped;
  uint64_t bytes_sent;
  uint64_t send_errors;
} QuicbitPublisherStats;

/**
 * A borrowed view of contiguous bytes owned by a handle.
 */
typedef struct {
  const uint8_t *ptr;
  uintptr_t len;
} QuicbitBytes;

typedef struct {
  uint64_t received;
  uint64_t disconnects;
  uint64_t stale_dropped;
  uint64_t incomplete_dropped;
  uint64_t bytes_received;
} QuicbitSubscriberStats;

typedef struct {
  uint64_t messages_out;
  uint64_t messages_in;
  uint64_t bytes_out;
  uint64_t bytes_in;
  uint64_t errors;
} QuicbitItemStats;

/**
 * Request handler callback: receives the request `kind` + bytes and the
 * `responder` to fill via [`quicbit_responder_set`].
 */
typedef void (*QuicbitReqHandler)(void *ctx,
                                  uint64_t kind,
                                  const uint8_t *data,
                                  uintptr_t len,
                                  QuicbitResponder *responder);

typedef void (*QuicbitAnsHandler)(void *ctx,
                                  uint64_t kind,
                                  const uint8_t *data,
                                  uintptr_t len,
                                  QuicbitAnsResponder *responder);

/**
 * Borrowed message input used by finite C convenience APIs.
 */
typedef struct {
  uint64_t kind;
  QuicbitBytes data;
} QuicbitRawMessage;

typedef void (*QuicbitAckHandler)(void *ctx,
                                  const QuicbitMessages *items,
                                  QuicbitResponder *responder);

typedef void (*QuicbitPipHandler)(void *ctx,
                                  const QuicbitMessages *items,
                                  QuicbitMessageResponder *responder);

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Returns the last error message on this thread, or NULL if the most
 * recent call succeeded. The pointer is valid until the next quicbit
 * call on the same thread.
 */
const char *quicbit_last_error_message(void);

/**
 * Create a node. `identity` may be NULL for an ephemeral key. Returns
 * NULL on failure (see [`quicbit_last_error_message`]).
 */
QuicbitNode *quicbit_node_new(const char *identity, bool no_relay);

QuicbitNodeConfig quicbit_node_config_default(void);

QuicbitNode *quicbit_node_new_with_config(QuicbitNodeConfig cfg);

void quicbit_node_free(QuicbitNode *node);

/**
 * This node's identity as a `did:key:z6Mk…` string. Caller owns the
 * returned C string and must free it with [`quicbit_string_free`].
 * Returns NULL on failure.
 */
char *quicbit_node_did_key(const QuicbitNode *node);

char *quicbit_node_endpoint_addr(const QuicbitNode *node);

bool quicbit_node_add_topic_route(const QuicbitNode *node,
                                  const char *topic,
                                  const char *endpoint_addr);

bool quicbit_node_add_system_peer(const QuicbitNode *node, const char *endpoint_addr);

QuicbitNodeStats quicbit_node_stats(const QuicbitNode *node);

/**
 * Free a string returned by quicbit (e.g. [`quicbit_node_did_key`]).
 */
void quicbit_string_free(char *s);

QuicbitTopicQos quicbit_topic_qos_reliable(void);

QuicbitTopicQos quicbit_topic_qos_latest(void);

QuicbitTopicQos quicbit_topic_qos_best_effort(void);

QuicbitPublisher *quicbit_publisher_new(const QuicbitNode *node, const char *topic);

QuicbitPublisher *quicbit_publisher_new_with_qos(const QuicbitNode *node,
                                                 const char *topic,
                                                 QuicbitTopicQos qos);

void quicbit_publisher_free(QuicbitPublisher *publisher);

QuicbitPublisherStats quicbit_publisher_stats(const QuicbitPublisher *publisher);

/**
 * Publish `data` (`len` bytes) with user tag `kind`. Returns false on
 * failure.
 */
bool quicbit_publisher_send(QuicbitPublisher *publisher,
                            uint64_t kind,
                            const uint8_t *data,
                            uintptr_t len);

QuicbitSubscriber *quicbit_subscriber_new(const QuicbitNode *node,
                                          const char *peer,
                                          const char *topic);

QuicbitSubscriber *quicbit_subscriber_new_with_qos(const QuicbitNode *node,
                                                   const char *peer,
                                                   const char *topic,
                                                   QuicbitTopicQos qos);

QuicbitSubscriber *quicbit_subscribe_new_with_qos(const QuicbitNode *node,
                                                  const char *topic,
                                                  QuicbitTopicQos qos);

void quicbit_subscriber_free(QuicbitSubscriber *subscriber);

QuicbitDatapodPublisher *quicbit_datapod_publisher_new_with_qos(const QuicbitNode *node,
                                                                const char *topic,
                                                                QuicbitTopicQos qos);

void quicbit_datapod_publisher_free(QuicbitDatapodPublisher *publisher);

/**
 * Publish a datapod wire message: `type_hash` plus `header || payload` bytes.
 */
bool quicbit_datapod_publisher_send(QuicbitDatapodPublisher *publisher,
                                    uint64_t type_hash,
                                    const uint8_t *wire,
                                    uintptr_t len);

QuicbitDatapodSubscriber *quicbit_datapod_subscriber_new_with_qos(const QuicbitNode *node,
                                                                  const char *peer,
                                                                  const char *topic,
                                                                  QuicbitTopicQos qos);

void quicbit_datapod_subscriber_free(QuicbitDatapodSubscriber *subscriber);

/**
 * Poll for a datapod sample without copying the wire bytes.
 */
int32_t quicbit_datapod_subscriber_take_sample(QuicbitDatapodSubscriber *subscriber,
                                               QuicbitDatapodSample **out_sample);

uint64_t quicbit_datapod_sample_type_hash(const QuicbitDatapodSample *sample);

/**
 * Borrowed zero-copy view of datapod `header || payload` wire bytes.
 */
QuicbitBytes quicbit_datapod_sample_wire(const QuicbitDatapodSample *sample);

void quicbit_datapod_sample_free(QuicbitDatapodSample *sample);

QuicbitSubscriberStats quicbit_subscriber_stats(const QuicbitSubscriber *subscriber);

/**
 * Poll for the next sample. Returns `1` and writes an owned message to
 * `*out_message` when one is available, `0` when none is ready, and
 * `-1` on error. A returned message must be freed with
 * [`quicbit_message_free`].
 */
int32_t quicbit_subscriber_take(QuicbitSubscriber *subscriber, QuicbitMessage **out_message);

/**
 * Poll for the next sample without copying payload bytes.
 *
 * Returns `1` and writes a borrowed sample handle to `*out_sample` when one is
 * available, `0` when none is ready, and `-1` on error. A returned sample must
 * be freed with [`quicbit_sample_free`]. The byte view returned from
 * [`quicbit_sample_data`] is valid until that free call.
 */
int32_t quicbit_subscriber_take_sample(QuicbitSubscriber *subscriber, QuicbitSample **out_sample);

uint64_t quicbit_sample_kind(const QuicbitSample *sample);

/**
 * Borrowed zero-copy view of a sample payload.
 *
 * For local SHM this points directly into the shared-memory slot and pins that
 * slot until [`quicbit_sample_free`] is called. Copy it if you need to keep the
 * data longer.
 */
QuicbitBytes quicbit_sample_data(const QuicbitSample *sample);

void quicbit_sample_free(QuicbitSample *sample);

QuicbitMessage *quicbit_message_new(uint64_t kind, const uint8_t *data, uintptr_t len);

uint64_t quicbit_message_kind(const QuicbitMessage *message);

/**
 * Borrowed view of the message payload, valid until the message is
 * freed.
 */
QuicbitBytes quicbit_message_data(const QuicbitMessage *message);

void quicbit_message_free(QuicbitMessage *message);

QuicbitReqClient *quicbit_req_client_new(const QuicbitNode *node,
                                         const char *peer,
                                         const char *topic);

QuicbitReqClient *quicbit_req_client_new_with_qos(const QuicbitNode *node,
                                                  const char *peer,
                                                  const char *topic,
                                                  QuicbitTopicQos qos);

QuicbitReqClient *quicbit_req_system_client_new_with_qos(const QuicbitNode *node,
                                                         const char *topic,
                                                         QuicbitTopicQos qos);

void quicbit_req_client_free(QuicbitReqClient *client);

QuicbitItemStats quicbit_req_client_stats(const QuicbitReqClient *client);

/**
 * Send a request and block for the response. Returns false on failure;
 * on success writes an owned response message to `*out_message` (free
 * with [`quicbit_message_free`]).
 */
bool quicbit_req_client_call(QuicbitReqClient *client,
                             uint64_t kind,
                             const uint8_t *data,
                             uintptr_t len,
                             QuicbitMessage **out_message);

QuicbitReqServer *quicbit_req_server_new(const QuicbitNode *node, const char *topic);

QuicbitReqServer *quicbit_req_server_new_with_qos(const QuicbitNode *node,
                                                  const char *topic,
                                                  QuicbitTopicQos qos);

void quicbit_req_server_free(QuicbitReqServer *server);

QuicbitItemStats quicbit_req_server_stats(const QuicbitReqServer *server);

/**
 * Set the response on a responder passed to a request handler. Copies
 * `data` immediately; safe to call once per handler invocation.
 */
void quicbit_responder_set(QuicbitResponder *responder,
                           uint64_t kind,
                           const uint8_t *data,
                           uintptr_t len);

/**
 * Serve at most one request, waiting up to `timeout_ms`. Invokes
 * `handler` with the request and a responder; whatever the handler sets
 * (via [`quicbit_responder_set`]) is sent back. Returns `1` if a request
 * was served, `0` on timeout, `-1` on error.
 */
int32_t quicbit_req_server_serve_one(QuicbitReqServer *server,
                                     uint64_t timeout_ms,
                                     QuicbitReqHandler handler,
                                     void *ctx);

int32_t quicbit_req_server_take(QuicbitReqServer *server,
                                uint64_t timeout_ms,
                                QuicbitPendingReq **out_pending);

const QuicbitMessage *quicbit_pending_req_request(const QuicbitPendingReq *pending);

bool quicbit_pending_req_reply(QuicbitPendingReq *pending,
                               uint64_t kind,
                               const uint8_t *data,
                               uintptr_t len);

void quicbit_pending_req_free(QuicbitPendingReq *pending);

uintptr_t quicbit_messages_len(const QuicbitMessages *messages);

uint64_t quicbit_messages_kind_at(const QuicbitMessages *messages, uintptr_t index);

QuicbitBytes quicbit_messages_data_at(const QuicbitMessages *messages, uintptr_t index);

void quicbit_messages_free(QuicbitMessages *messages);

QuicbitQueClient *quicbit_que_client_new_with_qos(const QuicbitNode *node,
                                                  const char *peer,
                                                  const char *topic,
                                                  QuicbitTopicQos qos);

QuicbitQueClient *quicbit_que_client_new(const QuicbitNode *node,
                                         const char *peer,
                                         const char *topic);

QuicbitQueClient *quicbit_que_system_client_new_with_qos(const QuicbitNode *node,
                                                         const char *topic,
                                                         QuicbitTopicQos qos);

void quicbit_que_client_free(QuicbitQueClient *client);

QuicbitItemStats quicbit_que_client_stats(const QuicbitQueClient *client);

bool quicbit_que_client_send(QuicbitQueClient *client,
                             uint64_t kind,
                             const uint8_t *data,
                             uintptr_t len,
                             QuicbitMessages **out_messages);

QuicbitAnsServer *quicbit_ans_server_new_with_qos(const QuicbitNode *node,
                                                  const char *topic,
                                                  QuicbitTopicQos qos);

QuicbitAnsServer *quicbit_ans_server_new(const QuicbitNode *node, const char *topic);

void quicbit_ans_server_free(QuicbitAnsServer *server);

QuicbitItemStats quicbit_ans_server_stats(const QuicbitAnsServer *server);

void quicbit_ans_responder_send(QuicbitAnsResponder *responder,
                                uint64_t kind,
                                const uint8_t *data,
                                uintptr_t len);

int32_t quicbit_ans_server_serve_one(QuicbitAnsServer *server,
                                     uint64_t timeout_ms,
                                     QuicbitAnsHandler handler,
                                     void *ctx);

int32_t quicbit_ans_server_take(QuicbitAnsServer *server,
                                uint64_t timeout_ms,
                                QuicbitPendingQue **out_pending);

const QuicbitMessage *quicbit_pending_que_request(const QuicbitPendingQue *pending);

bool quicbit_pending_que_send(QuicbitPendingQue *pending,
                              uint64_t kind,
                              const uint8_t *data,
                              uintptr_t len);

bool quicbit_pending_que_finish(QuicbitPendingQue *pending);

void quicbit_pending_que_free(QuicbitPendingQue *pending);

QuicbitPutClient *quicbit_put_client_new_with_qos(const QuicbitNode *node,
                                                  const char *peer,
                                                  const char *topic,
                                                  QuicbitTopicQos qos);

QuicbitPutClient *quicbit_put_client_new(const QuicbitNode *node,
                                         const char *peer,
                                         const char *topic);

QuicbitPutClient *quicbit_put_system_client_new_with_qos(const QuicbitNode *node,
                                                         const char *topic,
                                                         QuicbitTopicQos qos);

void quicbit_put_client_free(QuicbitPutClient *client);

QuicbitItemStats quicbit_put_client_stats(const QuicbitPutClient *client);

bool quicbit_put_client_upload(QuicbitPutClient *client,
                               const QuicbitRawMessage *items,
                               uintptr_t len,
                               QuicbitMessage **out_message);

bool quicbit_put_client_open(QuicbitPutClient *client, QuicbitPutUpload **out_upload);

bool quicbit_put_upload_send(QuicbitPutUpload *upload,
                             uint64_t kind,
                             const uint8_t *data,
                             uintptr_t len);

bool quicbit_put_upload_finish(QuicbitPutUpload *upload, QuicbitMessage **out_message);

void quicbit_put_upload_free(QuicbitPutUpload *upload);

QuicbitAckServer *quicbit_ack_server_new_with_qos(const QuicbitNode *node,
                                                  const char *topic,
                                                  QuicbitTopicQos qos);

QuicbitAckServer *quicbit_ack_server_new(const QuicbitNode *node, const char *topic);

void quicbit_ack_server_free(QuicbitAckServer *server);

QuicbitItemStats quicbit_ack_server_stats(const QuicbitAckServer *server);

int32_t quicbit_ack_server_serve_one(QuicbitAckServer *server,
                                     uint64_t timeout_ms,
                                     QuicbitAckHandler handler,
                                     void *ctx);

int32_t quicbit_ack_server_take(QuicbitAckServer *server,
                                uint64_t timeout_ms,
                                QuicbitPuts **out_puts);

int32_t quicbit_puts_next(QuicbitPuts *puts, QuicbitMessage **out_message);

bool quicbit_puts_ack(QuicbitPuts *puts, uint64_t kind, const uint8_t *data, uintptr_t len);

void quicbit_puts_free(QuicbitPuts *puts);

QuicbitPipClient *quicbit_pip_client_new_with_qos(const QuicbitNode *node,
                                                  const char *peer,
                                                  const char *topic,
                                                  QuicbitTopicQos qos);

QuicbitPipClient *quicbit_pip_client_new(const QuicbitNode *node,
                                         const char *peer,
                                         const char *topic);

QuicbitPipClient *quicbit_pip_system_client_new_with_qos(const QuicbitNode *node,
                                                         const char *topic,
                                                         QuicbitTopicQos qos);

void quicbit_pip_client_free(QuicbitPipClient *client);

QuicbitItemStats quicbit_pip_client_stats(const QuicbitPipClient *client);

bool quicbit_pip_client_exchange(QuicbitPipClient *client,
                                 const QuicbitRawMessage *items,
                                 uintptr_t len,
                                 QuicbitMessages **out_messages);

bool quicbit_pip_client_open(QuicbitPipClient *client, QuicbitPip **out_pip);

bool quicbit_pip_send(QuicbitPip *pip, uint64_t kind, const uint8_t *data, uintptr_t len);

bool quicbit_pip_finish_send(QuicbitPip *pip);

int32_t quicbit_pip_next(QuicbitPip *pip, QuicbitMessage **out_message);

void quicbit_pip_close(QuicbitPip *pip);

void quicbit_pip_free(QuicbitPip *pip);

QuicbitPipServer *quicbit_pip_server_new_with_qos(const QuicbitNode *node,
                                                  const char *topic,
                                                  QuicbitTopicQos qos);

QuicbitPipServer *quicbit_pip_server_new(const QuicbitNode *node, const char *topic);

void quicbit_pip_server_free(QuicbitPipServer *server);

QuicbitItemStats quicbit_pip_server_stats(const QuicbitPipServer *server);

void quicbit_message_responder_send(QuicbitMessageResponder *responder,
                                    uint64_t kind,
                                    const uint8_t *data,
                                    uintptr_t len);

int32_t quicbit_pip_server_serve_one(QuicbitPipServer *server,
                                     uint64_t timeout_ms,
                                     QuicbitPipHandler handler,
                                     void *ctx);

int32_t quicbit_pip_server_take(QuicbitPipServer *server,
                                uint64_t timeout_ms,
                                QuicbitPendingPip **out_pending);

int32_t quicbit_pending_pip_next(QuicbitPendingPip *pip, QuicbitMessage **out_message);

bool quicbit_pending_pip_send(QuicbitPendingPip *pip,
                              uint64_t kind,
                              const uint8_t *data,
                              uintptr_t len);

bool quicbit_pending_pip_finish_send(QuicbitPendingPip *pip);

void quicbit_pending_pip_close(QuicbitPendingPip *pip);

void quicbit_pending_pip_free(QuicbitPendingPip *pip);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* QUICBIT_H */
