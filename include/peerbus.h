#ifndef PEERBUS_H
#define PEERBUS_H

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
  PEERBUS_DELIVERY_RELIABLE = 0,
  PEERBUS_DELIVERY_LATEST = 1,
  PEERBUS_DELIVERY_BEST_EFFORT = 2,
} PeerbusDeliveryPolicy;

typedef struct PeerbusAckServer PeerbusAckServer;

/**
 * Passed to a que/ans handler so it can append answer items.
 */
typedef struct PeerbusAnsResponder PeerbusAnsResponder;

typedef struct PeerbusAnsServer PeerbusAnsServer;

typedef struct PeerbusDatapodAckServer PeerbusDatapodAckServer;

typedef struct PeerbusDatapodAnsServer PeerbusDatapodAnsServer;

typedef struct PeerbusDatapodMessage PeerbusDatapodMessage;

typedef struct PeerbusDatapodMessages PeerbusDatapodMessages;

typedef struct PeerbusDatapodPip PeerbusDatapodPip;

typedef struct PeerbusDatapodPipClient PeerbusDatapodPipClient;

typedef struct PeerbusDatapodPipServer PeerbusDatapodPipServer;

typedef struct PeerbusDatapodPublisher PeerbusDatapodPublisher;

typedef struct PeerbusDatapodPutClient PeerbusDatapodPutClient;

typedef struct PeerbusDatapodPutUpload PeerbusDatapodPutUpload;

typedef struct PeerbusDatapodPuts PeerbusDatapodPuts;

typedef struct PeerbusDatapodQueClient PeerbusDatapodQueClient;

typedef struct PeerbusDatapodReqClient PeerbusDatapodReqClient;

typedef struct PeerbusDatapodReqServer PeerbusDatapodReqServer;

typedef struct PeerbusDatapodSample PeerbusDatapodSample;

typedef struct PeerbusDatapodSubscriber PeerbusDatapodSubscriber;

/**
 * An owned, received message (kind tag + payload bytes).
 */
typedef struct PeerbusMessage PeerbusMessage;

/**
 * Passed to put/ack or pip handlers so they can build the final reply list.
 */
typedef struct PeerbusMessageResponder PeerbusMessageResponder;

/**
 * Owned message list returned by que/ans and pip convenience calls.
 */
typedef struct PeerbusMessages PeerbusMessages;

typedef struct PeerbusNode PeerbusNode;

typedef struct PeerbusPeerPathDiagnostics PeerbusPeerPathDiagnostics;

typedef struct PeerbusPendingDatapodPip PeerbusPendingDatapodPip;

typedef struct PeerbusPendingDatapodQue PeerbusPendingDatapodQue;

typedef struct PeerbusPendingDatapodReq PeerbusPendingDatapodReq;

typedef struct PeerbusPendingPip PeerbusPendingPip;

typedef struct PeerbusPendingQue PeerbusPendingQue;

typedef struct PeerbusPendingReq PeerbusPendingReq;

typedef struct PeerbusPip PeerbusPip;

typedef struct PeerbusPipClient PeerbusPipClient;

typedef struct PeerbusPipServer PeerbusPipServer;

typedef struct PeerbusPublisher PeerbusPublisher;

typedef struct PeerbusPutClient PeerbusPutClient;

typedef struct PeerbusPutUpload PeerbusPutUpload;

typedef struct PeerbusPuts PeerbusPuts;

typedef struct PeerbusQueClient PeerbusQueClient;

typedef struct PeerbusReqClient PeerbusReqClient;

typedef struct PeerbusReqServer PeerbusReqServer;

/**
 * Passed to a request handler so it can set the response.
 */
typedef struct PeerbusResponder PeerbusResponder;

typedef struct PeerbusSample PeerbusSample;

typedef struct PeerbusSubscriber PeerbusSubscriber;

/**
 * Optional node construction settings. NULL string pointers mean "unset";
 * zero numeric limits mean "use the Rust default".
 */
typedef struct {
  const char *identity;
  bool no_relay;
  const char *system_did;
  const char *const *allowed_peers;
  uintptr_t allowed_peers_len;
  uintptr_t max_payload_bytes;
  uint32_t history_depth;
  uint32_t subscriber_buffer;
  uint32_t max_publishers;
  uint32_t max_subscribers;
} PeerbusNodeConfig;

typedef struct {
  uintptr_t publisher_topics;
  uintptr_t cached_peers;
} PeerbusNodeStats;

/**
 * C mirror of [`TopicQos`]. Zero fields are allowed; use
 * [`peerbus_topic_qos_reliable`], [`peerbus_topic_qos_latest`], or
 * [`peerbus_topic_qos_best_effort`] for canonical defaults.
 */
typedef struct {
  PeerbusDeliveryPolicy delivery;
  uintptr_t max_message_bytes;
  uintptr_t max_inflight_bytes;
  uintptr_t chunk_bytes;
  uintptr_t subscriber_queue;
  uint8_t priority;
} PeerbusTopicQos;

typedef struct {
  uint64_t published;
  uint64_t remote_dropped;
  uint64_t stale_dropped;
  uint64_t bytes_sent;
  uint64_t send_errors;
} PeerbusPublisherStats;

/**
 * A borrowed view of contiguous bytes owned by a handle.
 */
typedef struct {
  const uint8_t *ptr;
  uintptr_t len;
} PeerbusBytes;

/**
 * Preferred three-letter generic-datapod que/ans answer list handle name.
 *
 * `PeerbusDatapodMessages` remains the shared finite-list storage type for
 * compatibility with earlier binding code.
 */
typedef PeerbusDatapodMessages PeerbusDatapodAnswers;

typedef struct {
  uint64_t received;
  uint64_t disconnects;
  uint64_t stale_dropped;
  uint64_t incomplete_dropped;
  uint64_t bytes_received;
} PeerbusSubscriberStats;

typedef struct {
  uint64_t messages_out;
  uint64_t messages_in;
  uint64_t bytes_out;
  uint64_t bytes_in;
  uint64_t errors;
} PeerbusItemStats;

/**
 * Borrowed datapod message input used by generic datapod C convenience APIs.
 */
typedef struct {
  uint64_t type_hash;
  PeerbusBytes wire;
} PeerbusDatapodRawMessage;

/**
 * Preferred three-letter generic-datapod put/ack sender handle name.
 *
 * `PeerbusDatapodPutUpload` remains as a compatibility alias in the C ABI.
 */
typedef PeerbusDatapodPutUpload PeerbusDatapodPutSender;

/**
 * Request handler callback: receives the request `kind` + bytes and the
 * `responder` to fill via [`peerbus_responder_set`].
 */
typedef void (*PeerbusReqHandler)(void *ctx,
                                  uint64_t kind,
                                  const uint8_t *data,
                                  uintptr_t len,
                                  PeerbusResponder *responder);

/**
 * Preferred three-letter que/ans answer list handle name.
 *
 * `PeerbusMessages` remains the shared finite-list storage type for
 * compatibility with earlier binding code.
 */
typedef PeerbusMessages PeerbusAnswers;

typedef void (*PeerbusAnsHandler)(void *ctx,
                                  uint64_t kind,
                                  const uint8_t *data,
                                  uintptr_t len,
                                  PeerbusAnsResponder *responder);

/**
 * Borrowed message input used by finite C convenience APIs.
 */
typedef struct {
  uint64_t kind;
  PeerbusBytes data;
} PeerbusRawMessage;

/**
 * Preferred three-letter put/ack sender handle name.
 *
 * `PeerbusPutUpload` remains as a compatibility alias in the C ABI.
 */
typedef PeerbusPutUpload PeerbusPutSender;

typedef void (*PeerbusAckHandler)(void *ctx,
                                  const PeerbusMessages *items,
                                  PeerbusResponder *responder);

typedef void (*PeerbusPipHandler)(void *ctx,
                                  const PeerbusMessages *items,
                                  PeerbusMessageResponder *responder);

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Returns the last error message on this thread, or NULL if the most
 * recent call succeeded. The pointer is valid until the next peerbus
 * call on the same thread.
 */
const char *peerbus_last_error_message(void);

/**
 * Create a node. `identity` may be NULL for an ephemeral key. Returns
 * NULL on failure (see [`peerbus_last_error_message`]).
 */
PeerbusNode *peerbus_node_new(const char *identity, bool no_relay);

PeerbusNodeConfig peerbus_node_config_default(void);

PeerbusNode *peerbus_node_new_with_config(PeerbusNodeConfig cfg);

void peerbus_node_free(PeerbusNode *node);

/**
 * This node's identity as a `did:key:z6Mk…` string. Caller owns the
 * returned C string and must free it with [`peerbus_string_free`].
 * Returns NULL on failure.
 */
char *peerbus_node_did_key(const PeerbusNode *node);

char *peerbus_node_endpoint_addr(const PeerbusNode *node);

bool peerbus_node_add_topic_route(const PeerbusNode *node,
                                  const char *topic,
                                  const char *endpoint_addr);

bool peerbus_node_add_system_peer(const PeerbusNode *node, const char *endpoint_addr);

PeerbusNodeStats peerbus_node_stats(const PeerbusNode *node);

PeerbusPeerPathDiagnostics *peerbus_node_peer_path_diagnostics(const PeerbusNode *node,
                                                               const char *endpoint_addr);

void peerbus_peer_path_diagnostics_free(PeerbusPeerPathDiagnostics *diag);

char *peerbus_peer_path_diagnostics_peer(const PeerbusPeerPathDiagnostics *diag);

uintptr_t peerbus_peer_path_diagnostics_path_count(const PeerbusPeerPathDiagnostics *diag);

bool peerbus_peer_path_diagnostics_max_datagram_size(const PeerbusPeerPathDiagnostics *diag,
                                                     uintptr_t *out);

uintptr_t peerbus_peer_path_diagnostics_datagram_send_buffer_space(const PeerbusPeerPathDiagnostics *diag);

char *peerbus_peer_path_diagnostics_path_id(const PeerbusPeerPathDiagnostics *diag,
                                            uintptr_t index);

char *peerbus_peer_path_diagnostics_remote_addr(const PeerbusPeerPathDiagnostics *diag,
                                                uintptr_t index);

bool peerbus_peer_path_diagnostics_path_selected(const PeerbusPeerPathDiagnostics *diag,
                                                 uintptr_t index);

bool peerbus_peer_path_diagnostics_path_is_ip(const PeerbusPeerPathDiagnostics *diag,
                                              uintptr_t index);

bool peerbus_peer_path_diagnostics_path_is_relay(const PeerbusPeerPathDiagnostics *diag,
                                                 uintptr_t index);

double peerbus_peer_path_diagnostics_path_rtt_ms(const PeerbusPeerPathDiagnostics *diag,
                                                 uintptr_t index);

uint16_t peerbus_peer_path_diagnostics_path_current_mtu(const PeerbusPeerPathDiagnostics *diag,
                                                        uintptr_t index);

uint64_t peerbus_peer_path_diagnostics_path_cwnd(const PeerbusPeerPathDiagnostics *diag,
                                                 uintptr_t index);

uint64_t peerbus_peer_path_diagnostics_path_lost_packets(const PeerbusPeerPathDiagnostics *diag,
                                                         uintptr_t index);

/**
 * Free a string returned by peerbus (e.g. [`peerbus_node_did_key`]).
 */
void peerbus_string_free(char *s);

PeerbusTopicQos peerbus_topic_qos_reliable(void);

PeerbusTopicQos peerbus_topic_qos_latest(void);

PeerbusTopicQos peerbus_topic_qos_best_effort(void);

PeerbusPublisher *peerbus_publisher_new(const PeerbusNode *node, const char *topic);

PeerbusPublisher *peerbus_publisher_new_with_qos(const PeerbusNode *node,
                                                 const char *topic,
                                                 PeerbusTopicQos qos);

void peerbus_publisher_free(PeerbusPublisher *publisher);

PeerbusPublisherStats peerbus_publisher_stats(const PeerbusPublisher *publisher);

/**
 * Publish `data` (`len` bytes) with user tag `kind`. Returns false on
 * failure.
 */
bool peerbus_publisher_send(PeerbusPublisher *publisher,
                            uint64_t kind,
                            const uint8_t *data,
                            uintptr_t len);

PeerbusSubscriber *peerbus_subscriber_new(const PeerbusNode *node,
                                          const char *peer,
                                          const char *topic);

PeerbusSubscriber *peerbus_subscriber_new_with_qos(const PeerbusNode *node,
                                                   const char *peer,
                                                   const char *topic,
                                                   PeerbusTopicQos qos);

PeerbusSubscriber *peerbus_subscribe_new_with_qos(const PeerbusNode *node,
                                                  const char *topic,
                                                  PeerbusTopicQos qos);

void peerbus_subscriber_free(PeerbusSubscriber *subscriber);

PeerbusDatapodPublisher *peerbus_datapod_publisher_new_with_qos(const PeerbusNode *node,
                                                                const char *topic,
                                                                PeerbusTopicQos qos);

PeerbusSubscriber *peerbus_subscribe_with_qos(const PeerbusNode *node,
                                              const char *topic,
                                              PeerbusTopicQos qos);

void peerbus_datapod_publisher_free(PeerbusDatapodPublisher *publisher);

/**
 * Publish a datapod wire message: `type_hash` plus `header || payload` bytes.
 */
bool peerbus_datapod_publisher_send(PeerbusDatapodPublisher *publisher,
                                    uint64_t type_hash,
                                    const uint8_t *wire,
                                    uintptr_t len);

PeerbusDatapodSubscriber *peerbus_datapod_subscriber_new_with_qos(const PeerbusNode *node,
                                                                  const char *peer,
                                                                  const char *topic,
                                                                  PeerbusTopicQos qos);

PeerbusDatapodSubscriber *peerbus_datapod_subscribe_new_with_qos(const PeerbusNode *node,
                                                                 const char *topic,
                                                                 PeerbusTopicQos qos);

PeerbusDatapodSubscriber *peerbus_datapod_subscribe_with_qos(const PeerbusNode *node,
                                                             const char *topic,
                                                             PeerbusTopicQos qos);

void peerbus_datapod_subscriber_free(PeerbusDatapodSubscriber *subscriber);

/**
 * Poll for a datapod sample without copying the wire bytes.
 */
int32_t peerbus_datapod_subscriber_take_sample(PeerbusDatapodSubscriber *subscriber,
                                               PeerbusDatapodSample **out_sample);

uint64_t peerbus_datapod_sample_type_hash(const PeerbusDatapodSample *sample);

/**
 * Borrowed zero-copy view of datapod `header || payload` wire bytes.
 */
PeerbusBytes peerbus_datapod_sample_wire(const PeerbusDatapodSample *sample);

void peerbus_datapod_sample_free(PeerbusDatapodSample *sample);

uint64_t peerbus_datapod_message_type_hash(const PeerbusDatapodMessage *message);

PeerbusBytes peerbus_datapod_message_wire(const PeerbusDatapodMessage *message);

void peerbus_datapod_message_free(PeerbusDatapodMessage *message);

uintptr_t peerbus_datapod_messages_len(const PeerbusDatapodMessages *messages);

uint64_t peerbus_datapod_messages_type_hash_at(const PeerbusDatapodMessages *messages,
                                               uintptr_t index);

PeerbusBytes peerbus_datapod_messages_wire_at(const PeerbusDatapodMessages *messages,
                                              uintptr_t index);

void peerbus_datapod_messages_free(PeerbusDatapodMessages *messages);

uintptr_t peerbus_datapod_answers_len(const PeerbusDatapodAnswers *answers);

uint64_t peerbus_datapod_answers_type_hash_at(const PeerbusDatapodAnswers *answers,
                                              uintptr_t index);

PeerbusBytes peerbus_datapod_answers_wire_at(const PeerbusDatapodAnswers *answers, uintptr_t index);

void peerbus_datapod_answers_free(PeerbusDatapodAnswers *answers);

PeerbusSubscriberStats peerbus_subscriber_stats(const PeerbusSubscriber *subscriber);

/**
 * Poll for the next sample. Returns `1` and writes an owned message to
 * `*out_message` when one is available, `0` when none is ready, and
 * `-1` on error. A returned message must be freed with
 * [`peerbus_message_free`].
 */
int32_t peerbus_subscriber_take(PeerbusSubscriber *subscriber, PeerbusMessage **out_message);

/**
 * Poll for the next sample without copying payload bytes.
 *
 * Returns `1` and writes a borrowed sample handle to `*out_sample` when one is
 * available, `0` when none is ready, and `-1` on error. A returned sample must
 * be freed with [`peerbus_sample_free`]. The byte view returned from
 * [`peerbus_sample_data`] is valid until that free call.
 */
int32_t peerbus_subscriber_take_sample(PeerbusSubscriber *subscriber, PeerbusSample **out_sample);

uint64_t peerbus_sample_kind(const PeerbusSample *sample);

/**
 * Borrowed zero-copy view of a sample payload.
 *
 * For local SHM this points directly into the shared-memory slot and pins that
 * slot until [`peerbus_sample_free`] is called. Copy it if you need to keep the
 * data longer.
 */
PeerbusBytes peerbus_sample_data(const PeerbusSample *sample);

void peerbus_sample_free(PeerbusSample *sample);

PeerbusMessage *peerbus_message_new(uint64_t kind, const uint8_t *data, uintptr_t len);

uint64_t peerbus_message_kind(const PeerbusMessage *message);

/**
 * Borrowed view of the message payload, valid until the message is
 * freed.
 */
PeerbusBytes peerbus_message_data(const PeerbusMessage *message);

void peerbus_message_free(PeerbusMessage *message);

PeerbusDatapodReqClient *peerbus_datapod_req_client_new_with_qos(const PeerbusNode *node,
                                                                 const char *peer,
                                                                 const char *topic,
                                                                 PeerbusTopicQos qos);

PeerbusDatapodReqClient *peerbus_datapod_req_system_client_new_with_qos(const PeerbusNode *node,
                                                                        const char *topic,
                                                                        PeerbusTopicQos qos);

void peerbus_datapod_req_client_free(PeerbusDatapodReqClient *client);

PeerbusItemStats peerbus_datapod_req_client_stats(const PeerbusDatapodReqClient *client);

bool peerbus_datapod_req_client_call(PeerbusDatapodReqClient *client,
                                     uint64_t type_hash,
                                     const uint8_t *wire,
                                     uintptr_t len,
                                     PeerbusDatapodMessage **out_message);

PeerbusDatapodReqServer *peerbus_datapod_req_server_new_with_qos(const PeerbusNode *node,
                                                                 const char *topic,
                                                                 PeerbusTopicQos qos);

void peerbus_datapod_req_server_free(PeerbusDatapodReqServer *server);

PeerbusItemStats peerbus_datapod_req_server_stats(const PeerbusDatapodReqServer *server);

int32_t peerbus_datapod_req_server_take(PeerbusDatapodReqServer *server,
                                        uint64_t timeout_ms,
                                        PeerbusPendingDatapodReq **out_pending);

const PeerbusDatapodMessage *peerbus_pending_datapod_req_request(const PeerbusPendingDatapodReq *pending);

bool peerbus_pending_datapod_req_reply(PeerbusPendingDatapodReq *pending,
                                       uint64_t type_hash,
                                       const uint8_t *wire,
                                       uintptr_t len);

void peerbus_pending_datapod_req_free(PeerbusPendingDatapodReq *pending);

PeerbusDatapodQueClient *peerbus_datapod_que_client_new_with_qos(const PeerbusNode *node,
                                                                 const char *peer,
                                                                 const char *topic,
                                                                 PeerbusTopicQos qos);

PeerbusDatapodQueClient *peerbus_datapod_que_system_client_new_with_qos(const PeerbusNode *node,
                                                                        const char *topic,
                                                                        PeerbusTopicQos qos);

void peerbus_datapod_que_client_free(PeerbusDatapodQueClient *client);

PeerbusItemStats peerbus_datapod_que_client_stats(const PeerbusDatapodQueClient *client);

bool peerbus_datapod_que_client_send(PeerbusDatapodQueClient *client,
                                     uint64_t type_hash,
                                     const uint8_t *wire,
                                     uintptr_t len,
                                     PeerbusDatapodMessages **out_messages);

PeerbusDatapodAnsServer *peerbus_datapod_ans_server_new_with_qos(const PeerbusNode *node,
                                                                 const char *topic,
                                                                 PeerbusTopicQos qos);

void peerbus_datapod_ans_server_free(PeerbusDatapodAnsServer *server);

PeerbusItemStats peerbus_datapod_ans_server_stats(const PeerbusDatapodAnsServer *server);

int32_t peerbus_datapod_ans_server_take(PeerbusDatapodAnsServer *server,
                                        uint64_t timeout_ms,
                                        PeerbusPendingDatapodQue **out_pending);

const PeerbusDatapodMessage *peerbus_pending_datapod_que_request(const PeerbusPendingDatapodQue *pending);

bool peerbus_pending_datapod_que_send(PeerbusPendingDatapodQue *pending,
                                      uint64_t type_hash,
                                      const uint8_t *wire,
                                      uintptr_t len);

bool peerbus_pending_datapod_que_finish(PeerbusPendingDatapodQue *pending);

void peerbus_pending_datapod_que_free(PeerbusPendingDatapodQue *pending);

PeerbusDatapodPutClient *peerbus_datapod_put_client_new_with_qos(const PeerbusNode *node,
                                                                 const char *peer,
                                                                 const char *topic,
                                                                 PeerbusTopicQos qos);

PeerbusDatapodPutClient *peerbus_datapod_put_system_client_new_with_qos(const PeerbusNode *node,
                                                                        const char *topic,
                                                                        PeerbusTopicQos qos);

void peerbus_datapod_put_client_free(PeerbusDatapodPutClient *client);

PeerbusItemStats peerbus_datapod_put_client_stats(const PeerbusDatapodPutClient *client);

bool peerbus_datapod_put_client_upload(PeerbusDatapodPutClient *client,
                                       const PeerbusDatapodRawMessage *items,
                                       uintptr_t len,
                                       PeerbusDatapodMessage **out_message);

bool peerbus_datapod_put_client_put(PeerbusDatapodPutClient *client,
                                    const PeerbusDatapodRawMessage *items,
                                    uintptr_t len,
                                    PeerbusDatapodMessage **out_message);

bool peerbus_datapod_put_client_open(PeerbusDatapodPutClient *client,
                                     PeerbusDatapodPutUpload **out_upload);

bool peerbus_datapod_put_client_open_sender(PeerbusDatapodPutClient *client,
                                            PeerbusDatapodPutSender **out_sender);

bool peerbus_datapod_put_upload_send(PeerbusDatapodPutUpload *upload,
                                     uint64_t type_hash,
                                     const uint8_t *wire,
                                     uintptr_t len);

bool peerbus_datapod_put_sender_send(PeerbusDatapodPutSender *sender,
                                     uint64_t type_hash,
                                     const uint8_t *wire,
                                     uintptr_t len);

bool peerbus_datapod_put_upload_finish(PeerbusDatapodPutUpload *upload,
                                       PeerbusDatapodMessage **out_message);

bool peerbus_datapod_put_sender_finish(PeerbusDatapodPutSender *sender,
                                       PeerbusDatapodMessage **out_message);

void peerbus_datapod_put_upload_free(PeerbusDatapodPutUpload *upload);

void peerbus_datapod_put_sender_free(PeerbusDatapodPutSender *sender);

PeerbusDatapodAckServer *peerbus_datapod_ack_server_new_with_qos(const PeerbusNode *node,
                                                                 const char *topic,
                                                                 PeerbusTopicQos qos);

void peerbus_datapod_ack_server_free(PeerbusDatapodAckServer *server);

PeerbusItemStats peerbus_datapod_ack_server_stats(const PeerbusDatapodAckServer *server);

int32_t peerbus_datapod_ack_server_take(PeerbusDatapodAckServer *server,
                                        uint64_t timeout_ms,
                                        PeerbusDatapodPuts **out_puts);

int32_t peerbus_datapod_puts_next(PeerbusDatapodPuts *puts, PeerbusDatapodMessage **out_message);

bool peerbus_datapod_puts_ack(PeerbusDatapodPuts *puts,
                              uint64_t type_hash,
                              const uint8_t *wire,
                              uintptr_t len);

void peerbus_datapod_puts_free(PeerbusDatapodPuts *puts);

PeerbusDatapodPipClient *peerbus_datapod_pip_client_new_with_qos(const PeerbusNode *node,
                                                                 const char *peer,
                                                                 const char *topic,
                                                                 PeerbusTopicQos qos);

PeerbusDatapodPipClient *peerbus_datapod_pip_system_client_new_with_qos(const PeerbusNode *node,
                                                                        const char *topic,
                                                                        PeerbusTopicQos qos);

void peerbus_datapod_pip_client_free(PeerbusDatapodPipClient *client);

PeerbusItemStats peerbus_datapod_pip_client_stats(const PeerbusDatapodPipClient *client);

bool peerbus_datapod_pip_client_exchange(PeerbusDatapodPipClient *client,
                                         const PeerbusDatapodRawMessage *items,
                                         uintptr_t len,
                                         PeerbusDatapodMessages **out_messages);

bool peerbus_datapod_pip_client_open(PeerbusDatapodPipClient *client, PeerbusDatapodPip **out_pip);

bool peerbus_datapod_pip_send(PeerbusDatapodPip *pip,
                              uint64_t type_hash,
                              const uint8_t *wire,
                              uintptr_t len);

bool peerbus_datapod_pip_finish_send(PeerbusDatapodPip *pip);

int32_t peerbus_datapod_pip_next(PeerbusDatapodPip *pip, PeerbusDatapodMessage **out_message);

void peerbus_datapod_pip_close(PeerbusDatapodPip *pip);

void peerbus_datapod_pip_free(PeerbusDatapodPip *pip);

PeerbusDatapodPipServer *peerbus_datapod_pip_server_new_with_qos(const PeerbusNode *node,
                                                                 const char *topic,
                                                                 PeerbusTopicQos qos);

void peerbus_datapod_pip_server_free(PeerbusDatapodPipServer *server);

PeerbusItemStats peerbus_datapod_pip_server_stats(const PeerbusDatapodPipServer *server);

int32_t peerbus_datapod_pip_server_take(PeerbusDatapodPipServer *server,
                                        uint64_t timeout_ms,
                                        PeerbusPendingDatapodPip **out_pending);

int32_t peerbus_pending_datapod_pip_next(PeerbusPendingDatapodPip *pip,
                                         PeerbusDatapodMessage **out_message);

bool peerbus_pending_datapod_pip_send(PeerbusPendingDatapodPip *pip,
                                      uint64_t type_hash,
                                      const uint8_t *wire,
                                      uintptr_t len);

bool peerbus_pending_datapod_pip_finish_send(PeerbusPendingDatapodPip *pip);

void peerbus_pending_datapod_pip_close(PeerbusPendingDatapodPip *pip);

void peerbus_pending_datapod_pip_free(PeerbusPendingDatapodPip *pip);

PeerbusReqClient *peerbus_req_client_new(const PeerbusNode *node,
                                         const char *peer,
                                         const char *topic);

PeerbusReqClient *peerbus_req_client_new_with_qos(const PeerbusNode *node,
                                                  const char *peer,
                                                  const char *topic,
                                                  PeerbusTopicQos qos);

PeerbusReqClient *peerbus_req_system_client_new_with_qos(const PeerbusNode *node,
                                                         const char *topic,
                                                         PeerbusTopicQos qos);

void peerbus_req_client_free(PeerbusReqClient *client);

PeerbusItemStats peerbus_req_client_stats(const PeerbusReqClient *client);

/**
 * Send a request and block for the response. Returns false on failure;
 * on success writes an owned response message to `*out_message` (free
 * with [`peerbus_message_free`]).
 */
bool peerbus_req_client_call(PeerbusReqClient *client,
                             uint64_t kind,
                             const uint8_t *data,
                             uintptr_t len,
                             PeerbusMessage **out_message);

PeerbusReqServer *peerbus_req_server_new(const PeerbusNode *node, const char *topic);

PeerbusReqServer *peerbus_req_server_new_with_qos(const PeerbusNode *node,
                                                  const char *topic,
                                                  PeerbusTopicQos qos);

void peerbus_req_server_free(PeerbusReqServer *server);

PeerbusItemStats peerbus_req_server_stats(const PeerbusReqServer *server);

/**
 * Set the response on a responder passed to a request handler. Copies
 * `data` immediately; safe to call once per handler invocation.
 */
void peerbus_responder_set(PeerbusResponder *responder,
                           uint64_t kind,
                           const uint8_t *data,
                           uintptr_t len);

/**
 * Serve at most one request, waiting up to `timeout_ms`. Invokes
 * `handler` with the request and a responder; whatever the handler sets
 * (via [`peerbus_responder_set`]) is sent back. Returns `1` if a request
 * was served, `0` on timeout, `-1` on error.
 */
int32_t peerbus_req_server_serve_one(PeerbusReqServer *server,
                                     uint64_t timeout_ms,
                                     PeerbusReqHandler handler,
                                     void *ctx);

int32_t peerbus_req_server_take(PeerbusReqServer *server,
                                uint64_t timeout_ms,
                                PeerbusPendingReq **out_pending);

const PeerbusMessage *peerbus_pending_req_request(const PeerbusPendingReq *pending);

bool peerbus_pending_req_reply(PeerbusPendingReq *pending,
                               uint64_t kind,
                               const uint8_t *data,
                               uintptr_t len);

void peerbus_pending_req_free(PeerbusPendingReq *pending);

uintptr_t peerbus_messages_len(const PeerbusMessages *messages);

uint64_t peerbus_messages_kind_at(const PeerbusMessages *messages, uintptr_t index);

PeerbusBytes peerbus_messages_data_at(const PeerbusMessages *messages, uintptr_t index);

void peerbus_messages_free(PeerbusMessages *messages);

uintptr_t peerbus_answers_len(const PeerbusAnswers *answers);

uint64_t peerbus_answers_kind_at(const PeerbusAnswers *answers, uintptr_t index);

PeerbusBytes peerbus_answers_data_at(const PeerbusAnswers *answers, uintptr_t index);

void peerbus_answers_free(PeerbusAnswers *answers);

PeerbusQueClient *peerbus_que_client_new_with_qos(const PeerbusNode *node,
                                                  const char *peer,
                                                  const char *topic,
                                                  PeerbusTopicQos qos);

PeerbusQueClient *peerbus_que_client_new(const PeerbusNode *node,
                                         const char *peer,
                                         const char *topic);

PeerbusQueClient *peerbus_que_system_client_new_with_qos(const PeerbusNode *node,
                                                         const char *topic,
                                                         PeerbusTopicQos qos);

void peerbus_que_client_free(PeerbusQueClient *client);

PeerbusItemStats peerbus_que_client_stats(const PeerbusQueClient *client);

bool peerbus_que_client_send(PeerbusQueClient *client,
                             uint64_t kind,
                             const uint8_t *data,
                             uintptr_t len,
                             PeerbusMessages **out_messages);

PeerbusAnsServer *peerbus_ans_server_new_with_qos(const PeerbusNode *node,
                                                  const char *topic,
                                                  PeerbusTopicQos qos);

PeerbusAnsServer *peerbus_ans_server_new(const PeerbusNode *node, const char *topic);

void peerbus_ans_server_free(PeerbusAnsServer *server);

PeerbusItemStats peerbus_ans_server_stats(const PeerbusAnsServer *server);

void peerbus_ans_responder_send(PeerbusAnsResponder *responder,
                                uint64_t kind,
                                const uint8_t *data,
                                uintptr_t len);

int32_t peerbus_ans_server_serve_one(PeerbusAnsServer *server,
                                     uint64_t timeout_ms,
                                     PeerbusAnsHandler handler,
                                     void *ctx);

int32_t peerbus_ans_server_take(PeerbusAnsServer *server,
                                uint64_t timeout_ms,
                                PeerbusPendingQue **out_pending);

const PeerbusMessage *peerbus_pending_que_request(const PeerbusPendingQue *pending);

bool peerbus_pending_que_send(PeerbusPendingQue *pending,
                              uint64_t kind,
                              const uint8_t *data,
                              uintptr_t len);

bool peerbus_pending_que_finish(PeerbusPendingQue *pending);

void peerbus_pending_que_free(PeerbusPendingQue *pending);

PeerbusPutClient *peerbus_put_client_new_with_qos(const PeerbusNode *node,
                                                  const char *peer,
                                                  const char *topic,
                                                  PeerbusTopicQos qos);

PeerbusPutClient *peerbus_put_client_new(const PeerbusNode *node,
                                         const char *peer,
                                         const char *topic);

PeerbusPutClient *peerbus_put_system_client_new_with_qos(const PeerbusNode *node,
                                                         const char *topic,
                                                         PeerbusTopicQos qos);

void peerbus_put_client_free(PeerbusPutClient *client);

PeerbusItemStats peerbus_put_client_stats(const PeerbusPutClient *client);

bool peerbus_put_client_upload(PeerbusPutClient *client,
                               const PeerbusRawMessage *items,
                               uintptr_t len,
                               PeerbusMessage **out_message);

bool peerbus_put_client_put(PeerbusPutClient *client,
                            const PeerbusRawMessage *items,
                            uintptr_t len,
                            PeerbusMessage **out_message);

bool peerbus_put_client_open(PeerbusPutClient *client, PeerbusPutUpload **out_upload);

bool peerbus_put_client_open_sender(PeerbusPutClient *client, PeerbusPutSender **out_sender);

bool peerbus_put_upload_send(PeerbusPutUpload *upload,
                             uint64_t kind,
                             const uint8_t *data,
                             uintptr_t len);

bool peerbus_put_sender_send(PeerbusPutSender *sender,
                             uint64_t kind,
                             const uint8_t *data,
                             uintptr_t len);

bool peerbus_put_upload_finish(PeerbusPutUpload *upload, PeerbusMessage **out_message);

bool peerbus_put_sender_finish(PeerbusPutSender *sender, PeerbusMessage **out_message);

void peerbus_put_upload_free(PeerbusPutUpload *upload);

void peerbus_put_sender_free(PeerbusPutSender *sender);

PeerbusAckServer *peerbus_ack_server_new_with_qos(const PeerbusNode *node,
                                                  const char *topic,
                                                  PeerbusTopicQos qos);

PeerbusAckServer *peerbus_ack_server_new(const PeerbusNode *node, const char *topic);

void peerbus_ack_server_free(PeerbusAckServer *server);

PeerbusItemStats peerbus_ack_server_stats(const PeerbusAckServer *server);

int32_t peerbus_ack_server_serve_one(PeerbusAckServer *server,
                                     uint64_t timeout_ms,
                                     PeerbusAckHandler handler,
                                     void *ctx);

int32_t peerbus_ack_server_take(PeerbusAckServer *server,
                                uint64_t timeout_ms,
                                PeerbusPuts **out_puts);

int32_t peerbus_puts_next(PeerbusPuts *puts, PeerbusMessage **out_message);

bool peerbus_puts_ack(PeerbusPuts *puts, uint64_t kind, const uint8_t *data, uintptr_t len);

void peerbus_puts_free(PeerbusPuts *puts);

PeerbusPipClient *peerbus_pip_client_new_with_qos(const PeerbusNode *node,
                                                  const char *peer,
                                                  const char *topic,
                                                  PeerbusTopicQos qos);

PeerbusPipClient *peerbus_pip_client_new(const PeerbusNode *node,
                                         const char *peer,
                                         const char *topic);

PeerbusPipClient *peerbus_pip_system_client_new_with_qos(const PeerbusNode *node,
                                                         const char *topic,
                                                         PeerbusTopicQos qos);

void peerbus_pip_client_free(PeerbusPipClient *client);

PeerbusItemStats peerbus_pip_client_stats(const PeerbusPipClient *client);

bool peerbus_pip_client_exchange(PeerbusPipClient *client,
                                 const PeerbusRawMessage *items,
                                 uintptr_t len,
                                 PeerbusMessages **out_messages);

bool peerbus_pip_client_open(PeerbusPipClient *client, PeerbusPip **out_pip);

bool peerbus_pip_send(PeerbusPip *pip, uint64_t kind, const uint8_t *data, uintptr_t len);

bool peerbus_pip_finish_send(PeerbusPip *pip);

int32_t peerbus_pip_next(PeerbusPip *pip, PeerbusMessage **out_message);

void peerbus_pip_close(PeerbusPip *pip);

void peerbus_pip_free(PeerbusPip *pip);

PeerbusPipServer *peerbus_pip_server_new_with_qos(const PeerbusNode *node,
                                                  const char *topic,
                                                  PeerbusTopicQos qos);

PeerbusPipServer *peerbus_pip_server_new(const PeerbusNode *node, const char *topic);

void peerbus_pip_server_free(PeerbusPipServer *server);

PeerbusItemStats peerbus_pip_server_stats(const PeerbusPipServer *server);

void peerbus_message_responder_send(PeerbusMessageResponder *responder,
                                    uint64_t kind,
                                    const uint8_t *data,
                                    uintptr_t len);

int32_t peerbus_pip_server_serve_one(PeerbusPipServer *server,
                                     uint64_t timeout_ms,
                                     PeerbusPipHandler handler,
                                     void *ctx);

int32_t peerbus_pip_server_take(PeerbusPipServer *server,
                                uint64_t timeout_ms,
                                PeerbusPendingPip **out_pending);

int32_t peerbus_pending_pip_next(PeerbusPendingPip *pip, PeerbusMessage **out_message);

bool peerbus_pending_pip_send(PeerbusPendingPip *pip,
                              uint64_t kind,
                              const uint8_t *data,
                              uintptr_t len);

bool peerbus_pending_pip_finish_send(PeerbusPendingPip *pip);

void peerbus_pending_pip_close(PeerbusPendingPip *pip);

void peerbus_pending_pip_free(PeerbusPendingPip *pip);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* PEERBUS_H */
