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
 * An owned, received message (kind tag + payload bytes).
 */
typedef struct QuicbitMessage QuicbitMessage;

typedef struct QuicbitNode QuicbitNode;

typedef struct QuicbitPublisher QuicbitPublisher;

typedef struct QuicbitReqClient QuicbitReqClient;

typedef struct QuicbitReqServer QuicbitReqServer;

/**
 * Passed to a request handler so it can set the response.
 */
typedef struct QuicbitResponder QuicbitResponder;

typedef struct QuicbitSubscriber QuicbitSubscriber;

/**
 * A borrowed view of contiguous bytes owned by a handle.
 */
typedef struct {
  const uint8_t *ptr;
  uintptr_t len;
} QuicbitBytes;

/**
 * Request handler callback: receives the request `kind` + bytes and the
 * `responder` to fill via [`quicbit_responder_set`].
 */
typedef void (*QuicbitReqHandler)(void *ctx,
                                  uint64_t kind,
                                  const uint8_t *data,
                                  uintptr_t len,
                                  QuicbitResponder *responder);

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

void quicbit_node_free(QuicbitNode *node);

/**
 * This node's identity as a `did:key:z6Mk…` string. Caller owns the
 * returned C string and must free it with [`quicbit_string_free`].
 * Returns NULL on failure.
 */
char *quicbit_node_did_key(const QuicbitNode *node);

/**
 * Free a string returned by quicbit (e.g. [`quicbit_node_did_key`]).
 */
void quicbit_string_free(char *s);

QuicbitPublisher *quicbit_publisher_new(const QuicbitNode *node, const char *topic);

void quicbit_publisher_free(QuicbitPublisher *publisher);

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

void quicbit_subscriber_free(QuicbitSubscriber *subscriber);

/**
 * Poll for the next sample. Returns `1` and writes an owned message to
 * `*out_message` when one is available, `0` when none is ready, and
 * `-1` on error. A returned message must be freed with
 * [`quicbit_message_free`].
 */
int32_t quicbit_subscriber_take(QuicbitSubscriber *subscriber, QuicbitMessage **out_message);

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

void quicbit_req_client_free(QuicbitReqClient *client);

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

void quicbit_req_server_free(QuicbitReqServer *server);

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

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* QUICBIT_H */
