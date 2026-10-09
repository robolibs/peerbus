#include "peerbus.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

typedef struct {
  PeerbusAckServer *server;
  int rc;
} ServerThread;

static void put_handler(void *ctx, const PeerbusMessages *items, PeerbusResponder *responder) {
  (void)ctx;
  uint8_t sum = 0;
  for (uintptr_t i = 0; i < peerbus_messages_len(items); i++) {
    PeerbusBytes bytes = peerbus_messages_data_at(items, i);
    for (uintptr_t j = 0; j < bytes.len; j++) {
      sum = (uint8_t)(sum + bytes.ptr[j]);
    }
  }
  peerbus_responder_set(responder, 99, &sum, 1);
}

static void *serve_ack(void *arg) {
  ServerThread *thread = (ServerThread *)arg;
  thread->rc = peerbus_ack_server_serve_one(thread->server, 3000, put_handler, NULL);
  return NULL;
}

int main(void) {
  PeerbusNode *node = peerbus_node_new("c-put-ack-demo", true);
  if (!node) {
    fprintf(stderr, "node: %s\n", peerbus_last_error_message());
    return 1;
  }
  PeerbusAckServer *server = peerbus_ack_server_new(node, "c/upload");
  PeerbusPutClient *client = peerbus_put_client_new(node, "c-put-ack-demo", "c/upload");
  if (!server || !client) {
    fprintf(stderr, "setup: %s\n", peerbus_last_error_message());
    return 1;
  }

  ServerThread thread = {.server = server, .rc = 0};
  pthread_t tid;
  pthread_create(&tid, NULL, serve_ack, &thread);

  const uint8_t a[] = {1, 2};
  const uint8_t b[] = {3, 4};
  PeerbusRawMessage items[] = {
      {.kind = 1, .data = {.ptr = a, .len = sizeof a}},
      {.kind = 1, .data = {.ptr = b, .len = sizeof b}},
  };
  PeerbusMessage *ack = NULL;
  bool ok = peerbus_put_client_put(client, items, 2, &ack);
  pthread_join(tid, NULL);

  int status = 1;
  if (ok && ack && thread.rc == 1) {
    PeerbusBytes bytes = peerbus_message_data(ack);
    status = (peerbus_message_kind(ack) == 99 && bytes.len == 1 && bytes.ptr[0] == 10) ? 0 : 1;
    peerbus_message_free(ack);
  } else {
    fprintf(stderr, "put: %s\n", peerbus_last_error_message());
  }

  peerbus_put_client_free(client);
  peerbus_ack_server_free(server);
  peerbus_node_free(node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
