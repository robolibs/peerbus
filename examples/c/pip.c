#include "peerbus.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

typedef struct {
  PeerbusPipServer *server;
  int rc;
} ServerThread;

static void echo_handler(void *ctx,
                         const PeerbusMessages *items,
                         PeerbusMessageResponder *responder) {
  (void)ctx;
  for (uintptr_t i = 0; i < peerbus_messages_len(items); i++) {
    uint8_t out[16];
    PeerbusBytes bytes = peerbus_messages_data_at(items, i);
    uintptr_t len = bytes.len > sizeof out ? sizeof out : bytes.len;
    for (uintptr_t j = 0; j < len; j++) {
      out[j] = (uint8_t)(bytes.ptr[j] * 2);
    }
    peerbus_message_responder_send(responder, peerbus_messages_kind_at(items, i), out, len);
  }
}

static void *serve_pip(void *arg) {
  ServerThread *thread = (ServerThread *)arg;
  thread->rc = peerbus_pip_server_serve_one(thread->server, 3000, echo_handler, NULL);
  return NULL;
}

int main(void) {
  PeerbusNode *node = peerbus_node_new("c-pip-demo", true);
  if (!node) {
    fprintf(stderr, "node: %s\n", peerbus_last_error_message());
    return 1;
  }
  PeerbusPipServer *server = peerbus_pip_server_new(node, "c/session");
  PeerbusPipClient *client = peerbus_pip_client_new(node, "c-pip-demo", "c/session");
  if (!server || !client) {
    fprintf(stderr, "setup: %s\n", peerbus_last_error_message());
    return 1;
  }

  ServerThread thread = {.server = server, .rc = 0};
  pthread_t tid;
  pthread_create(&tid, NULL, serve_pip, &thread);

  const uint8_t a[] = {2, 4};
  PeerbusRawMessage items[] = {{.kind = 7, .data = {.ptr = a, .len = sizeof a}}};
  PeerbusMessages *replies = NULL;
  bool ok = peerbus_pip_client_exchange(client, items, 1, &replies);
  pthread_join(tid, NULL);

  int status = 1;
  if (ok && replies && thread.rc == 1 && peerbus_messages_len(replies) == 1) {
    PeerbusBytes bytes = peerbus_messages_data_at(replies, 0);
    status = (peerbus_messages_kind_at(replies, 0) == 7 && bytes.len == 2 && bytes.ptr[0] == 4 &&
              bytes.ptr[1] == 8)
                 ? 0
                 : 1;
    peerbus_messages_free(replies);
  } else {
    fprintf(stderr, "exchange: %s\n", peerbus_last_error_message());
  }

  peerbus_pip_client_free(client);
  peerbus_pip_server_free(server);
  peerbus_node_free(node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
