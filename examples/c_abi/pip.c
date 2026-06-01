#include "quicbit.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

typedef struct {
  QuicbitPipServer *server;
  int rc;
} ServerThread;

static void echo_handler(void *ctx,
                         const QuicbitMessages *items,
                         QuicbitMessageResponder *responder) {
  (void)ctx;
  for (uintptr_t i = 0; i < quicbit_messages_len(items); i++) {
    uint8_t out[16];
    QuicbitBytes bytes = quicbit_messages_data_at(items, i);
    uintptr_t len = bytes.len > sizeof out ? sizeof out : bytes.len;
    for (uintptr_t j = 0; j < len; j++) {
      out[j] = (uint8_t)(bytes.ptr[j] * 2);
    }
    quicbit_message_responder_send(responder, quicbit_messages_kind_at(items, i), out, len);
  }
}

static void *serve_pip(void *arg) {
  ServerThread *thread = (ServerThread *)arg;
  thread->rc = quicbit_pip_server_serve_one(thread->server, 3000, echo_handler, NULL);
  return NULL;
}

int main(void) {
  QuicbitNode *node = quicbit_node_new("c-pip-demo", true);
  if (!node) {
    fprintf(stderr, "node: %s\n", quicbit_last_error_message());
    return 1;
  }
  QuicbitPipServer *server = quicbit_pip_server_new(node, "c/session");
  QuicbitPipClient *client = quicbit_pip_client_new(node, "c-pip-demo", "c/session");
  if (!server || !client) {
    fprintf(stderr, "setup: %s\n", quicbit_last_error_message());
    return 1;
  }

  ServerThread thread = {.server = server, .rc = 0};
  pthread_t tid;
  pthread_create(&tid, NULL, serve_pip, &thread);

  const uint8_t a[] = {2, 4};
  QuicbitRawMessage items[] = {{.kind = 7, .data = {.ptr = a, .len = sizeof a}}};
  QuicbitMessages *replies = NULL;
  bool ok = quicbit_pip_client_exchange(client, items, 1, &replies);
  pthread_join(tid, NULL);

  int status = 1;
  if (ok && replies && thread.rc == 1 && quicbit_messages_len(replies) == 1) {
    QuicbitBytes bytes = quicbit_messages_data_at(replies, 0);
    status = (quicbit_messages_kind_at(replies, 0) == 7 && bytes.len == 2 && bytes.ptr[0] == 4 &&
              bytes.ptr[1] == 8)
                 ? 0
                 : 1;
    quicbit_messages_free(replies);
  } else {
    fprintf(stderr, "exchange: %s\n", quicbit_last_error_message());
  }

  quicbit_pip_client_free(client);
  quicbit_pip_server_free(server);
  quicbit_node_free(node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
