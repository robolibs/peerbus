#include "quicbit.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

typedef struct {
  QuicbitAckServer *server;
  int rc;
} ServerThread;

static void upload_handler(void *ctx, const QuicbitMessages *items, QuicbitResponder *responder) {
  (void)ctx;
  uint8_t sum = 0;
  for (uintptr_t i = 0; i < quicbit_messages_len(items); i++) {
    QuicbitBytes bytes = quicbit_messages_data_at(items, i);
    for (uintptr_t j = 0; j < bytes.len; j++) {
      sum = (uint8_t)(sum + bytes.ptr[j]);
    }
  }
  quicbit_responder_set(responder, 99, &sum, 1);
}

static void *serve_ack(void *arg) {
  ServerThread *thread = (ServerThread *)arg;
  thread->rc = quicbit_ack_server_serve_one(thread->server, 3000, upload_handler, NULL);
  return NULL;
}

int main(void) {
  QuicbitNode *node = quicbit_node_new("c-put-ack-demo", true);
  if (!node) {
    fprintf(stderr, "node: %s\n", quicbit_last_error_message());
    return 1;
  }
  QuicbitAckServer *server = quicbit_ack_server_new(node, "c/upload");
  QuicbitPutClient *client = quicbit_put_client_new(node, "c-put-ack-demo", "c/upload");
  if (!server || !client) {
    fprintf(stderr, "setup: %s\n", quicbit_last_error_message());
    return 1;
  }

  ServerThread thread = {.server = server, .rc = 0};
  pthread_t tid;
  pthread_create(&tid, NULL, serve_ack, &thread);

  const uint8_t a[] = {1, 2};
  const uint8_t b[] = {3, 4};
  QuicbitRawMessage items[] = {
      {.kind = 1, .data = {.ptr = a, .len = sizeof a}},
      {.kind = 1, .data = {.ptr = b, .len = sizeof b}},
  };
  QuicbitMessage *ack = NULL;
  bool ok = quicbit_put_client_upload(client, items, 2, &ack);
  pthread_join(tid, NULL);

  int status = 1;
  if (ok && ack && thread.rc == 1) {
    QuicbitBytes bytes = quicbit_message_data(ack);
    status = (quicbit_message_kind(ack) == 99 && bytes.len == 1 && bytes.ptr[0] == 10) ? 0 : 1;
    quicbit_message_free(ack);
  } else {
    fprintf(stderr, "upload: %s\n", quicbit_last_error_message());
  }

  quicbit_put_client_free(client);
  quicbit_ack_server_free(server);
  quicbit_node_free(node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
