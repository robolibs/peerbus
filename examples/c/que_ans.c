#include "peerbus.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

typedef struct {
  PeerbusAnsServer *server;
  int rc;
} ServerThread;

static void range_handler(void *ctx,
                          uint64_t kind,
                          const uint8_t *data,
                          uintptr_t len,
                          PeerbusAnsResponder *responder) {
  (void)ctx;
  uint8_t start = len > 0 ? data[0] : 0;
  uint8_t count = len > 1 ? data[1] : 0;
  for (uint8_t i = 0; i < count; i++) {
    uint8_t value = (uint8_t)(start + i);
    peerbus_ans_responder_send(responder, kind, &value, 1);
  }
}

static void *serve_ans(void *arg) {
  ServerThread *thread = (ServerThread *)arg;
  thread->rc = peerbus_ans_server_serve_one(thread->server, 3000, range_handler, NULL);
  return NULL;
}

int main(void) {
  PeerbusNode *node = peerbus_node_new("c-que-ans-demo", true);
  if (!node) {
    fprintf(stderr, "node: %s\n", peerbus_last_error_message());
    return 1;
  }
  PeerbusAnsServer *server = peerbus_ans_server_new(node, "c/range");
  PeerbusQueClient *client = peerbus_que_client_new(node, "c-que-ans-demo", "c/range");
  if (!server || !client) {
    fprintf(stderr, "setup: %s\n", peerbus_last_error_message());
    return 1;
  }

  ServerThread thread = {.server = server, .rc = 0};
  pthread_t tid;
  pthread_create(&tid, NULL, serve_ans, &thread);

  const uint8_t input[] = {10, 3};
  PeerbusMessages *answers = NULL;
  bool ok = peerbus_que_client_send(client, 12, input, sizeof input, &answers);
  pthread_join(tid, NULL);

  int status = 1;
  if (ok && answers && thread.rc == 1 && peerbus_messages_len(answers) == 3) {
    status = 0;
    for (uintptr_t i = 0; i < 3; i++) {
      PeerbusBytes bytes = peerbus_messages_data_at(answers, i);
      if (peerbus_messages_kind_at(answers, i) != 12 || bytes.len != 1 ||
          bytes.ptr[0] != (uint8_t)(10 + i)) {
        status = 1;
      }
    }
    peerbus_messages_free(answers);
  } else {
    fprintf(stderr, "send: %s\n", peerbus_last_error_message());
  }

  peerbus_que_client_free(client);
  peerbus_ans_server_free(server);
  peerbus_node_free(node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
