#include "peerbus.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

typedef struct {
  PeerbusReqServer *server;
  int rc;
} ServerThread;

static void double_handler(void *ctx,
                           uint64_t kind,
                           const uint8_t *data,
                           uintptr_t len,
                           PeerbusResponder *responder) {
  (void)ctx;
  uint8_t out[16];
  if (len > sizeof out) {
    len = sizeof out;
  }
  for (uintptr_t i = 0; i < len; i++) {
    out[i] = (uint8_t)(data[i] * 2);
  }
  peerbus_responder_set(responder, kind, out, len);
}

static void *serve_req(void *arg) {
  ServerThread *thread = (ServerThread *)arg;
  thread->rc = peerbus_req_server_serve_one(thread->server, 3000, double_handler, NULL);
  return NULL;
}

int main(void) {
  /* NULL secret key => ephemeral (random) identity. */
  PeerbusNode *node = peerbus_node_new(NULL, true);
  if (!node) {
    fprintf(stderr, "node: %s\n", peerbus_last_error_message());
    return 1;
  }
  PeerbusReqServer *server = peerbus_req_server_new(node, "c/double");
  /* Address the peer by id: this node's own endpoint address string.
     Same host => the client attaches over shared memory. */
  char *self_addr = peerbus_node_endpoint_addr(node);
  PeerbusReqClient *client =
      self_addr ? peerbus_req_client_new(node, self_addr, "c/double") : NULL;
  peerbus_string_free(self_addr);
  if (!server || !client) {
    fprintf(stderr, "setup: %s\n", peerbus_last_error_message());
    return 1;
  }

  ServerThread thread = {.server = server, .rc = 0};
  pthread_t tid;
  pthread_create(&tid, NULL, serve_req, &thread);

  const uint8_t input[] = {1, 2, 3};
  PeerbusMessage *reply = NULL;
  bool ok = peerbus_req_client_call(client, 7, input, sizeof input, &reply);
  pthread_join(tid, NULL);

  int status = 1;
  if (ok && reply && thread.rc == 1) {
    PeerbusBytes bytes = peerbus_message_data(reply);
    status = (peerbus_message_kind(reply) == 7 && bytes.len == 3 && bytes.ptr[0] == 2 &&
              bytes.ptr[1] == 4 && bytes.ptr[2] == 6)
                 ? 0
                 : 1;
    peerbus_message_free(reply);
  } else {
    fprintf(stderr, "call: %s\n", peerbus_last_error_message());
  }

  peerbus_req_client_free(client);
  peerbus_req_server_free(server);
  peerbus_node_free(node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
