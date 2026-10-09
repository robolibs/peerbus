/*
 * Minimal C demo: publish and subscribe over peerbus via the C ABI.
 *
 * Build (from the repo root):
 *   cargo build --lib
 *   cc examples/c_abi/pubsub.c -Iinclude -Ltarget/debug -lpeerbus -o /tmp/qb_pubsub
 *   LD_LIBRARY_PATH=target/debug /tmp/qb_pubsub
 *
 * Or:
 *   make -C examples/c_abi pubsub
 *
 * Same-host: publisher and subscriber share one node, so routing goes
 * through shared memory. Point the subscriber at a remote peer's
 * did:key / EndpointAddr string to cross hosts over iroh instead.
 */

#include "peerbus.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(void) {
  PeerbusNode* node = peerbus_node_new("c-demo", true);
  if (!node) {
    fprintf(stderr, "node: %s\n", peerbus_last_error_message());
    return 1;
  }

  char* did = peerbus_node_did_key(node);
  printf("node did: %s\n", did ? did : "(null)");
  peerbus_string_free(did);

  PeerbusPublisher* pub = peerbus_publisher_new(node, "c/topic");
  PeerbusSubscriber* sub = peerbus_subscriber_new(node, "c-demo", "c/topic");
  if (!pub || !sub) {
    fprintf(stderr, "setup: %s\n", peerbus_last_error_message());
    return 1;
  }

  const uint8_t payload[] = {0xDE, 0xAD, 0xBE, 0xEF};
  if (!peerbus_publisher_send(pub, 42, payload, sizeof payload)) {
    fprintf(stderr, "send: %s\n", peerbus_last_error_message());
    return 1;
  }

  PeerbusMessage* msg = NULL;
  int rc = 0;
  for (int i = 0; i < 200 && rc == 0; i++) {
    rc = peerbus_subscriber_take(sub, &msg);
    if (rc == 0) {
      usleep(5000);
    }
  }

  int status = 1;
  if (rc == 1) {
    PeerbusBytes bytes = peerbus_message_data(msg);
    printf("received kind=%llu len=%zu bytes=", (unsigned long long)peerbus_message_kind(msg),
           bytes.len);
    for (size_t i = 0; i < bytes.len; i++) {
      printf("%02X", bytes.ptr[i]);
    }
    printf("\n");
    status = (bytes.len == sizeof payload && memcmp(bytes.ptr, payload, bytes.len) == 0) ? 0 : 1;
    peerbus_message_free(msg);
  } else {
    fprintf(stderr, "take rc=%d: %s\n", rc, peerbus_last_error_message());
  }

  peerbus_subscriber_free(sub);
  peerbus_publisher_free(pub);
  peerbus_node_free(node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
