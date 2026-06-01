#include "quicbit.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/*
 * Minimal C-side datapod bridge shape.
 *
 * Real datapod C helpers should provide the same pieces: a stable
 * TYPE_HASH/kind and wire bytes. quicbit stays decoupled and only
 * transports kind + bytes.
 */

enum { DEMO_POD_TYPE_HASH = 0xD00D };

static void demo_pod_to_wire(uint32_t value, uint8_t out[4]) {
  out[0] = (uint8_t)(value & 0xff);
  out[1] = (uint8_t)((value >> 8) & 0xff);
  out[2] = (uint8_t)((value >> 16) & 0xff);
  out[3] = (uint8_t)((value >> 24) & 0xff);
}

static uint32_t demo_pod_from_wire(const uint8_t *data, uintptr_t len) {
  if (len != 4) {
    return 0;
  }
  return (uint32_t)data[0] | ((uint32_t)data[1] << 8) | ((uint32_t)data[2] << 16) |
         ((uint32_t)data[3] << 24);
}

int main(void) {
  QuicbitNode *node = quicbit_node_new("c-pod-demo", true);
  if (!node) {
    fprintf(stderr, "node: %s\n", quicbit_last_error_message());
    return 1;
  }

  QuicbitPublisher *pub = quicbit_publisher_new(node, "c/pod");
  QuicbitSubscriber *sub = quicbit_subscriber_new(node, "c-pod-demo", "c/pod");
  if (!pub || !sub) {
    fprintf(stderr, "setup: %s\n", quicbit_last_error_message());
    return 1;
  }

  uint8_t wire[4];
  demo_pod_to_wire(1234, wire);
  if (!quicbit_publisher_send(pub, DEMO_POD_TYPE_HASH, wire, sizeof wire)) {
    fprintf(stderr, "send: %s\n", quicbit_last_error_message());
    return 1;
  }

  QuicbitMessage *msg = NULL;
  int rc = 0;
  for (int i = 0; i < 200 && rc == 0; i++) {
    rc = quicbit_subscriber_take(sub, &msg);
    if (rc == 0) {
      usleep(5000);
    }
  }

  int status = 1;
  if (rc == 1) {
    QuicbitBytes bytes = quicbit_message_data(msg);
    uint32_t value = demo_pod_from_wire(bytes.ptr, bytes.len);
    printf("pod kind=%llu value=%u\n", (unsigned long long)quicbit_message_kind(msg), value);
    status = quicbit_message_kind(msg) == DEMO_POD_TYPE_HASH && value == 1234 ? 0 : 1;
    quicbit_message_free(msg);
  } else {
    fprintf(stderr, "take rc=%d: %s\n", rc, quicbit_last_error_message());
  }

  quicbit_subscriber_free(sub);
  quicbit_publisher_free(pub);
  quicbit_node_free(node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
