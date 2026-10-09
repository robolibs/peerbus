#include "peerbus.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(void) {
  PeerbusNode *seed = peerbus_node_new(NULL, true);
  if (!seed) {
    fprintf(stderr, "seed: %s\n", peerbus_last_error_message());
    return 1;
  }
  char *system_did = peerbus_node_did_key(seed);
  peerbus_node_free(seed);
  if (!system_did) {
    fprintf(stderr, "did: %s\n", peerbus_last_error_message());
    return 1;
  }

  PeerbusNodeConfig cfg = {
      .identity = NULL,
      .no_relay = true,
      .system_did = system_did,
      .max_payload_bytes = 0,
      .history_depth = 8,
      .subscriber_buffer = 8,
  };
  PeerbusNode *pub_node = peerbus_node_new_with_config(cfg);
  PeerbusNode *sub_node = peerbus_node_new_with_config(cfg);
  peerbus_string_free(system_did);
  if (!pub_node || !sub_node) {
    fprintf(stderr, "nodes: %s\n", peerbus_last_error_message());
    return 1;
  }

  PeerbusTopicQos qos = peerbus_topic_qos_latest();
  PeerbusPublisher *pub = peerbus_publisher_new_with_qos(pub_node, "c/system", qos);
  PeerbusSubscriber *sub = peerbus_subscribe_new_with_qos(sub_node, "c/system", qos);
  if (!pub || !sub) {
    fprintf(stderr, "setup: %s\n", peerbus_last_error_message());
    return 1;
  }

  const uint8_t payload[] = {'s', 'y', 's'};
  peerbus_publisher_send(pub, 55, payload, sizeof payload);

  PeerbusMessage *msg = NULL;
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
    status = (peerbus_message_kind(msg) == 55 && bytes.len == sizeof payload &&
              memcmp(bytes.ptr, payload, bytes.len) == 0)
                 ? 0
                 : 1;
    peerbus_message_free(msg);
  } else {
    fprintf(stderr, "take: %s\n", peerbus_last_error_message());
  }

  peerbus_subscriber_free(sub);
  peerbus_publisher_free(pub);
  peerbus_node_free(sub_node);
  peerbus_node_free(pub_node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
