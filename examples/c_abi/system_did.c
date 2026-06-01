#include "quicbit.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(void) {
  QuicbitNode *seed = quicbit_node_new(NULL, true);
  if (!seed) {
    fprintf(stderr, "seed: %s\n", quicbit_last_error_message());
    return 1;
  }
  char *system_did = quicbit_node_did_key(seed);
  quicbit_node_free(seed);
  if (!system_did) {
    fprintf(stderr, "did: %s\n", quicbit_last_error_message());
    return 1;
  }

  QuicbitNodeConfig cfg = {
      .identity = NULL,
      .no_relay = true,
      .system_did = system_did,
      .max_payload_bytes = 0,
      .history_depth = 8,
      .subscriber_buffer = 8,
  };
  QuicbitNode *pub_node = quicbit_node_new_with_config(cfg);
  QuicbitNode *sub_node = quicbit_node_new_with_config(cfg);
  quicbit_string_free(system_did);
  if (!pub_node || !sub_node) {
    fprintf(stderr, "nodes: %s\n", quicbit_last_error_message());
    return 1;
  }

  QuicbitTopicQos qos = quicbit_topic_qos_latest();
  QuicbitPublisher *pub = quicbit_publisher_new_with_qos(pub_node, "c/system", qos);
  QuicbitSubscriber *sub = quicbit_subscribe_new_with_qos(sub_node, "c/system", qos);
  if (!pub || !sub) {
    fprintf(stderr, "setup: %s\n", quicbit_last_error_message());
    return 1;
  }

  const uint8_t payload[] = {'s', 'y', 's'};
  quicbit_publisher_send(pub, 55, payload, sizeof payload);

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
    status = (quicbit_message_kind(msg) == 55 && bytes.len == sizeof payload &&
              memcmp(bytes.ptr, payload, bytes.len) == 0)
                 ? 0
                 : 1;
    quicbit_message_free(msg);
  } else {
    fprintf(stderr, "take: %s\n", quicbit_last_error_message());
  }

  quicbit_subscriber_free(sub);
  quicbit_publisher_free(pub);
  quicbit_node_free(sub_node);
  quicbit_node_free(pub_node);
  printf(status == 0 ? "OK\n" : "MISMATCH\n");
  return status;
}
