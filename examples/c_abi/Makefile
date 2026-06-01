SHELL := /bin/bash

ROOT    := ../..
BIN_DIR := $(ROOT)/target/debug/c_abi
LIB_DIR := $(ROOT)/target/debug
LIBS    := -lquicbit -Wl,-rpath,$(shell cd $(ROOT) && pwd)/target/debug -lm -lpthread -ldl
CFLAGS  := -Wall -Wextra -O2 -g -I$(ROOT)/include

DEMOS := pubsub req_res que_ans put_ack pip system_did datapod_bridge
BINS  := $(addprefix $(BIN_DIR)/,$(DEMOS))

.PHONY: all build run clean $(DEMOS)

all: run

$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

$(BIN_DIR)/%: %.c | $(BIN_DIR)
	@cd $(ROOT) && cargo build --lib
	@$(CC) $(CFLAGS) $< -L$(LIB_DIR) $(LIBS) -o $@

build: $(BINS)

run: build
	@for demo in $(BINS); do \
		echo "== $$(basename $$demo) =="; \
		$$demo; \
	done

$(DEMOS): %: $(BIN_DIR)/%
	@$<

clean:
	@rm -rf $(BIN_DIR)
