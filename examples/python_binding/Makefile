SHELL := /bin/bash

ROOT := ../..
PYTHON ?= $(PYO3_PYTHON)
ifeq ($(PYTHON),)
PYTHON := python3
endif
CARGO_BIN ?= $(QUICBIT_CARGO)
RUSTC_BIN ?= $(QUICBIT_RUSTC)
ifeq ($(CARGO_BIN),)
CARGO_BIN := cargo
endif
ifeq ($(RUSTC_BIN),)
RUSTC_BIN := rustc
endif
VENV := .venv
VENV_PYTHON := $(CURDIR)/$(VENV)/bin/python

.PHONY: all develop demo clean

all: demo

$(VENV_PYTHON):
	@$(PYTHON) -m venv $(VENV)
	@$(VENV_PYTHON) -m pip install --upgrade pip maturin

develop: $(VENV_PYTHON)
	@cd $(ROOT) && \
	  VIRTUAL_ENV=$(CURDIR)/$(VENV) \
	  PATH=$(CURDIR)/$(VENV)/bin:$$PATH \
	  PYO3_PYTHON=$(VENV_PYTHON) \
	  CARGO=$(CARGO_BIN) RUSTC=$(RUSTC_BIN) \
	  $(CURDIR)/$(VENV)/bin/maturin develop --features python

demo: develop
	@$(VENV_PYTHON) demo.py

clean:
	@rm -rf $(VENV)
