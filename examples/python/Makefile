SHELL := /bin/bash

ROOT := ../..
PYTHON ?= $(PYO3_PYTHON)
ifeq ($(PYTHON),)
PYTHON := python3
endif
CARGO_BIN ?= $(PEERBUS_CARGO)
RUSTC_BIN ?= $(PEERBUS_RUSTC)
ifeq ($(CARGO_BIN),)
CARGO_BIN := cargo
endif
ifeq ($(RUSTC_BIN),)
RUSTC_BIN := rustc
endif
VENV := .venv
VENV_PYTHON := $(CURDIR)/$(VENV)/bin/python
DATAPOD_REF ?= 0.4.1
DATAPOD_GIT ?= git+https://codeberg.org/robolibs/datapod.git@$(DATAPOD_REF)

.PHONY: all bind-py develop demo clean

all: demo

$(VENV_PYTHON):
	@$(PYTHON) -m venv $(VENV)
	@$(VENV_PYTHON) -m pip install --upgrade pip maturin

bind-py: $(VENV_PYTHON)
	@$(VENV_PYTHON) -m pip install --upgrade maturin
	@$(VENV_PYTHON) -c 'import datapod; assert datapod.__version__ == "$(DATAPOD_REF)", datapod.__version__' || \
	  $(VENV_PYTHON) -m pip install --force-reinstall --no-deps '$(DATAPOD_GIT)'
	@cd $(ROOT) && \
	  VIRTUAL_ENV=$(CURDIR)/$(VENV) \
	  PATH=$(CURDIR)/$(VENV)/bin:$$PATH \
	  PYO3_PYTHON=$(VENV_PYTHON) \
	  CARGO=$(CARGO_BIN) RUSTC=$(RUSTC_BIN) \
	  $(VENV_PYTHON) -m maturin develop --features python

develop: bind-py

demo: develop
	@$(VENV_PYTHON) demo.py

clean:
	@rm -rf $(VENV)
