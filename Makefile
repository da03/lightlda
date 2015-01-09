# Assuming this Makefile lives in project root directory
PROJECT := $(shell readlink $(dir $(lastword $(MAKEFILE_LIST))) -f)

LDA_ROOT = $(PROJECT)

include $(LDA_ROOT)/defns.mk

# defined in defns.mk
THIRD_PARTY = $(LDA_THIRD_PARTY)
THIRD_PARTY_SRC = $(LDA_THIRD_PARTY_SRC)
THIRD_PARTY_LIB = $(LDA_THIRD_PARTY_LIB)
THIRD_PARTY_INCLUDE = $(LDA_THIRD_PARTY_INCLUDE)
THIRD_PARTY_BIN = $(LDA_THIRD_PARTY_BIN)

BIN = $(PROJECT)/bin

NEED_MKDIR = $(BIN) \
             $(THIRD_PARTY_SRC) \
             $(THIRD_PARTY_LIB) \
             $(THIRD_PARTY_INCLUDE)


all: path \
	third_party_all \
	lda_st_all

path: $(NEED_MKDIR)

$(NEED_MKDIR):
	mkdir -p $@

clean: lda_st_clean
	rm -rf $(BIN)

distclean: clean
	rm -rf $(filter-out $(THIRD_PARTY)/third_party.mk, \
		            $(wildcard $(THIRD_PARTY)/*))

.PHONY: all path clean distclean

include $(LDA_ST)/light_lda_st.mk

include $(THIRD_PARTY)/third_party.mk
