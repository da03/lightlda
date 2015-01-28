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
LIB = $(PROJECT)/lib

NEED_MKDIR = $(BIN) \
             $(THIRD_PARTY_SRC) \
             $(THIRD_PARTY_LIB) \
             $(THIRD_PARTY_INCLUDE)
			 #$(LIB) \


all: path \
	third_party_all \
	dump_dict_meta_mn_all \
	dump_binary_doc_mn_all \
	lda_all
	#ps_lib \

path: $(NEED_MKDIR)

$(NEED_MKDIR):
	mkdir -p $@

clean: dump_dict_meta_mn_clean \
	dump_binary_doc_mn_clean \
	light_lda_clean 
	rm -rf $(BIN) 
	rm -rf $(LIB)
	#petuum_ps_clean \

distclean: clean
	rm -rf $(filter-out $(THIRD_PARTY)/third_party.mk, \
		            $(wildcard $(THIRD_PARTY)/*))

.PHONY: all path clean distclean

include $(DUMP_DICT_META_MN)/dump_dict_meta_mn.mk
include $(DUMP_BINARY_DOC_MN)/dump_binary_doc_mn.mk
#include $(PETUUM_PS)/petuum_ps.mk
include $(LIGHT_LDA)/light_lda.mk

include $(THIRD_PARTY)/third_party.mk
