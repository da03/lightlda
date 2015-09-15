# Requires LDA_ROOT to be defined

# dump_dict_meta_mn
DUMP_DICT_META_MN = $(LDA_ROOT)/src/dump_dict_meta_mn
DUMP_DICT_META_MN_BIN = $(LDA_ROOT)/bin

# generate_datablocks
GENERATE_DATABLOCKS = $(LDA_ROOT)/src/generate_datablocks
GENERATE_DATABLOCKS_BIN = $(LDA_ROOT)/bin


# light lda
LIGHT_LDA = $(LDA_ROOT)/src/light_lda
LIGHT_LDA_BIN = $(LDA_ROOT)/bin

# third party
LDA_THIRD_PARTY = $(LDA_ROOT)/third_party
LDA_THIRD_PARTY_SRC = $(LDA_THIRD_PARTY)/src
LDA_THIRD_PARTY_INCLUDE = $(LDA_THIRD_PARTY)/include
LDA_THIRD_PARTY_LIB = $(LDA_THIRD_PARTY)/lib
LDA_THIRD_PARTY_BIN = $(LDA_THIRD_PARTY)/bin

LDA_CXX = g++
LDA_CXXFLAGS = -O3 \
           -std=c++11 \
		   -static-libstdc++ \
           -Wall \
		   -Wno-sign-compare \
           -fno-builtin-malloc \
           -fno-builtin-calloc \
           -fno-builtin-realloc \
           -fno-builtin-free \
           -fno-omit-frame-pointer \
		   -DLINUX 



LDA_INCFLAGS = -I$(LDA_ROOT) -I$(LDA_THIRD_PARTY_INCLUDE)

LDA_LDFLAGS = -Wl,-rpath,$(LDA_THIRD_PARTY_LIB) \
          -L$(LDA_THIRD_PARTY_LIB) \
          -pthread \
          -lglog \
          -lgflags \
          -lboost_thread \
          -lboost_system \
		  -lboost_filesystem \
		  -lzmq 
