# Requires LDA_ROOT to be defined
LDA_ST = $(LDA_ROOT)/src/light_lda_st
LDA_ST_BIN = $(LDA_ROOT)/bin
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
           -fno-omit-frame-pointer


LDA_INCFLAGS = -I$(LDA_THIRD_PARTY_INCLUDE)

LDA_LDFLAGS = -Wl,-rpath,$(LDA_THIRD_PARTY_LIB) \
          -L$(LDA_THIRD_PARTY_LIB) \
          -pthread \
          -lglog \
          -lgflags
