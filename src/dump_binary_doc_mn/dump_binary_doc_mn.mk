# DUMP BINARY DOC MN Makefile
DUMP_BINARY_DOC_MN_SRC = $(wildcard $(DUMP_BINARY_DOC_MN)/*.cpp)
DUMP_BINARY_DOC_MN_HDR = $(wildcard $(DUMP_BINARY_DOC_MN)/*.hpp)
DUMP_BINARY_DOC_MN_OBJ = $(DUMP_BINARY_DOC_MN_SRC:.cpp=.o)

dump_binary_doc_mn_all: dump_binary_doc_mn

dump_binary_doc_mn: $(DUMP_BINARY_DOC_MN_BIN)/dump_binary_doc_mn

$(DUMP_BINARY_DOC_MN_BIN)/dump_binary_doc_mn: $(DUMP_BINARY_DOC_MN_OBJ) $(DUMP_BINARY_DOC_MN_BIN)
	$(LDA_CXX) $(LDA_CXXFLAGS) $(LDA_INCFLAGS) \
	$(DUMP_BINARY_DOC_MN_OBJ) $(LDA_LDFLAGS) -o $@

$(DUMP_BINARY_DOC_MN_OBJ): %.o: %.cpp $(DUMP_BINARY_DOC_MN_HDR)
	$(LDA_CXX) $(LDA_CXXFLAGS) -Wno-unused-result $(LDA_INCFLAGS) -c $< -o $@

dump_binary_doc_mn_clean:
	rm -rf $(DUMP_BINARY_DOC_MN_OBJ)
	rm -rf $(dump_binary_doc_mn)

.PHONY: dump_binary_doc_mn_clean dump_binary_doc_mn_all
