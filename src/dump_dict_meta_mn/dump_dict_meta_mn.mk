# DUMP BINARY DOC MN Makefile
DUMP_DICT_META_MN_SRC = $(wildcard $(DUMP_DICT_META_MN)/*.cpp)
DUMP_DICT_META_MN_HDR = $(wildcard $(DUMP_DICT_META_MN)/*.hpp)
DUMP_DICT_META_MN_OBJ = $(DUMP_DICT_META_MN_SRC:.cpp=.o)

dump_dict_meta_mn_all: dump_dict_meta_mn

dump_dict_meta_mn: $(DUMP_DICT_META_MN_BIN)/dump_dict_meta_mn

$(DUMP_DICT_META_MN_BIN)/dump_dict_meta_mn: $(DUMP_DICT_META_MN_OBJ) $(DUMP_DICT_META_MN_BIN)
	$(LDA_CXX) $(LDA_CXXFLAGS) $(LDA_INCFLAGS) \
	$(DUMP_DICT_META_MN_OBJ) $(LDA_LDFLAGS) -o $@

$(DUMP_DICT_META_MN_OBJ): %.o: %.cpp $(DUMP_DICT_META_MN_HDR)
	$(LDA_CXX) $(LDA_CXXFLAGS) -Wno-unused-result $(LDA_INCFLAGS) -c $< -o $@

dump_dict_meta_mn_clean:
	rm -rf $(DUMP_DICT_META_MN_OBJ)
	rm -rf $(dump_dict_meta_mn)

.PHONY: dump_dict_meta_mn_clean dump_dict_meta_mn_all
