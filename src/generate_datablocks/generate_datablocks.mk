# Makefile for generating datablocks
GENERATE_DATABLOCKS_SRC = $(wildcard $(GENERATE_DATABLOCKS)/*.cpp)
GENERATE_DATABLOCKS_HDR = $(wildcard $(GENERATE_DATABLOCKS)/*.hpp)
GENERATE_DATABLOCKS_OBJ = $(GENERATE_DATABLOCKS_SRC:.cpp=.o)

generate_datablocks_all: generate_datablocks 

generate_datablocks: $(GENERATE_DATABLOCKS_BIN)/generate_datablocks

$(GENERATE_DATABLOCKS_BIN)/generate_datablocks: $(GENERATE_DATABLOCKS_OBJ) $(GENERATE_DATABLOCKS_BIN)
	$(LDA_CXX) $(LDA_CXXFLAGS) $(LDA_INCFLAGS) \
	$(GENERATE_DATABLOCKS_OBJ) $(LDA_LDFLAGS) -o $@

$(GENERATE_DATABLOCKS_OBJ): %.o: %.cpp $(GENERATE_DATABLOCKS_HDR)
	$(LDA_CXX) $(LDA_CXXFLAGS) -Wno-unused-result $(LDA_INCFLAGS) -c $< -o $@

generate_datablocks_clean:
	rm -rf $(GENERATE_DATABLOCKS_OBJ)
	rm -rf $(generate_datablocks)

.PHONY: generate_datablocks_clean generate_datablocks
