
LDA_DIR = $(LIGHT_LDA)/src
LDA_BIN = $(LIGHT_LDA_BIN)

LDA_CXX += -I$(LDA_DIR)
LDA_SRC = $(shell find $(LDA_DIR) -type f -name "*.cpp")
LDA_HEADERS = $(shell find $(LDA_DIR) -type f -name "*.hpp" -o -name "*.h")
LDA_OBJ = $(LDA_SRC:.cpp=.o)

lda_all: $(LDA_BIN)/lda_main

$(LDA_BIN)/lda_main: $(LDA_OBJ)
	$(LDA_CXX) $(LDA_CXXFLAGS) $(LDA_INCFLAGS) \
	$(LDA_OBJ) $(LDA_LDFLAGS) -o $@

$(LDA_OBJ): %.o: %.cpp $(LDA_HEADERS)
	$(LDA_CXX) $(LDA_CXXFLAGS) $(LDA_INCFLAGS) -c $< -o $@

light_lda_clean:
	rm -rf $(LDA_OBJ) $(lda_all)

.PHONY: lda_all light_lda_clean
