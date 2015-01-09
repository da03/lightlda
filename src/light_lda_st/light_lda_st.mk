# Light LDA ST Makefile
LDA_ST_SRC = $(wildcard $(LDA_ST)/*.cpp)
LDA_ST_HDR = $(wildcard $(LDA_ST)/*.hpp)
LDA_ST_OBJ = $(LDA_ST_SRC:.cpp=.o)

lda_st_all: lda_st

lda_st: $(LDA_ST_BIN)/lda_st

$(LDA_ST_BIN)/lda_st: $(LDA_ST_OBJ) $(LDA_ST_BIN)
	$(LDA_CXX) $(LDA_CXXFLAGS) $(LDA_INCFLAGS) \
	$(LDA_ST_OBJ) $(LDA_LDFLAGS) -o $@

$(LDA_ST_OBJ): %.o: %.cpp $(LDA_ST_HDR)
	$(LDA_CXX) $(LDA_CXXFLAGS) -Wno-unused-result $(LDA_INCFLAGS) -c $< -o $@

lda_st_clean:
	rm -rf $(LDA_ST_OBJ)
	rm -rf $(lda_st)

.PHONY: lda_st_clean lda_st_all
