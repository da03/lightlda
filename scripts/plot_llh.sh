#!/usr/bin/bash
# call scripts/plot_llh.py and scripts/plot_llh.m to plot figure
python scripts/plot_llh.py log/lda_main.INFO log/llh.dat && matlab -nodisplay -nodesktop -r "run scripts/plot_llh.m, quit()"
