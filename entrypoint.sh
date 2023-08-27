#!/bin/bash --login

conda run -n lapis snakemake data/output.txt --cores 1
