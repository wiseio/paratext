#!/usr/bin/env Rscript
#
# test_readcsv.R in.csv out.json
#
# Loads the file in.csv into an R data frame. Sums its numeric
# columns. Outputs the run times and memory usage to the JSON file
# out.json.

require("rjson", quietly=TRUE)

memory_usage <- function(){
 return(strtoi(system(paste("ps -o rss ", Sys.getpid(), "| tail -1"), intern=TRUE))*1024)
}

args <- commandArgs(TRUE)
filename <- args[1]
result_filename <- args[2]
load_tic <- Sys.time()
df = read.csv(filename)
load_toc <- Sys.time()
load_time <- as.double(difftime(load_toc, load_tic, units="secs"))

mem <- memory_usage()

sum_tic <- Sys.time()
s <- colSums(Filter(is.numeric, df))
s2 <- apply(Filter(function(x){!is.numeric(x)}, df), 2, function(x){sum(nchar(x))})
sum_toc <- Sys.time()
sum_time <- as.double(difftime(sum_toc, sum_tic, units="secs"))

results = list(cmd = "R-readcsv", load_time = load_time, mem = mem, sum_time = sum_time)
json = rjson::toJSON(results)
write(json, result_filename)
