#!/usr/bin/env Rscript
#
# test_fread.R in.csv out.json
#
# Loads the file in.csv into an R data frame with fread, sums its numeric
# columns, and outputs the run times and memory usage to the JSON file
# out.json.

require("data.table", quietly=TRUE)
require("rjson", quietly=TRUE)

"OlsonNames" = function () 
{
    if (.Platform$OS.type == "windows") 
        tzdir <- Sys.getenv("TZDIR", file.path(R.home("share"), 
            "zoneinfo"))
    else {
        tzdirs <- c(Sys.getenv("TZDIR"), file.path(R.home("share"), 
            "zoneinfo"), "/usr/share/zoneinfo", "/usr/share/lib/zoneinfo", 
            "/usr/lib/zoneinfo", "/usr/local/etc/zoneinfo", "/etc/zoneinfo", 
            "/usr/etc/zoneinfo")
        tzdirs <- tzdirs[file.exists(tzdirs)]
        if (!length(tzdirs)) {
            warning("no Olson database found")
            return(character())
        }
        else tzdir <- tzdirs[1]
    }
    x <- list.files(tzdir, recursive = TRUE)
    grep("^[ABCDEFGHIJKLMNOPQRSTUVWXYZ]", x, value = TRUE)
}

memory_usage <- function(){
 return(strtoi(system(paste("ps -o rss ", Sys.getpid(), "| tail -1"), intern=TRUE))*1024)
}

args <- commandArgs(TRUE)
filename <- args[1]
result_filename <- args[2]
load_tic <- Sys.time()
df = fread(filename)
load_toc <- Sys.time()
load_time <- as.double(difftime(load_toc, load_tic, units="secs"))

mem <- memory_usage()

sum_tic <- Sys.time()
s <- colSums(Filter(is.numeric, df))
s <- s + apply(Filter(function(x){!is.numeric(x)}, df), 2, function(x){sum(nchar(x))})
sum_toc <- Sys.time()
sum_time <- as.double(difftime(sum_toc, sum_tic, units="secs"))

results = list(cmd = "R-fread", load_time = load_time, mem = mem, sum_time = sum_time)
json = rjson::toJSON(results)
write(json, result_filename)
