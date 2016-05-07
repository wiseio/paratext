require("rjson", quietly=TRUE)

memory_usage <- function(){
 return(strtoi(system(paste("ps -o rss ", Sys.getpid(), "| tail -1"), intern=TRUE))*1024)
}

args <- commandArgs(TRUE)
filename <- args[1]
load_tic <- Sys.time()
df = read.csv(filename)
load_toc <- Sys.time()
load_time <- load_toc - load_tic

mem <- memory_usage()

sum_tic <- Sys.time()
s <- colSums(Filter(is.numeric, df))
sum_toc <- Sys.time()
sum_time <- sum_toc - sum_tic

results = list(cmd = "R-read.csv", load_time = load_time, mem = mem, sum_time = sum_time)
json = rjson::toJSON(results)
write(json, "result.json")