library(parallel)

f1 = list.files("/home/data/NYCTaxis/", pattern = "fare.*\\.csv$", full.names = TRUE)

f2 = list.files("/home/data/NYCTaxis/", pattern = "data.*\\.csv$", full.names = TRUE)

dyn.load("check.so")

#check whether each line of two rows matched,0 means match
unEqual = function(filename){
  numLines = as.integer(system2("wc",
                                args = c("-l", filename,
                                         " | awk '{print $1}'"),
                                stdout = TRUE))
  
  checkEqual = rep(1, numLines - 1)
  
  filename1 = filename
  filename2 = sub("fare", "data",filename)
  
  output = .C("check", as.character(filename1),as.character(filename2), as.integer(checkEqual))
  
  return(sum(output[[3]]))
  
}

clzw = makeCluster(12,"FORK")
els = clusterSplit(clzw, f1)
wc = clusterApply(clzw, els, function(x) unEqual(x))
stopCluster(clzw)

#res ==0 means all lines in two kinds of files matched
res = sum(unlist(wc))
res