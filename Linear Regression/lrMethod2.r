f1 = list.files("/home/data/NYCTaxis/", pattern = "fare.*\\.csv$", full.names = TRUE)

f2 = list.files("/home/data/NYCTaxis/", pattern = "data.*\\.csv$", full.names = TRUE)

dyn.load("getXY.so")

lr = function(filename){
  numLines = as.integer(system2("wc",
                                args = c("-l", filename,
                                         " | awk '{print $1}'"),
                                stdout = TRUE))
  
  trip_time = rep(0, numLines - 1)
  fee = rep(0, numLines - 1)
  
  filename1 = filename
  filename2 = sub("fare", "data",filename)
  
  
  output = .C("getXY", as.character(filename1),as.character(filename2), as.numeric(trip_time),as.numeric(fee))
  
  return(data.frame(trip_time = output[[3]], fee = output[[4]]))
  
}
library(parallel)

clzw = makeCluster(12,"FORK")

els = clusterSplit(clzw, f1)
wc = clusterApply(clzw, els, function(x) lr(x))
stopCluster(clzw)

library(biglm)
fit = biglm(fee~trip_time, data= wc[[1]])
for(i in 2:12){fit = update(fit, wc[[i]])}
summary(fit)$mat
