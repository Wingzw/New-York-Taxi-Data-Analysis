f1 = list.files("/home/data/NYCTaxis/", pattern = "fare.*\\.csv$", full.names = TRUE)

f2 = list.files("/home/data/NYCTaxis/", pattern = "data.*\\.csv$", full.names = TRUE)

dyn.load("getX2Y.so")

mlr = function(filename){
  numLines = as.integer(system2("wc",
                                args = c("-l", filename,
                                         " | awk '{print $1}'"),
                                stdout = TRUE))
  
  trip_time = rep(0, numLines - 1)
  fee = rep(0, numLines - 1)
  surcharge = rep(0, numLines - 1)
  
  filename1 = filename
  filename2 = sub("fare", "data",filename)
  
  
  output = .C("getXY", as.character(filename1),as.character(filename2), as.numeric(trip_time),as.numeric(surcharge),as.numeric(fee))
  
  x12 = sum(output[[3]]^2)
  x1 = sum(output[[3]])
  x22 = sum(output[[4]]^2)
  x2 = sum(output[[4]])
  x1y = sum(output[[3]]*output[[5]])
  x2y = sum(output[[4]]*output[[5]])
  x1x2 = sum(output[[3]]*output[[4]])
  y = sum(output[[5]])
  n = numLines - 1
  return(c(n = n,x1 = x1, x12 = x12, x2 = x2, x22 = x22, x1x2=x1x2, x1y=x1y, x2y=x2y, y=y))
  
}
library(parallel)

clzw = makeCluster(12,"FORK")

els = clusterSplit(clzw, f1)
wc = clusterApply(clzw, els, function(x) mlr(x))
stopCluster(clzw)

res = c(n = 0, x1 = 0, x12 = 0, x2 = 0, x22 = 0, x1x2=0, x1y = 0, x2y = 0, y=0)
for(i in 1:12){
   res = res + wc[[i]]
}

sumx12 = res[3] - res[2]*res[2]/res[1]
sumx22 = res[5] - res[4]*res[4]/res[1]
sumx1y = res[7] - res[2]*res[9]/res[1]
sumx2y = res[8] - res[4]*res[9]/res[1]
sumx1x2 = res[6] - res[2]*res[4]/res[1]

beta_1 = (sumx22*sumx1y - sumx1x2*sumx2y)/(sumx12*sumx22 - sumx1x2*sumx1x2)
beta_2 = (sumx12*sumx2y - sumx1x2*sumx1y)/(sumx12*sumx22 - sumx1x2*sumx1x2)
beta_0 = (res[9] - beta_1*res[2] - beta_2*res[4])/res[1]

