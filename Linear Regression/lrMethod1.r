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
  trip_time <<-output[[3]]
  fee <<-output[[4]]
  xy = sum(output[[3]]*output[[4]])
  x2 = sum(output[[3]]^2)
  n <<- numLines - 1
  sumx = sum(output[[3]])
  sumy = sum(output[[4]])
  return(c(n = n,sumxy = xy, sumx2 = x2, sumx = sumx, sumy = sumy))
  
}

anovaLR = function(filename,beta_0,beta_1,ymean){
   pred = beta_0 + beta_1*trip_time
   residual = fee - pred
   sse = sum(residual^2)
   ssr = sum((pred - ymean)^2)
   return(c(sse = sse, ssr = ssr))
}

library(parallel)

clzw = makeCluster(12,"FORK")

els = clusterSplit(clzw, f1)
wc = clusterApply(clzw, els, function(x) lr(x))

res = c(n = 0, sumxy = 0, sumx2 = 0, sumx = 0, sumy = 0)
for(i in 1:12){
   res = res + wc[[i]]
}

up1 = res[2] - res[4]*res[5]/res[1]
down1 = res[3] - res[4]*res[4]/res[1]

beta_1 = up1 / down1

beta_0 = res[5]/res[1] - beta_1*res[4]/res[1]

xmean = res[4]/res[1]
ymean = res[4] / res[1]

clusterExport(clzw,"beta_0", environment())
clusterExport(clzw,"beta_1", environment())
clusterExport(clzw,"ymean", environment())

wc2 = clusterApply(clzw, els, function(x) anovaLR(x,beta_0, beta_1, ymean))
stopCluster(clzw)

res2 = c(sse = 0, ssr=0)
for(i in 1:12){
   res2 = res2 + wc2[[i]]
}
sse = res2[1]
ssr = res2[2]
mse = sse/(res[1] - 2)
msr = ssr
fStar = msr/mse
ssto = sse + ssr
msto = ssto/(res[1]-1)
