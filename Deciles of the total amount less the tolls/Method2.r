f = list.files("/home/data/NYCTaxis/", pattern = "fare.*\\.csv$", full.names = TRUE)

dyn.load("getFee.so")


getFee = function(filename){
  numLines = as.integer(system2("wc",
                                args = c("-l", filename,
                                         " | awk '{print $1}'"),
                                stdout = TRUE))

  fee = rep(0, numLines - 1)

  output = .C("getFee", as.character(filename),as.numeric(fee))

  z = round(output[[2]],digits = 2)
  return(table(z))

}

library(parallel)
cl = makeCluster(12, "FORK")
els =  clusterSplit(cl, f)

wc = clusterApply(cl, els, function(x) getFee(x))

stopCluster(cl)

wc2 = unlist(wc)

v = data.frame(id = names(wc2), freq = wc2)
library(plyr)
z = aggregate(freq ~ id, data = v, sum)


z$id = as.numeric(as.character(z$id))

quantile.from.freq = function(x,freq,quant) {
  ord = order(x)
  x = x[ord]
  freq = freq[ord]
  cs = cumsum(freq)/sum(freq)
  return(x[max(which(cs<quant))+1])
}

quant = c(0.1,0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)

res = sapply(quant, function(i) {quantile.from.freq(z$id,z$freq,i)})

names(res) = quant


