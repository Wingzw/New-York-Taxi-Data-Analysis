library(parallel)

#list the file names
f = list.files("/home/data/NYCTaxis/", pattern = "fare.*\\.csv$", full.names = TRUE)

#get the frenquency table of total amount less the tolls
getFee = function(filename){
  
  #read the file and get total amount less the tolls
  fee = as.numeric(system2("awk", args=c(' -F"," \'NR>1{print $11-$10}\'',filename),
                           stdout = T))
  z = round(fee,digits = 2)
  return(table(z))
  
}

#make a cluster of 12 node and let each node read one file
cl = makeCluster(12, "FORK")
els =  clusterSplit(cl, f)

wc = clusterApply(cl, els, function(x) getFee(x))
stopCluster(cl) 

#merge all informatin in the master node
wc2 = unlist(wc)
v = data.frame(id = names(wc2), freq = wc2)
library(plyr)
z = aggregate(freq ~ id, data = v, sum)

#quantile function
quantile.from.freq = function(x,freq,quant) {
  ord = order(x)
  x = x[ord]
  freq = freq[ord]
  cs = cumsum(freq)/sum(freq)
  return(x[max(which(cs<quant))+1])
}

#get all quantiles
z$id = as.numeric(as.character(z$id))
quant = seq(0.1, 0.9, 0.1)
res = sapply(quant, function(i) {quantile.from.freq(z$id,z$freq,i)})
names(res) = quant


