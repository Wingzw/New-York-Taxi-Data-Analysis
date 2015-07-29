quantile.from.freq = function(x,freq,quant) {
  ord = order(x)
  x = x[ord]
  freq = freq[ord]
  cs = cumsum(freq)/sum(freq)
  return(x[max(which(cs<quant))+1])
}

con = file('final.txt')
data = readLines(con)
close(con)

a = sapply(data, function(i) as.numeric(unlist(strsplit(i,'\t'))))
a = unname(a)

quant = c(0.1,0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)

res = sapply(quant, function(i) {quantile.from.freq(a[1,],a[2,],i)})

names(res) = quant

res
