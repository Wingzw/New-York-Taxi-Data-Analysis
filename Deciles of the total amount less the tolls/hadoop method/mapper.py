#mapper.py

import sys
for line in sys.stdin:
	line = line.strip()
	if(line.startswith('medallion') == 0):
		tmp = line.split(',')
		word = round(float(tmp[-1])- float(tmp[-2]),2)
		print "%f\t%s" % (word, 1)

