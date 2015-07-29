
for i in `seq 1 12`
do
	wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_${i}.csv.zip
	unzip trip_data_${i}.csv.zip
	mv trip_data_${i}.csv data/
	hadoop fs -put data/trip_data_${i}.csv data/
done

for i in `seq 1 12`
do
	wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_${i}.csv.zip
	unzip trip_fare_${i}.csv.zip
	mv trip_fare_${i}.csv data/
	hadoop fs -put data/trip_fare_${i}.csv data/
done
