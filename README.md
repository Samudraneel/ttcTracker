# Follow this guide

To install pyspark, use the following link:
https://stackoverflow.com/questions/45336367/install-apache-spark-on-ubuntu-for-pyspark

# Open data

The site containing route information:
http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a=ttc&r={{ROUTE_NUMBER}}

Note that for the site above, it's only available cleanly in XML.

# Important pieces in open data

stop_times.txt contains information regarding trip_id, arrival_time, departure_time.

trips.txt contains route_id and trip_id
