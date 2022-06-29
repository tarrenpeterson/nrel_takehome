# nrel_takehome

This repository contains the following files and directories:

* __wind_summary.py__: The main script for this assignment, please see example command below for how to run this file
* __data/__: The data directory, where geojson files should be stored for input to the script
* __graphs/__: The graphs directory, where output .png files will be stored after running the script

An example command for running the __wind_summary.py__ script is given below:

```
python wind_summary.py 2012 polygon data/chicago.geojson month
```

The following arguments, in this order, are needed to successfully run the script:

* *year*: The year for which data should be pulled
* *geometry_type*: A string that indicates the type of geometry being input via the geojson file, can be either point or polygon
* *geometry_file*: The geojson file with the area of interest, should be stored in the data directory
* *time_of_interest*: The time period of analysis, can be either month or day