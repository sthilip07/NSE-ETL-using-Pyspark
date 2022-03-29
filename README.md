# NSE-ETL-using-Pyspark
This repository contains Extraction of real live data from National stock Exchange and applied some Transformation and sinking it into CSV File using PySpark .
Along with that animated graph will be generated from the CSV file source

#Do Not forget to install necessary libraries
#If you are having a problem with setting pyspark, feel free to use pandas -> want to know how to do the same exact thing with Panadas?
#Here i got new repo which will use pandas for the same thing done with PySpark

Follow the below procedure to execute this program

1.Execute pd_main.py in your editor or local and let it run for 5 minutes to generate 5 minutes 5 live data
  Note:once you stopped and re-run it will continue only from current timestamp
2.Execute plot_graph.py in your editor or local and it will generate a grapgh figure which will be updated every minute
  Note:Once you stopped and re-run it will start from first
  
Note: All the files should be in same directory or give the exact path to files wherever necessary
