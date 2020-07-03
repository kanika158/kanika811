1. For unblocked reading of files, we have used pyspark framework with multithreading module in python
2. N number of threads can be used to read N files at once depending upon the cluster capacity 
3. The code has been made generic so that it can work with any csv files provided to it
4. For tracing updates in database we have used Slowly changing dimension type 2 (SCD-2) approach.
