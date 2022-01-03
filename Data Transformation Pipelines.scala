// Databricks notebook source
/*
Loading this data file using the SparkContext will return RDD data APIs.
*/
// loading the textfiles using EDD data API

val textFile = spark.sparkContext.textFile("/FileStore/tables/Text Corpus/*.text")
// print the number of lines across all the files
textFile.count()

// COMMAND ----------

// print the RDD content 
textFile.collect.foreach(println)

// COMMAND ----------

/* Get the number of antibiotics entries */
val AntibioticsEntries= textFile.filter(line => line.contains("antibiotics"))
AntibioticsEntries.count()

// COMMAND ----------

/* Get the number of patient entries */
val patient= textFile.filter(line => line.contains("patient"))
patient.count()

// COMMAND ----------

/* Get the number of admitted entries */
val admitted= textFile.filter(line =>line.contains("admitted"))
admitted.count()


// COMMAND ----------

//transformation pipeline 
val patient= textFile.filter(line => line.contains("patient")) // get the lines which have patient from the textfile
val PatientAdmitted= patient.filter(line =>line.contains("admitted")) //get the lines which have admitted from the lines which has patient 

reducedByKey.collect.foreach(println)  //print the lines which have both patient and admitted
reducedByKey.count() /*  = 7  */ // print the count number of the lines which have both patient and admitted

// COMMAND ----------

// Another method of print the count number of the lines which have both patient and admitted
val patient_admitted= textFile.filter(line => line.contains("patient") && line.contains("admitted"))
patient_admitted.count() // = 7

