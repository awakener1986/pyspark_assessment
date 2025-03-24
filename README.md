# pyspark_assessment

In this repo is the pyspark assessment answers for Python Developer (Data Engineer) Assessment
* pyspark-assessment.ipynb - jupyter noteboook pyspark assessment 
* passcode.txt - passcode used to run aes_encyrpt on PII data 
* uszips.csv - reference data for us zipcodes to determine US state



**Handling PII Data**

for Handling PII data aes_encrypt function to obfuscate PII data , by passing in a passcode. data can be decrypted by using this function with the same passcode https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.aes_decrypt.html

```
aes_encrypt(cc_num, '"+str(passcode)+"', 'ECB', 'PKCS')
 ```


**Data Quality Assurance**

for Data Quality , we have identified that the personal_detail.person_name field had alot of dirty data  (e.g. Jennifer,Banks,eeeee , Edward@Sanchez ). Method of cleaning up data is using regex to remove special chars like ,@! and to be replace with " " which would then using split function to get First Name and Last Name 

``` 
regexp_replace(regexp_replace(col('personal_detail.person_name'), ",|@|\||!|/|\."," "),"\s+"," ").alias('Person Name')
```


**JSON Flattening**

to parse personal_detail and personal_detail_address into a schema for the inner json to be able to flatten the json into one row 
``` 
.withColumn("personal_detail", from_json("personal_detail", personal_detail_schema))\
.withColumn("personal_detail_address", from_json("personal_detail.address", address_schema))\
```


**Timestamp Conversion**

For timestamp convertion of the merch_last_update_time and merch_eff_time used the following logic to convert from unix timestemp ( assumptions: that merch_last_update_time were in miliseconds and merch_eff_time is in microseconds , and also the dates now looks more in line with the data )
``` 
from_unixtime(col("merch_last_update_time")/1000,"MM-dd-yyyy HH:mm:ss").alias("Merchant Last Update Time"),\
from_unixtime(col("merch_eff_time")/1000000,"MM-dd-yyyy HH:mm:ss").alias("Merchant Effective Registration Time"),\
```


**Name Derivation**

Method of cleaning up data is using regex to remove special chars like ,@! and to be replace with " " similar to what has been described on Data Quality Assurance section 

``` 
withColumn("First Name", split(col("Person Name"), " ").getItem(0)).\
withColumn("Last Name", split(col("Person Name"), " ").getItem(1))
```

**Visualization and Analysis**

For visualization and analysis I made use of apache superset ( open-source lightweight visualization tool https://superset.apache.org/docs/intro ) , to import the results of the data-set and build a simple dashboard 

![Screenshot 2025-03-25 020614](https://github.com/user-attachments/assets/18e61fe0-dd11-486c-ac3b-6fa298140a6a)

***Credit Card Spending by State***

This was derived from the merchant zip code which does a left join to the uszipcode reference table to determine the US state to be able to display it on a map  

***Credit Card Spending by State***

Pie chart to show spendings done based on merchant_category 

***Credit Card Spending by State Bar Chart***

Similar analytics as the map chart but in a bar chart format to be able to clearly see which state contributes to most spending 

***Credit Card Spending by Merchant***

Display which merchant contributes to most spending done 
