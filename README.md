# pyspark_assessment

In this repo is the pyspark assessment answers for Python Developer (Data Engineer) Assessment
* pyspark-assessment.ipynb - jupyter noteboook pyspark assessment 
* passcode.txt - passcode used to run aes_encyrpt on PII data 
* uszips.csv - reference data for us zipcodes to determine US state

Handling PII Data

for Handling PII data aes_encrypt function to obfuscate PII data , by passing in a passcode. data can be decrypted by using this function with the same passcode https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.aes_decrypt.html

```
aes_encrypt(cc_num, '"+str(passcode)+"', 'ECB', 'PKCS')
 ```


Data Quality Assurance 

for Data Quality , we have identified that the personal_detail.person_name field had alot of dirty data  (e.g. Jennifer,Banks,eeeee , Edward@Sanchez ). Method of cleaning up data is using regex to remove special chars like ,@! and to be replace with " " which would then using split function to get First Name and Last Name 

``` 
regexp_replace(regexp_replace(col('personal_detail.person_name'), ",|@|\||!|/|\."," "),"\s+"," ").alias('Person Name')
```
