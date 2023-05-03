#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from pyspark.sql import *
from pyspark import StorageLevel
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
spark = SparkSession.builder.getOrCreate()
def printall():
    df.show()
    df.count()
def limitedrows():
    c = int(input('Enter number of rows to be disaplyed'))
    print(df.take(c))
       
def sortingdataset():
    e=df1.map(lambda x: x.split(','))
    c=e.map(lambda x: (x[2],x[3])).sortByKey(0).take(10)  
    d=e.map(lambda x: (x[2],x[6])).sortByKey(0).take(10)
    print("After sorting the data")
    print(c)
    print(d)
         
def filtering():
    print('filtering the persons who is having age greater than 25')
    c=df.filter(df["Age"] == "25")
    print(c.count())
    print('number of people who is having honda and is Single ')
    d1=df.filter((df["Make"]=="Honda") & (df["MaritalStatus"] == "Single"))
    print(d1.count())
    
def agegroup():
    age = int(input())
    c = df.filter((df["Age"] == age))
    if c.count()>0:
        df.select("Age","MaritalStatus","Sex","Month","DriverRating","PolicyNumber").filter(df.Age == age).show()
    else:
        print('The entered  age group is not present in dataset')
        
def persist():
    df.persist(StorageLevel.DISK_ONLY)
    result = df.filter(df['Make'] == "Mazda").show()
    df.unpersist()
    
def BasePolicy():
    df.filter(col("BasePolicy")== 'Liability').show()

def AccidentArea():
    df.filter(col('AccidentArea').isin(['Rural'])).show()
    
def MonthClaimed():
    inp = input("enter monthclaimed you want to view ")
    df.select("Month","MonthClaimed" , "Make" ,"Year").filter((df["MonthClaimed"] == inp)).show()
    
def PolicyType():
    k = df.filter((df["PolicyType"] == "Sedan - Liability") & (df["Sex"] == "Female"))
    k.show()
    
def VehiclePrice():
      df.filter((col("VehiclePrice") >= "60000") & (col("DriverRating") >= "4")).show()
        
def Make():
    inp = input("enter make you want to view ")
    df.select("Month","Make" ,"Year","Sex").filter((df["Make"] == inp)).show()
    
        
def partioningofdataaccordingtomake():
    df.groupby('Make').count().orderBy("count",ascending = True).show()
    
def tofetch_Third_Party():
    df.createOrReplaceTempView("tableA")
    c=spark.sql("select Age,MaritalStatus,Make from tableA where Fault='Third Party' ")
    c.show()
    d=spark.sql("select FraudFound_P,Make from tableA where FraudFound_p = '1' ")
    d.show()
    
    


def visulizedataset():
    pd1=df.toPandas()
    plt.figure(figsize=(4,4))
    sns.barplot(x='Month',y='Age', data=pd1)
    plt.title("Month vs Age")
   
    
    
while True:
    df = spark.read.csv("D:\\Users\\DELL\\Downloads\\fraud_oracle.csv",inferSchema=True,header=True)
    df1 = spark.sparkContext.textFile("D:\\Users\\DELL\\Downloads\\fraud_oracle.csv")
    print('---1.to view  complete dataset--- ')
    print('---2.to view limited rows as per user required---')
    print('---3.to perfrom sorting operation on dataset---')
    print('---4.to perfrom filter operation on dataset---')
    print('---5.to display data of particular age group---')
    print('---6. to persist data---')
    print('---7.to dispaly the BasePolicy---')
    print('---8.to display the AccidentArea---')
    print('---9.to display the MonthClaimed---')
    print('---10.to display the PolicyType---')
    print('---11.to display the VehiclePrice---')
    print('---12.to perform Make---')
    print('---13.to print details of Fault where Fault=Third Party---')
    print('---14.to perform Partitioning dataset on columns---')
    print('---15.to visualizedata---')
    print('---Enter any number to exit---')
    choice = int(input('Enter a number to choose to perfrom an action :'))
    if choice == 1:
        printall()
        print('To show complete data set')
    elif choice == 2:
        limitedrows()
    elif choice == 3:
        sortingdataset()
        print('To perform sort operation on dataset')
    elif choice == 4:
        filtering()
        print('To filter dataset')
    elif choice == 5:
        print('to display data of particular age group')
        agegroup()
    elif choice == 6:
        print('to persist data')
        persist()
    elif choice == 7:
        print('to display the BasePolicy')
        BasePolicy()
    elif choice == 8:
        print('to display the AccidentArea')
        AccidentArea()
    elif choice == 9:
        print('to display the MonthClaimed')
        MonthClaimed()
    elif choice == 10:
        print('to display the PolicyType')
        PolicyType()
    elif choice == 11:
        print('to display the VehiclePrice')
        VehiclePrice()
    elif choice == 12:
        print('to perform Make')
        Make()
    elif choice == 13:
        print('to fetch the age maritial status and make where fault =third party')
        tofetch_Third_Party()
    elif choice == 14:
        partioningofdataaccordingtomake()
        print('Partitioning dataframe on column ‘Make’')
    elif choice == 15:
        visulizedataset()
    else:
        print('The number you enterted may not exist or please choose in range between from 1 and 11')
        break


# In[ ]:




