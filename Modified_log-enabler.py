import re
import pandas
import matplotlib.pyplot as plt
import seaborn as sns
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *


class log_enabler:
    spark = SparkSession.builder.appName("Test").getOrCreate()

    file = 'file:///C:\\Users\\M1055990\\Desktop\\New folder\\Log Explorer\\Test Data Legacy Knowledge Builder.xlsx'
    file1 = 'C:\\Users\\M1055990\\Desktop\\New folder\\Utility solution catalog.xlsx'

    with pandas.ExcelFile(file) as xls:
        for sheet_name in xls.sheet_names:
            pdf = pandas.read_excel(xls, sheet_name=sheet_name)

    ydf = pdf.fillna(method='ffill')

    with pandas.ExcelFile(file1) as xls:
        for sheet_name in xls.sheet_names:
            pdf1 = pandas.read_excel(xls, sheet_name=sheet_name)

    mySchema = StructType([StructField("JOB NAME", StringType(), True)
                              , StructField("Date", DateType(), True)
                              , StructField("OWNER", StringType(), True)
                              , StructField("JOB CLASS", StringType(), True)
                              , StructField("JOB ID", StringType(), True)
                              , StructField("RUN TIME", StringType(), True)
                              , StructField("CPU", StringType(), True)
                              , StructField("File name", StringType(), True)
                              , StructField("STEPS IN JCL", StringType(), True)
                              , StructField("UTILITY", StringType(), True)
                              , StructField("COND CODES", StringType(), True)
                              , StructField("RUN STATUS", StringType(), True)
                              , StructField("Inventory info", StringType(), True)
                              , StructField("STARTED TIME", StringType(), True)
                              , StructField("ENDED TIME", StringType(), True)
                              , StructField("System Parameters", StringType(), True)
                              , StructField("Application Program summary", StringType(), True)])

    mySchema1 = StructType([StructField("UTILITIES", StringType(), True)
                               , StructField("CONDITION", StringType(), True)
                               , StructField("ABENDS", StringType(), True)
                               , StructField("ABEND_STATUS", StringType(), True)])

    df = spark.createDataFrame(ydf, schema=mySchema)
    df1 = spark.createDataFrame(pdf1, schema=mySchema1)

    # Abend Count
    Abend_df = df.withColumn('RUN STATUS', F.regexp_replace('RUN STATUS', "['']", ''))
    df_abend = df1.select("ABENDS", "ABEND_STATUS")
    joinedDF = Abend_df.join(df_abend, Abend_df["RUN STATUS"] == df_abend.ABENDS, how='left')
    joinedDF1 = joinedDF.select("RUN STATUS", "ABEND_STATUS")

    count_df = joinedDF1.groupBy("ABEND_STATUS").agg(F.count("ABEND_STATUS").alias('COUNT'))
    count_df = count_df.na.drop()
    count_df.show()

    runstatus_df = count_df.toPandas()

    # pie plot for Abend Count
    Tasks = runstatus_df['COUNT']

    my_labels = runstatus_df['ABEND_STATUS']
    plt.pie(Tasks, labels=my_labels, autopct='%1.0f%%', shadow=True, startangle=140)
    plt.title('Abends')
    plt.axis('equal')
    plt.savefig("C:\\Users\\M1055990\\Desktop\\Graphs\\graph2.png")

    # Parsing Plan , Program and Table
    jobinfo_df = df.select('JOB NAME', 'STEP WISE DATA SETS', 'UTILITY', 'JOB INFO').filter(
        (df.UTILITY == ' IKJEFT01') | (df.UTILITY == 'IKJEFT01'))

    def parsing(x):
        # print(x)
        m = re.search("RUN (.*?) \LIB", x)
        if m is None:
            return "None"
        else:
            return m.group(1)

    def table(x):
        v = re.search("TABLE (\w+)", x)
        if v is None:
            return "None"
        else:
            return v.group(1)

    udffun = F.udf(parsing, returnType=StringType())
    jobinfo_df1 = jobinfo_df.withColumn("PRGM/PLAN", udffun("JOB INFO"))

    udff = F.udf(table, returnType=StringType())
    jobinfo_df2 = jobinfo_df1.withColumn("Table", udff("JOB INFO"))

    # Utility and Category

    utility_df = df.select("UTILITY").alias("UTILITY")
    df_utility = df1.select("UTILITIES", "CONDITION")
    joinedDF = utility_df.join(df_utility, utility_df.UTILITY == df_utility.UTILITIES, how='left')
    joinedDF1 = joinedDF.select("UTILITY", "CONDITION")
    joinedDF1 = joinedDF1.na.fill("COBOL", "CONDITION")

    count_df = joinedDF1.groupBy("CONDITION").agg(F.count("CONDITION").alias('COUNT'))
    count_df.show()

    count_df.toPandas().to_excel('C:\\Users\\M1055990\\Desktop\\fileOutput.xls', sheet_name='Sheet1', index=False)

    plot_df = pandas.read_excel("C:\\Users\\M1055990\\Desktop\\fileOutput.xls")

    # Barplot for Utility count
    plt.clf()
    sns.barplot(x=plot_df["CONDITION"],
                y=plot_df["COUNT"], data=plot_df, edgecolor='red')

    plt.savefig("C:\\Users\\M1055990\\Desktop\\Graphs\\graph.png")

    # Runtime plot
    plt.clf()
    ydf['RUN TIME'] = ydf['RUN TIME'].str.replace('0 days', '')
    ydf['RUN TIME'] = ydf['RUN TIME'].str.replace('.0000', '')
    print(pdf['RUN TIME'])
    y = ydf["RUN TIME"]
    x = ydf["JOB NAME"]

    # plot
    plt.scatter(x, y, data=ydf)
    # beautify the x-labels
    plt.gcf().autofmt_xdate()
    plt.savefig("C:\\Users\\M1055990\\Desktop\\Graphs\\graph1.png")
    # plt.show()


if __name__ == '__main__':
    enabler = log_enabler()
