import re
import pandas
import matplotlib.pyplot as plt
import seaborn as sns
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *


class log_enabler:
    spark = SparkSession.builder.appName("Test").getOrCreate()

    file = 'C:\\Users\\M1055990\\Desktop\\New folder\\Job log explorer\\ABEND_ANALYSIS1_OP.xlsx'
    file1 = 'C:\\Users\\M1055990\\Desktop\\New folder\\Job log explorer\\cpudata.xlsx'

    with pandas.ExcelFile(file) as xls:
        for sheet_name in xls.sheet_names:
            pdf = pandas.read_excel(xls, sheet_name=sheet_name)

    ydf = pdf.fillna(method='ffill')

    with pandas.ExcelFile(file1) as xls:
        for sheet_name in xls.sheet_names:
            pdf1 = pandas.read_excel(xls, sheet_name=sheet_name)

    mySchema = StructType([StructField("JOB NAME", StringType(), True)
                              , StructField("OWNER", StringType(), True)
                              , StructField("JOB CLASS", StringType(), True)
                              , StructField("JOB ID", StringType(), True)
                              , StructField("RUN TIME", StringType(), True)
                              , StructField("CPU", StringType(), True)
                              , StructField("STEPS IN JCL", StringType(), True)
                              , StructField("UTILITY", StringType(), True)
                              , StructField("COND CODES", StringType(), True)
                              , StructField("RUN STATUS", StringType(), True)
                              , StructField("REASON FOR ABEND", StringType(), True)
                              , StructField("MORE ABEND INFO", StringType(), True)
                              , StructField("STEP WISE DATA SETS", StringType(), True)
                              , StructField("STARTED TIME", StringType(), True)
                              , StructField("ENDED TIME", StringType(), True)
                              , StructField("STEP AND DBRM LIB", StringType(), True)
                              , StructField("JOB INFO", StringType(), True)])

    mySchema1 = StructType([StructField("JOB NAME", StringType(), True)
                               , StructField("OWNER", StringType(), True)
                               , StructField("JOB CLASS", StringType(), True)
                               , StructField("JOB ID", StringType(), True)
                               , StructField("RUN TIME", StringType(), True)
                               , StructField("CPU", StringType(), True)])

    df = spark.createDataFrame(ydf, schema=mySchema)
    xdf = spark.createDataFrame(pdf1, schema=mySchema1)

    sort_df = xdf.sort(F.desc("RUN TIME"))

    sort_df.select('RUN TIME', 'JOB NAME').show()

    writer = pandas.ExcelWriter("C:\\Users\\M1055990\\Desktop\\check1\\Log_Enabler_output.xls",
                                engine='xlsxwriter')

    # sort_df.toPandas().to_excel('C:\\Users\\M1055990\\Desktop\\check1\\SortfileOutput.xls', sheet_name='Sheet_1',
    # index=False)
    sort_df.toPandas().to_excel(writer, sheet_name='Sorted data',
                                index=False)
    runstatus_df = df.withColumn('RUN STATUS', F.regexp_replace('RUN STATUS', "['']", ''))

    # runstatus_df.show()

    def ABEND_COUNT(x):
        if x == '[S0C7]' or x == '[S0C4]' or x == '[S0CB]':
            return 'DATA'
        elif x == '[SB37]' or x == '[SD37]':
            return 'SPACE'
        elif x == 'RUN SUCCESSFULL':
            return 'SUCCESSFULL RUN'
        else:
            return 'DB2 ABEND'

    udffunc_runstatus = F.udf(ABEND_COUNT, returnType=StringType())
    runstatus_df = runstatus_df.withColumn("ABEND STATUS", udffunc_runstatus("RUN STATUS"))

    runstatus_df1 = runstatus_df.groupBy("ABEND STATUS").agg(F.count("ABEND STATUS").alias('ABEND STATUS COUNT'))
    runstatus_df1.show()

    runstatus_df2 = runstatus_df1.toPandas()

    # plt.show()
    Tasks = runstatus_df2['ABEND STATUS COUNT']

    my_labels = runstatus_df2['ABEND STATUS']
    plt.pie(Tasks, labels=my_labels, autopct='%1.0f%%')
    plt.title('Abends')
    plt.axis('equal')
    plt.savefig("C:\\Users\\M1055990\\Desktop\\Graphs\\graph2.png")
    # plt.show()

    jobinfo_df = df.select('JOB NAME', 'STEP WISE DATA SETS', 'UTILITY', 'JOB INFO').filter(
        (df.UTILITY == ' IKJEFT01') | (df.UTILITY == 'IKJEFT01'))

    def parsing(x):
        # print(x)
        m = re.search("RUN (.*?) \ LIB", x)
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

    # jobinfo_df2.show()

    # jobinfo_df2.toPandas().to_excel('C:\\Users\\M1055990\\Desktop\\check1\\JobinfoOutput.xls', sheet_name='Sheet1',
    # index=False)
    jobinfo_df2.toPandas().to_excel(writer, sheet_name='Job Info', index=False)

    def condition(x):
        #    print(x)
        if x == ' IKJEFT01' or x == 'IKJEFT01':
            return "DB2"
        elif x == 'DFSRRC00' or x == ' DFSRC00':
            return 'IMS-DB'
        elif x == "IDCAMS" or x == " IEBGENER" or x == " IDCAMS" or x == "IEBGENER":
            return "VSAM"
        elif x == 'DSNUTILB' or x == ' DSNUTILB':
            return "DB2 LOAD"
        elif x == "SORT" or x == "ICEMAN" or x == " ICEMAN" or x == " SORT":
            return "SORT"
        else:
            return "COBOL"

    udffunc = F.udf(condition, returnType=StringType())
    df2 = df.withColumn("CATEGORY", udffunc("UTILITY"))
    # df2.show()

    df3 = df2.select("UTILITY", "CATEGORY")
    # df3.show()

    df1 = df3.groupBy("CATEGORY").agg(F.count("CATEGORY").alias('COUNT'))
    df1.show()
    # df1.toPandas().to_excel('C:\\Users\\M1055990\\Desktop\\fileOutput.xls', sheet_name='Sheet1', index=False)
    df1.toPandas().to_excel(writer, sheet_name='File output', index=False)

    # plot_df = pandas.read_excel("C:\\Users\\M1055990\\Desktop\\check1\\Log_Enabler_output.xls",sheet_name="File
    # Output") print(plot_df)

    plot_df = df1.toPandas()

    plt.clf()
    sns.barplot(x=plot_df["CATEGORY"],
                y=plot_df["COUNT"], data=plot_df, edgecolor='red')
    # plt.show()
    # plt.plot(plot_df['CATEGORY'], plot_df['COUNT'], color='blue', marker='o')
    # plt.title('Utility Count Graph', fontsize=14)
    # plt.xlabel('Utility Category', fontsize=14)
    # plt.ylabel('Utility Count', fontsize=14)
    # plt.fill(True)
    # plt.backgroundColor("rgba(244, 144, 128, 0.8)")
    plt.savefig("C:\\Users\\M1055990\\Desktop\\Graphs\\graph.png")
    # plt.show()

    plt.clf()
    ydf['RUN TIME'] = ydf['RUN TIME'].str.replace('0 days', '')
    ydf['RUN TIME'] = ydf['RUN TIME'].str.replace('.0000', '')
    print(pdf['RUN TIME'])
    y = ydf["RUN TIME"]
    x = ydf["JOB NAME"]

    # plot
    plt.scatter(x, y)
    # beautify the x-labels
    plt.gcf().autofmt_xdate()
    plt.savefig("C:\\Users\\M1055990\\Desktop\\Graphs\\graph1.png")
    # plt.show()
    writer.save()


if __name__ == '__main__':
    enabler = log_enabler()
