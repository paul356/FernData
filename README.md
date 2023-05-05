---
layout: default
title: HTAP Initiative
tags: [HTAP, database]
nav_order: {{ page.date }}
---


# HTAP Initiative


## The Goal

Now there are two main classes of databases. One class is optimized for OLTP, like Oracle, SQL Server, MySQL, etc. The other class is for OLAP tasks. This class includes warehouse systems, like SAP HANA, Hive, Iceberg, Hudi, etc. Usually we need these two classes of systems in business operations. So the data flow between these two classes of systems requires extra work to keep them consistent and ready. So our aim is to design a HTAP system which takes OLTP and OLAP tasks into consideration. This is not an easy task, but with good examples of open source systems. We can address this task by adding the parts missed by OLTP to open source OLAP systems.


## The Plan

OLTP systems can deal with random queries and data mutations very quickly, because OLTP systems have delicately designed indices and data layout formats. These systems can pin-point a small set of data quickly. Such design is something that we find missing in open source OLAP systems. To make this task more practical we choose to enhance Apache Iceberg and make necessary optimizations. [Apache Iceberg](https://iceberg.apache.org) is one of the best datalake systems publicly available. It can be used along with Spark to serve relational tables. Users can use SQL to run queries and modifications. The data in Apache Spark is distributed into partitions. This data distribution is called partitioning. Partitioning works by assigning data having same values in [partition columns](https://iceberg.apache.org/docs/latest/partitioning/) in the same partition. Besides partitoning Apache Iceberg [utilizes json files and manifest files (Avro format)](https://iceberg.apache.org/spec/) to build an index. It is used to speed up locating data files. This index structure is suitable for batch process tasks which often go through whole data files. Considering OLTP tasks there are two limits with such design. The first limit is that it requires experience to assign partition columns. Inapproriate partition selection may severely deteriorate the performance. The second limit is that there is no data level index. It is not efficient for record operations because the index is targeted to files. 

The first optimization is to eliminate the need to select partition columns. In order to skip partition selection we plan to organize the data in levels which is similar to LSM-Tree. Data is first write to a long struture accompanied by a in-memory index struture. After enough data is accumulated, it is flushed to the first level. When the data in the first level is accumulated data is merged into the second level. This process is continued through all the levels of the data organization. The next level also adds one dimension to partition the data. For example the first level data is formed in ranges, the second level data is formed in squares, the third level data is formed in cubes, and so on. These partition dimensions are selected based on data distribution characteristics of the last level. To help with that we need to collect enough data statistics for each data columns. With such an data orgnaization we can handle data mutation more easily and also make the data distributed evenly.

The second optimization is about the index. In order to quickly pin-point any data we plan to build a two level index. The first level is an index for data files. The second level consists of per data file indices, which are used to locate a data record. When one wants to find a data record, he/she can first check the data file index, then the a per data file index to pin-point the data record.

With these two optimization the new system still can handle OLAP tasks by going through data files as a whole. But with re-organization of the data format and new indices it can also handle record level data changes and queries more efficiently.


## Other considerations

Besides these two optimizations we plan to use open source components as many as possible. Parquet, Avro are good candidates. These projects should be used as data files and log files. And an efficient and powerful programming language can improve efficiency, too. Though Iceberg is mainly a Java project, scala is more compact and has many modern language features. And Scala can interoperate with java code and libraries. So we think Scala is good option for this project. Since we will use the open source projects as the building blocks of this initiative, it is better to make this initiative a open source project, too. Anyone who is interested in our idea can contribute to this project.

