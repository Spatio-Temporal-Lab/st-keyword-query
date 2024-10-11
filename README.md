# Spatio-Temporal Keyword Query Processing Based on Key-Value Data Stores
***
KV-STK is  an efficient and scalable spatio-temporal keywords query framework.

## KV-STK feature
- Efficient, KV-STK propose a hybrid index that combining in-memory index
with on-disk index to answer STK queries based on key-value data stores. The hybrid index effectively ensures the querying and insertion efficiency.
- Scalable: KV-STK design a framework for managing in-memory filters to support the acquisition and eviction of filters within a given memory threshold. The framework ensures excellent scalability of KV-STK.

## KV-STK Structure
KV-STK consists of three main modules, _Key Generator_, _Filter_ and _Filter Manager_.

#### Key Generator
Key Generator is used to convert object into a one-dimensional key and convert query into multi key ranges. The main code is in the *org.urbcomp.startdb.stkq.keyGenerator* package. The implementation of Hilbert Curve is from https://github.com/davidmoten/hilbert-curve.

#### Filter
Filter is used to narrow down the key ranges obtained from Key Generator. The main code is in the *org.urbcomp.startdb.stkq.filter* package. The implementation of InfiniFilter is referred to the original author's github: https://github.com/nivdayan/FilterLibrary.

#### Filter Manager
Filter Manager is used to evict the “cold” filters out of the memory
and load necessary filters from the key-value data stores. The main code is in the *org.urbcomp.startdb.stkq.filter.manager* package.

## Test KV-STK
The default query parameters are set according to the table below and can be adjusted for different data sets.

<table>
  <tr>
    <th>Parameter</th>
    <th>Range</th>
    <th>Default</th>
  </tr>
  <tr>
    <th>Spatial Query Range(KM^2)</th>
    <th>1x1,2x2,3x3,4x4,5x5</th>
    <th>3x3</th>
  </tr>
  <tr>
    <th>Temporal Query Range(h)</th>
    <th>1,2,3,4,5</th>
    <th>3</th>
  </tr>
  <tr>
    <th>Keywords Count</th>
    <th>1,2,3,4,5</th>
    <th>3</th>
  </tr>
</table>

### Prerequisites for testing

The following resources need to be downloaded and installed:

- Java 8 download: https://www.oracle.com/java/technologies/downloads/#java8
- Scala download: https://www.scala-lang.org/download/2.12.4.html
- IntelliJ IDEA download: https://www.jetbrains.com/idea/
- git download:https://git-scm.com/download
- maven download: https://archive.apache.org/dist/maven/maven-3/

Download and install jdk-8, IntelliJ IDEA and git. IntelliJ IDEA's maven project comes with maven, you can also use your
own maven environment, just change it in the settings.

### Clone code

1. Open *IntelliJ IDEA*, find the *git* column, and select *Clone...*

2. In the *Repository URL* interface, *Version control* selects *git*

3. URL filling: *https://anonymous.4open.science/r/StreamingTrajSegment-683E.git*

### Set SDK

#### JDK Set up
File -> Project Structure -> Project -> Project SDK -> *add SDK*

Click *JDK* to select the address where you want to download jdk-8

#### Scala Set up
Please refer to https://www.jetbrains.com/help/idea/get-started-with-scala.html#new-scala-project-sbt

### Prerequisites for testing

The main code for experiments are in the *org.urbcomp.startdb.stkq.experiments* package. Before testing, the following works need to be done in advance: 

### HBase Set up

Please edit the zookeeper address in *org.urbcomp.startdb.stkq.io.HBaseUtil*;

### Query Generate

Please run *org.urbcomp.startdb.stkq.preProcessing.QueryGenerator* to get the queries. You can adjust the path where the query is written for later testing.

### Data Insertion

Please run *org.urbcomp.startdb.stkq.preProcessing.BatchWrite* to load the data set to the HBase.


### Datasets

Yelp dataset can be found in following link，tweet dataset is not publicly available for the time being.

- Yelp: https://www.yelp.com/dataset/download


