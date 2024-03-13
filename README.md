### Home Assessment

Imagine you are a data engineer working for AdvertiseX, a digital advertising technology company. AdvertiseX specializes in programmatic advertising and manages multiple online advertising campaigns for its clients. The company handles vast amounts of data generated by ad impressions, clicks, conversions, and more. Your role as a data engineer is to address the following challenges:

### How to run ?

#### Need to download Kafka and Spark

```
$ wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz

$ tar -xvf kafka_2.13-3.7.0.tgz

$ wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz

$ tar -xvf spark-3.5.1-bin-hadoop3.tgz 

```

#### Syntatic data generation

* Data_generator script will create batchs of 3 sets of data points
```
$ python Data_generator/data_faker_both.py --size_count 5 --batch_size 100000
```
* --size_count -> Number of batches to be created
* --batch_size -> Number of datapoints included in a batch
* The arguments mentioned are defaults values which can also to configuired to create fake batch of dataset

