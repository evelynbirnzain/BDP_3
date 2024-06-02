# Assignment report

There are two options for handling the results from streaming data analytics: the results
will be (1) sent back to the tenants in near real-time and (2) stored into mysimbdp-coredms. A single result
can be handled using a single option or both options, depending on the configuration and the type of the
result (based on the analytics design).

Monitoring working hours of (taxi/truck) drivers (assume events
about pickup/drop captured at near real-time):
● Windows: 12 hours
● Partitioning data/Keyed streams: licenseID
● Function: determine working and break times and check with the
law/regulation
Source:
https://www.infoworld.com/article/3293426/how-to-build-stateful-streaming-applications-with-apache-flink.html

# Part 1 - Design for streaming analytics (weighted factor for grades = 3)

1. As a tenant, select a dataset suitable for streaming data analytics as a running scenario. Explain the
   dataset and why the dataset is suitable for streaming data analytics in your scenario. As a tenant,
   present at least two different analytics: (i) a streaming analytics (`tenantstreamapp`) which analyzes
   streaming data from the tenant and (ii) a batch analytics, using the workflow model, which analyzes
   historical results outputted by the streaming analytics. The explanation should be at a high level to
   allow us to understand the data and possible analytics so that, later on, you can implement and use
   them in answering other questions. (1 point)

I selected the
[open dataset about air quality monitoring from Germany](https://github.com/opendata-stuttgart/meta/wiki/EN-APIs).
The dataset consists of measurements taken by thousands of sensors across Europe. There are two main types of sensors,
one measuring weather-related data such as temperature, humidity, and air pressure; the other one measures air
pollution. The dataset is suitable for streaming analytics since it consists of small measurements that, in a real-life
scenario, arrive at a high volume and in near real-time; there is a large number of applications for processing the
data in near real-time that are much more involved than just storing the data.

(i) For streaming analytics, if we consider the air pollution sensors, we could e.g. do short-term aggregation for a
real-time air quality index, detect pollution spikes so that alerts can be sent out to concerned citizens, or monitor
the sensors themselves for malfunctions (if a sensor stops sending measurement or the measurements are abnormal compared
to other nearby sensors). For the weather sensors, we could do very similar things, e.g. monitor for weather anomalies,
detect malfunctioning sensors, or do short-term weather forecasting.

(ii) For batch analytics, we could use the historical data to, for example, build a model that we can then feed with the
near real-time data to predict future air pollution levels or weather conditions. We could also use the historical data
to detect long-term trends in air pollution or weather conditions.

2. The tenant will send data through a messaging system, which provides stream data sources. Discuss,
   explain and give examples for the following aspects in the streaming analytics: (i) should the
   streaming analytics handle keyed or non-keyed data streams for the tenant data, and (ii) which types
   of message delivery guarantees should be suitable for the stream analytics and why. (1 point)

(i) It would make sense to key/partition the data based on the sensor id, so that all measurements from the same sensor
can be processed together. Like this, it is straightforward e.g. calculate the average pollution level for a sensor
in a time windows to create a less noisy air quality index, detect malfunctioning sensors, or detect pollution spikes
based on the trend of the measurements in that one sensor.

(ii) Generally, I think all the delivery guarantees could be suitable here. I would be nice to have exactly once
delivery, but it is not strictly necessary, as there isn't necessarily anything that speaks against getting some
measurements multiple times or losing some measurements in transit. Based on that, I would say that a possible
performance penalty for going with exactly once delivery would probably not be worth it, and since the data is not
critical, and it would be fine if some measurements are lost, at most once delivery would be the best choice.

3. Given streaming data from the tenant (selected before). Explain and give examples in your
   explanation for the following points: (i) which types of time should be associated with stream data
   sources for the analytics and be considered in stream processing (if the data sources have no
   timestamps associated with records, then what would be your solution), (ii) which types of windows
   should be developed for the analytics (if no window, then why), (iii) what could cause out-of-order
   data/records with your selected data in your running example, and (iv) will watermarks be needed or
   not and explain why.(1 point)

(i) As-is, the measurements come with a timestamp that gives the time the measurement was taken (event time), which
would be ideal for processing. This is the time we would likely be most interested in when drawing conclusions from the
data. If the data didn't come with a timestamp recording the event time, I would try to approximate the event time as
close as possible -- i.e. the time the message was received by the messaging system (ingestion time) or if all else
fails, the time the message was processed by the stream processing system (processing time). For system-level analytics
it could also be interesting to look at the latency between the different timestamps to check for performance issues.

(ii) Generally, the options would be tumbling windows, sliding windows, and session windows. For the air quality data,
session windows would not make sense. Tumbling windows could be used to calculate e.g. an hourly air quality index.
Sliding windows seems like the most interesting option, as it would allow us to calculate e.g. a rolling average of the
pollution levels over the last hour, which could be used to detect pollution spikes.

(iii) Out-of-order data can mainly be caused by network issues, where messages are delayed and arrive out of order. In
this case, the sensors are also located in different countries with possibly different time zones that are not indicated
in the timestamps, so this could also cause technically out-of-order data; or the sensor clocks might not be perfectly
synchronized.

(iv) Watermarks should be used so that the stream processing system can close each window in a reasonable time so that
the results become available in a timely manner. Measurements that are very late can just be dropped, as they are likely
not that relevant for real-time analytics anymore at that point.

4. List important performance metrics for the streaming analytics for your tenant cases. For each
   metric, explain (i) its definition, (ii) how to measure it in your analytics/platform and (iii) its
   importance and relevance for the analytics of the tenant (for whom/components and for which
   purposes). (1 point)

* Latency: The time between a message being received by the messaging system (t_1) and the result becoming available
  (t_2).
* Throughput: The number of messages processed per second.

TODO: explain how to measure and why they are important

5. Provide a design of your architecture for the streaming analytics service in which you clarify: tenant
   data sources, mysimbdp messaging system, mysimbdp streaming computing service, tenant
   streaming analytics app, `mysimbdp-coredms`, and other components, if needed. Explain your choices
   of technologies for implementing your design and reusability of existing assignment works. Note
   that the result from `tenantstreamapp` will be sent back to the tenant in near real-time and/or will be
   ingested into `mysimbdp-coredms`. (1 point)

   TODO: insert figure and explain

# Part 2 - Implementation of streaming analytics (weighted factor for grades = 3)

1. As a tenant, implement a `tenantstreamapp`. For code design and implementation, explain (i) the
   structures/schemas of the input streaming data and the analytics output/result in your
   implementation, the role/importance of such schemas and the reason to enforce them for input data
   and results, and (ii) the data serialization/deserialization for the streaming analytics application
   (`tenantstreamapp`). (1 point)

(i) The schema of the input data can be found in ![schema.py](../code/tenantstreamapp/schema.py). Each input measurement
has an id, timestamp, location, sensor metadata, and some actual measurement data and is represented as a nested
dictionary. Upon ingestion into the messaging system, 


TODO: output schema

(ii) The input data is JSON serialized. Upon ingestion into the messaging system, each measurement becomes a Kafka message
with the key being the sensor id and the value being the JSON serialized measurement, as well as some metadata (e.g. 
timestamp ==  ingestion time, topic, partition, offset). The `tenantstreamapp` uses Spark Structured Streaming to read
the input stream using the SS Kafka connector and then deserializes the JSON data using both the schema described above
and the Spark SQL JSON deserializer `from_json`.

TODO: output serialization

2. Explain the logic of the functions for processing events/records in `tenantstreamapp` in your
   implementation. Explain under which conditions/configurations and how the results are sent back to
   the tenant in a near real time manner and/or are stored into `mysimbdp-coredms` as the final sink. (1 point)

3. Explain a test environment for testing `tenantstreamapp`, including how you emulate streaming data,
   configuration of mysimbdp and other relevant parameters. Run `tenantstreamapp` and show the
   operation of the `tenantstreamapp` with your test environments. Discuss the analytics and its
   performance observations when you increase/vary the speed of streaming data. (1 point)

4. Present your tests and explain them for the situation in which wrong data is sent from or is within
   the data sources. Explain how you emulate wrong data for your tests. Report how your
   implementation deals with that (e.g., exceptions, failures, and decreasing performance). You should
   test with different error rates. (1 point)

5. Explain parallelism settings in your implementation (`tenantstreamapp`) and test with different
   (higher) degrees of parallelism for at least two instances of `tenantstreamapp` (e.g., using different
   subsets of the same dataset). Report the performance and issues you have observed in your testing
   environments. Is there any situation in which a high value of the application parallelism degree could
   cause performance problems, given your limited underlying computing resources (1 point).

# Part 3 - Extension (weighted factor for grades = 2)

1. Assume that you have an external RESTful (micro) service, which accepts a batch of data (processed
   records), performs an ML inference and returns the result. How would you integrate such a service
   into your current platform and suggest the tenant to use it in the streaming analytics? Explain what
   the tenant must do in order to use such a service. (1 point)

2. Given the output of streaming analytics stored in `mysimbdp-coredms`. Explain a batch analytics (see
   also Part 1, question 1) that could be used to analyze such historical output for your scenario.
   Assume you have to design the analytics with the workflow model, draw the workflow of the batch
   analytics and explain how would you implement it? (1 point)

3. Assume that the streaming analytics detects a critical condition (e.g., a very high rate of alerts) that
   should trigger the execution of the above-mentioned batch analytics to analyze historical data. The
   result of the batch analytics will be shared in a cloud storage and a user within the tenant will receive
   the information about the result. Explain how would you use workflow technologies to coordinate
   these interactions and tasks (use a figure to explain your design). (1 point)

4. Given your choice of technology for implementing your streaming analytics. Assume that a new
   schema of the input data has been developed and new input data follows the new schema (schema
   evolution), how would you make sure that the running `tenantstreamapp` does not handle a wrong
   schema? Assume the developer/owner of the `tenantstreamapp` should be aware of any new schema
   of the input data before deploying the `tenantstreamapp`, what could be a possible way that the
   developer/owner can detect the change of the schemas for the input data. (1 point)

5. Is it possible to achieve end-to-end exactly once delivery in your current `tenantstreamapp` design
   and implementation? If yes, explain why. If not, what could be conditions and changes to achieve end-
   to-end exactly once? If it is impossible to have end-to-end exactly once delivery in your view, explain
   the reasons. (1 point)
