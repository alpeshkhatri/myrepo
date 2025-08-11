# research ideas

## kafka Enable Request Logging
Configure Kafka broker logging to capture client connection details: properties# In server.properties
```
log4j.logger.kafka.request.logger=DEBUG, requestAppender
log4j.additivity.kafka.request.logger=false

log4j.appender.requestAppender=org.apache.log4j.DailyRollingFileAppender
log4j.appender.requestAppender.DatePattern='.'yyyy-MM-dd-HH
log4j.appender.requestAppender.File=${kafka.logs.dir}/kafka-request.log
log4j.appender.requestAppender.layout=org.apache.log4j.PatternLayout
log4j.appender.requestAppender.layout.ConversionPattern=[%d] %p %m (%c)%n
```
This will log all requests including client IP addresses, but you'll need to filter for specific topics in the logs.

