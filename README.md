# kafkacat-avro

## build/run
### docker
```
docker run -v /your/local/dir/:/tmp/ mcurse/kafkacat-avro -t your-topic -b your-kafka-server -s /tmp/your-avro.json
```
or see [https://hub.docker.com/r/mcurse/kafkacat-avro](https://hub.docker.com/r/mcurse/kafkacat-avro)
### from scratch
```bash
git clone git@github.com:mkurth/kafkacat-avro.git
cd kafkacat-avro
sbt stage
cd target/universal/stage

./bin/kafkacat-avro -t your-topic -b your-kafka-server -s /tmp/your-avro.json
```
