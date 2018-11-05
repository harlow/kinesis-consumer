# Examples with opentracing

The examples are roughly the same as thoe without tracing, but to demonstrate what the code will look liek with distributed tracing integrated. The tracing api spec that we are using is Opentracing, due to a wider and more stable support at the moment.

Please refer to README under examples/consumer and examples/producer. 

## Installation
### Setup data for producer to upload

    $ curl https://s3.amazonaws.com/kinesis.test/users.txt > /tmp/users.txt
    $ go run main.go --stream streamName

### Setup AWS

For consumer and producer:

 * export the required environment vars for connecting to the AWS resources:

```
export AWS_ACCESS_KEY=
export AWS_REGION=
export AWS_SECRET_KEY=
```

 * export the Jaeger Environment to connect to Jaeger agent:
Reference (https://www.jaegertracing.io) for various variables settings.

 ```
export JAEGER_SAMPLER_TYPE=const
export JAEGER_SAMPLER_PARAM=1
export JAEGER_AGENT_HOST=localhost
export JAEGER_AGENT_PORT=6831
 ```

### Setup Backend

Opencensus supports both tracing and stat backend. For demo purposes, we are going to use Jaeger as the tracing backend and Prometheus for stats.

### Tracing Backend
Please refer to docs in reference section for Jaeger.
Setup Jaeger Agent using the all-in-one docker image
```
$ docker run -d --name jaeger \
  -e COLLECTOR_ZIPKIN_HTTP_PORT=9411 \
  -p 5775:5775/udp \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 14268:14268 \
  -p 9411:9411 \
  jaegertracing/all-in-one:1.6

```
You should be able to access the UI via http://localhost:16686.

## Development
You need opentracing-go as development depenency. If you want to see the result on UI, you need to choose an appropriate vendor (https://opentracing.io/)

```
go get -u github.com/opentracing/opentracing-go

```

References:
> Opentracing (https://github.com/opentracing/opentracing-go) &nbsp;&middot;&nbsp;
> Jaeger (https://www.jaegertracing.io/docs/1.6/getting-started/) &nbsp;&middot;&nbsp;
> Prometheus (https://prometheus.io/docs/prometheus/latest/installation/) &nbsp;&middot;&nbsp;
