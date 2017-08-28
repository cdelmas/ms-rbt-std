package io.github.cdelmas.poc;


import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.StandardMetricsCollector;
import com.readytalk.metrics.StatsDReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static Logger logger = LoggerFactory.getLogger("ms-rbt-std-main");

    public static void main(String[] args) throws Exception {

        logger.info("Starting the service");
        MetricRegistry metrics = new MetricRegistry();
        final Counter jobs = metrics.counter(name(Main.class, "job-runs"));

        ConnectionFactory connectionFactory = new ConnectionFactory();

        String rabbitUri = System.getenv("RABBIT_URI");
        connectionFactory.setUri(rabbitUri);
        connectionFactory.setMetricsCollector(new StandardMetricsCollector(metrics));

        logger.info("Connection to RabbitMQ");
        final Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();

        boolean statsd = Boolean.parseBoolean(System.getenv().getOrDefault("PROD", "false"));
        if (statsd) {
            logger.info("Connection to statsd");
            StatsDReporter.forRegistry(metrics)
                    .build("statsd.example.com", 8125) // configuration -> STATSD_HOST, STATSD_PORT
                    .start(10, TimeUnit.SECONDS);
        }

        channel.exchangeDeclare("in-ex", BuiltinExchangeType.DIRECT);
        channel.queueDeclare("in-queue", true, false, false, new HashMap<>());
        channel.queueBind("in-queue", "in-ex", "job.to.do");

        channel.basicConsume("in-queue", false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                CompletableFuture.supplyAsync(() -> {
                    try {
                        logger.info("Starting a new job");
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        TimeUnit.SECONDS.sleep(15);
                        return 42;
                    } catch (Exception e) {
                        logger.error("Oops", e);
                        return 0;
                    }
                }).thenAccept(result -> {
                    jobs.inc();
                    logger.info("Job done: {}", result);
                });
            }
        });
        logger.info("Server initialized");
    }
}
