package io.github.cdelmas.poc;


import java.io.IOException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.codahale.metrics.MetricRegistry.name;
import static java.lang.Math.abs;
import static java.lang.Math.sqrt;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MINUTES;
import static spark.Spark.get;
import static spark.Spark.port;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
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
        final Timer timer = metrics.timer(name(Main.class, "job-exec-time"));

        ConnectionFactory connectionFactory = new ConnectionFactory();

        String rabbitUri = System.getenv("RABBIT_URI");
        connectionFactory.setUri(rabbitUri);
        connectionFactory.setMetricsCollector(new StandardMetricsCollector(metrics));

        logger.info("Connection to RabbitMQ");
        final Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();

        boolean isProd = Boolean.parseBoolean(System.getenv().getOrDefault("PROD", "false"));
        if (isProd) {
            logger.info("Connection to statsd");
            StatsDReporter.forRegistry(metrics)
                    .build("localhost", 8125) // configuration -> STATSD_HOST, STATSD_PORT
                    .start(10, TimeUnit.SECONDS);
        }

        logger.info("Declaring RabbitMQ topology");
        channel.exchangeDeclare("in-ex", BuiltinExchangeType.DIRECT);
        channel.queueDeclare("in-queue", true, false, false, new HashMap<>());
        channel.queueBind("in-queue", "in-ex", "job.to.do");

        AtomicBoolean working = new AtomicBoolean(false);

        channel.basicQos(1);
        final String queue = "in-queue";

        logger.info("Creating the job handler");
        final DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                if (!working.getAndSet(true)) {
                    logger.info("Starting a new job");
                    logger.info("Stop fetching new messages");
                    channel.basicCancel(getConsumerTag()); // stop fetching messages
                    CompletableFuture.supplyAsync(() -> {
                        final Timer.Context timerContext = timer.time();
                        try {
                            channel.basicAck(envelope.getDeliveryTag(), false);
                            Instant end = now().plus(2, MINUTES);
                            Random random = new SecureRandom();
                            do { // 100% cpu task for 2 minutes
                                final double v1 = sqrt(abs(random.nextLong()));
                                final double v2 = sqrt(abs(random.nextLong()));
                                final double v3 = sqrt(abs(random.nextLong()));
                                final double total = v1 + v2 + v3;
                                if (36 < total && total < 10000) {
                                    logger.info("So nice, we had some chance: {}", total);
                                }
                            } while (end.isAfter(Instant.now()));
                            TimeUnit.SECONDS.sleep(15);
                            return 42;
                        } catch (Exception e) {
                            logger.error("Oops", e);
                            return 0;
                        } finally {
                            timerContext.stop();
                        }
                    }).thenAccept(result -> {
                        jobs.inc();
                        logger.info("Job done: {}", result);
                        working.set(false);
                        try {
                            logger.info("Refetch messages");
                            channel.basicConsume(queue, false, this);
                        } catch (IOException e) {
                            logger.error("Could not consume again");
                        }
                    });
                } else {
                    logger.info("Rejecting the message, as already working");
                    channel.basicReject(envelope.getDeliveryTag(), true); // back pressure :)
                }
            }
        };
        logger.info("Consumer tag: {}", consumer.getConsumerTag());
        channel.basicConsume(queue, false, consumer); // bootstrap
        final int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        logger.info("Starting the HTTP server on {}", port);
        port(port);
        get("/", (req, res) -> "UP");

        logger.info("Server initialized");
    }

}
