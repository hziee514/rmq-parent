package org.mq.rpc.client;

import com.rabbitmq.client.*;
import org.mq.rpc.Constants;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author wurenhai
 * @date 2018/1/28
 */
public class Client {

    private Connection connection;
    private Channel channel;
    private String replyQueueName;

    public Client() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(Constants.URI);
        connection = factory.newConnection();
        channel = connection.createChannel();
        replyQueueName = channel.queueDeclare().getQueue();
    }

    public String call(String message) throws Exception {
        String corrId = UUID.randomUUID().toString();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();
        channel.basicPublish("", Constants.QUEUE_NAME, props, message.getBytes());
        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);
        channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                if (properties.getCorrelationId().equals(corrId)) {
                    response.offer(new String(body, "utf-8"));
                }
            }
        });
        return response.take();
    }

    public void close() throws Exception {
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client();
        System.out.println(" [x] Requesting fib(30)");
        String response = client.call("30");
        System.out.println(" [.] Got '" + response + "'");
        client.close();
    }

}
