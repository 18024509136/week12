package com.geek;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * P2P模式
 */
public class QueueDemo {

    public static void main(String[] args) {
        try {
            Destination destination = new ActiveMQQueue("test.queue");

            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://192.168.3.203:61616");
            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // queue模式，可以先不启动消费者，因为消息会落地，直到有消费者消费
            MessageProducer producer = session.createProducer(destination);
            for (int i = 0; i < 40; i++) {
                TextMessage textMessage = session.createTextMessage("message_" + i);
                producer.send(textMessage);
                Thread.sleep(200);
            }

            // 这里启动4个消费线程同时消费，发现每条消息只能被单个消费者消费
            ExecutorService executorService = Executors.newFixedThreadPool(4);
            for (int i = 0; i < 4; i++) {
                int finalI = i;
                QueueMessageListener queueMessageListener = new QueueMessageListener("监听者：" + finalI);
                QueueConsumerThread queueConsumerThread = new QueueConsumerThread(queueMessageListener);
                executorService.execute(queueConsumerThread);
            }

            Thread.sleep(10000);

            session.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class QueueConsumerThread implements Runnable {

        private MessageListener messageListener;

        public QueueConsumerThread(MessageListener messageListener) {
            this.messageListener = messageListener;
        }

        @Override
        public void run() {
            try {
                Destination destination = new ActiveMQQueue("test.queue");

                ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://192.168.3.203:61616");
                Connection connection = connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                MessageConsumer consumer = session.createConsumer(destination);
                consumer.setMessageListener(this.messageListener);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class QueueMessageListener implements MessageListener {

        private String name;

        public QueueMessageListener(String name) {
            this.name = name;
        }

        @Override
        public void onMessage(Message message) {
            System.out.println(name + " 接收到消息为：" + message);
            // 消费出现异常，默认重试次数为6，由消息borker去维护消息的redeliveryCounter，每次重试+1
            // throw new RuntimeException("构造的异常");
        }
    }
}
