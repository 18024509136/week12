package com.geek;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 发布/订阅模式
 */
public class TopicDemo {

    public static void main(String[] args) {
        try {
            Destination destination = new ActiveMQTopic("test.topic");

            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://192.168.3.203:61616");
            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // 必须先启动消费者，因为topic不会保存消息，只有实时连着的消费者才可以消费消息，否则消息被丢弃
            // 这里模拟10个消费者消费topic，发现每条消息都会被10个消费者消费，属于发布/订阅模式
            ExecutorService executorService = Executors.newFixedThreadPool(10);
            for (int i = 0; i < 10; i++) {
                int finalI = i;
                TopicMessageListener topicMessageListener = new TopicMessageListener("监听者：" + finalI);
                TopicConsumerThread topicConsumerThread = new TopicConsumerThread(topicMessageListener);
                executorService.execute(topicConsumerThread);
            }

            // 等待消费线程全部启动完毕
            Thread.sleep(3000);

            MessageProducer producer = session.createProducer(destination);
            for (int i = 0; i < 2; i++) {
                TextMessage textMessage = session.createTextMessage("message_" + i);
                producer.send(textMessage);
                Thread.sleep(200);
            }

            Thread.sleep(10000);
            session.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static class TopicConsumerThread implements Runnable {

        private MessageListener messageListener;

        public TopicConsumerThread(MessageListener messageListener) {
            this.messageListener = messageListener;
        }

        @Override
        public void run() {
            try {
                Destination destination = new ActiveMQTopic("test.topic");

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

    private static class TopicMessageListener implements MessageListener {

        private String name;

        public TopicMessageListener(String name) {
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
