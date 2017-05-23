package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	private static final String LINK_TO_LOCAL_HOST = "tcp://localhost:8161";
	private final String queueName;
	private final String topicName;
	private Connection connectionToBroker;

	public JmsSender(final String queueName, final String topicName) throws JMSException {
		this.queueName = queueName;
		this.topicName = topicName;
		this.connectionToBroker = new ActiveMQConnectionFactory(LINK_TO_LOCAL_HOST).createConnection();
	}

	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) throws JMSException {
		connectionToBroker.start();
		Session session = connectionToBroker.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue(this.queueName);
		MessageProducer producer = session.createProducer(destination);

		Order order = new Order(orderId, product, price);
		ObjectMessage message = session.createObjectMessage(order);
		message.setJMSType("Order");
		message.setStringProperty("WDSR-System", "OrderProcessor");
		producer.send(message);

		session.close();
		connectionToBroker.close();
	}

	public void sendTextToQueue(String text) throws JMSException {
		connectionToBroker.start();
		Session session = connectionToBroker.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue(this.queueName);
		MessageProducer producer = session.createProducer(destination);

		TextMessage message = session.createTextMessage();
		message.setText(text);
		producer.send(message);

		session.close();
		connectionToBroker.close();
	}

	public void sendMapToTopic(Map<String, String> map) throws JMSException {
		connectionToBroker.start();
		Session session = connectionToBroker.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createTopic(this.topicName);
		MessageProducer producer = session.createProducer(destination);
		MapMessage mapMessage = session.createMapMessage();

		for (Map.Entry<String, String> entry : map.entrySet()) {
			mapMessage.setString(entry.getKey(), entry.getValue());
		}
		producer.send(mapMessage);

		session.close();
		connectionToBroker.close();
	}
}
