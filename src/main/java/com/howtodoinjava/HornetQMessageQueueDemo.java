package com.howtodoinjava;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;

import org.hornetq.integration.transports.netty.TransportConstants;

import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;

import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

public class HornetQMessageQueueDemo {
    private final Logger log = LoggerFactory.getLogger(HornetQMessageQueueDemo.class);

    private Connection connection = null;
    private JMSServerManager jmsServerManager = null;

    private int startServer()
    {
        int status = 0;
        try
        {
            log.info("01 - Read hornet configuration");
            FileConfiguration configuration = new FileConfiguration();
            configuration.setConfigurationUrl("hornetq-configuration.xml");
            configuration.start();
            log.info("02 - Read configuration, ready to use it");

            HornetQServer server = HornetQServers.newHornetQServer(configuration);
            log.info("03 - Created hornet mq server");

            jmsServerManager = new JMSServerManagerImpl(server, "hornetq-jms.xml");
            log.info("04 - Created jms server manager");

            //if you want to use JNDI, simple inject a context here or don't call this method and make sure the JNDI parameters are set.
            jmsServerManager.setContext(null);
            jmsServerManager.start();

            log.info("05 - Server started !!");
        }
        catch (Throwable e)
        {
            log.error("06 - Damn it !!, server not started", e);
            status = -1;
        }
        return status;
    }

    private void stopServer() {
        try {
            jmsServerManager.stop();
        } catch (Exception e) {
            log.error("Error during stop jms server: ", e);
        }
    }

    private Queue createQueue(final String name) {

        // Step 1. Directly instantiate the JMS Queue object.
        Queue queue = HornetQJMSClient.createQueue(name);
        log.info("10 - created queue: {}", name);

        return queue;
    }

    private TransportConfiguration getTransportConfiguration() {
        Map<String, Object> connectionParams = new HashMap<String, Object>();
        connectionParams.put(TransportConstants.PORT_PROP_NAME, 5445);

        TransportConfiguration transportConfiguration = new TransportConfiguration(
                NettyConnectorFactory.class.getName(), connectionParams);
        log.info("11 - Setup tranport configuration for netty");

        return transportConfiguration;
    }

    private Session getSession(final TransportConfiguration transportConfiguration) {
        Session session = null;

        try {
            ConnectionFactory cf = (ConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transportConfiguration);
            log.info("12 - Created connection factory");

            // Step 4.Create a JMS Connection
            connection = cf.createConnection();
            log.info("13 - Setup connection");

            // Step 5. Create a JMS Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            log.info("14 - Created a JMS Session");
        } catch (Exception e) {
            log.error("Couldnot create session: ", e);
            session = null;
            connection = null;
        }

        return session;
    }

    private MessageProducer getMessageProducer(final Queue queue, final Session session) throws JMSException {
        MessageProducer producer = session.createProducer(queue);
        log.info("15 - Setup a JMS producer");
        return producer;
    }


    private void sendMessage(final Session session, final MessageProducer producer, final String message) throws JMSException {
        TextMessage mesg = session.createTextMessage(message);
        log.info("16 - Sent message: " + mesg.getText());

        // Step 8. Send the Message
        producer.send(mesg);
        log.info("17 - Producer send message" );
    }


    public void doTest() {
        int status = -1;

        status = startServer();

        if (0 == status ) {

            try {
                // Step 1. Directly instantiate the JMS Queue object.
                Queue queue = createQueue("exampleQueue");

                // Step 2. Instantiate the TransportConfiguration object which
                // contains the knowledge of what transport to use,
                // The server port etc.

                TransportConfiguration transportConfiguration = getTransportConfiguration();

                // Step 3 Directly instantiate the JMS ConnectionFactory object
                // using that TransportConfiguration

                Session session = getSession(transportConfiguration);

                // Step 6. Create a JMS Message Producer
                MessageProducer producer = getMessageProducer(queue, session);

                // Step 7. Create a Text Message
                sendMessage(session, producer, "How to do in java dot com");

                // Step 9. Create a JMS Message Consumer
                MessageConsumer messageConsumer = session.createConsumer(queue);
                log.info("18 - Create a jms consumer" );

                // Step 10. Start the Connection
                connection.start();
                log.info("19 - Start a jms connection" );

                // Step 11. Receive the message
                TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
                log.info("20 - Client read message" );

                log.info("21 - Received message: " + messageReceived.getText());
            } catch (Exception e) {
                log.error("22 - Error during processing: ", e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                        log.info("23 - Connection closed");
                    } catch (Exception e) {
                        log.error("Error during cleanup: ", e);
                    }
                }
            }
            stopServer();
        }
    }




    public static void main(String[] args) throws Exception {
        HornetQMessageQueueDemo hd = new HornetQMessageQueueDemo();

        hd.doTest();

    }

}
