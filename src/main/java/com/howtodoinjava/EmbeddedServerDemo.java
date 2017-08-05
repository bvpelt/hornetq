package com.howtodoinjava;

import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedServerDemo {

    private final Logger log = LoggerFactory.getLogger(EmbeddedServerDemo.class);

    public void doTest() {
        try {
            //Load the file configuration first of all
            FileConfiguration configuration = new FileConfiguration();
            configuration.setConfigurationUrl("hornetq-configuration.xml");
            log.info("01 - Loaded configuration");

            configuration.start();
            log.info("02 - Configuration started");

            //Create a new instance of hornetq server
            HornetQServer server = HornetQServers.newHornetQServer(configuration);
            log.info("03 - Server created");

            //Wrap inside a JMS server
            JMSServerManager jmsServerManager = new JMSServerManagerImpl(
                    server, "hornetq-jms.xml");
            log.info("04 - Wrapped in JMS Server");

            // if you want to use JNDI, simple inject a context here or don't
            // call this method and make sure the JNDI parameters are set.
            jmsServerManager.setContext(null);
            log.info("05 - Set JNDI context");

            //Start the server
            jmsServerManager.start();
            log.info("06 - JMS Server started");
            //WOO HOO
            log.info("HornetQ server started successfully !!");
        } catch (Throwable e) {
            System.out.println("Well, you seems to doing something wrong. Please check if config files are in your classes folder.");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        EmbeddedServerDemo demo = new EmbeddedServerDemo();

        demo.doTest();
    }
}