package com.glocks;


import com.glocks.configuration.ConnectionConfiguration;
import com.glocks.configuration.PropertiesReader;
import com.glocks.util.Util;
import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

import java.sql.Connection;

import static com.glocks.parser.ParserProcess.CdrParserProces;

@EnableAsync
@SpringBootConfiguration
@SpringBootApplication(scanBasePackages = {"com.glocks"})
@EnableEncryptableProperties
//ETl-Class
public class EdrP3Process {
    public static String appdbName = null;
    public static String edrappdbName = null;
    public static String auddbName = null;
    public static String repdbName = null;
    public static String serverName = null;
    public static String dateFunction = null;
    public static   int usageInsert = 0;
    public static  int usageUpdate = 0;
    public static  int duplicateInsert = 0;
    public static  int duplicateUpdate = 0;

    public  static int usageInsertForeign = 0;
    public  static int usageUpdateForeign = 0;
    public static  int duplicateInsertForeign = 0;
    public static  int duplicateUpdateForeign = 0;
    public static PropertiesReader propertiesReader = null;
    public static Connection conn = null;
    static Logger logger = LogManager.getLogger(EdrP3Process.class);
    static ConnectionConfiguration connectionConfiguration = null;

    public static void main(String args[]) { // OPERATOR FilePath

        ApplicationContext context = SpringApplication.run(EdrP3Process.class, args);
        propertiesReader = (PropertiesReader) context.getBean("propertiesReader");
        connectionConfiguration = (ConnectionConfiguration) context.getBean("connectionConfiguration");
        logger.info("connectionConfiguration :" + connectionConfiguration.getConnection().toString());
        conn = connectionConfiguration.getConnection();

        appdbName = propertiesReader.appdbName;
        edrappdbName = propertiesReader.edrappdbName;
        auddbName = propertiesReader.auddbName;
        repdbName = propertiesReader.repdbName;
        serverName = propertiesReader.serverName;
        dateFunction = Util.defaultDateNow(conn.toString().contains("oracle"));

        String filePath = null;
        if (args[0] == null) {
            logger.error("Enter the Correct File Path");
        } else {
            filePath = args[0].trim();
        }
        if (!filePath.endsWith("/")) {
            filePath += "/";
        }
        CdrParserProces(conn, filePath);
        System.exit(0);

    }
}
