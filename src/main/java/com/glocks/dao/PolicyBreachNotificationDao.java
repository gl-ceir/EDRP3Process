package com.glocks.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.glocks.util.Util;
import org.springframework.stereotype.Repository;

@Repository
public class PolicyBreachNotificationDao {

     static Logger logger = LogManager.getLogger(PolicyBreachNotificationDao.class);
     public void insertNotification(Connection conn, PolicyBreachNotification policyBreachNotification) {
          boolean isOracle = conn.toString().contains("oracle");
          String dateFunction = Util.defaultDate(isOracle);

          String query = "insert into policy_breach_notification ( channel_type,CONTACT_NUMBER, IMEI,message,RETRY_COUNT ,created_on, modified_on) values "
                  + " (?, ?,?, ? ,0 , " + dateFunction + "," + dateFunction + ")";

          try (PreparedStatement preparedStatement = conn.prepareStatement(query);) {
               preparedStatement.setString(1, policyBreachNotification.getChannelType());
               preparedStatement.setString(2, policyBreachNotification.getContactNumber());
               preparedStatement.setString(3, policyBreachNotification.getImei());
               preparedStatement.setString(4, policyBreachNotification.getMessage());
               logger.info("Query " + preparedStatement);
               preparedStatement.execute();
               logger.info("Inserted in notification succesfully.");
               preparedStatement.execute();
               preparedStatement.close();
          } catch (SQLException e) {
               logger.error(e.getMessage(), e);
          }
     }
}


