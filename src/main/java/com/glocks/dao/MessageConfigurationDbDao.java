package com.glocks.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.glocks.parser.ParserProcess;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.springframework.stereotype.Repository;

import static com.glocks.EdrP3Process.appdbName;

@Repository
public class MessageConfigurationDbDao {
	static Logger logger = LogManager.getLogger(MessageConfigurationDbDao.class);

	public static void sendMessageToMsisdn(Connection conn, String msisdn, String imei) {

		MessageConfigurationDbDao messageConfigurationDbDao = new MessageConfigurationDbDao();
		PolicyBreachNotificationDao policyBreachNotificationDao = new PolicyBreachNotificationDao();
		MessageConfigurationDb messageDb = null;

		try {
			Optional<MessageConfigurationDb> messageDbOptional = messageConfigurationDbDao.getMessageDbTag(conn, "USER_REG_MESSAGE", appdbName);
			if (messageDbOptional.isPresent()) {
				messageDb = messageDbOptional.get();
				String message = messageDb.getValue().replace("<imei>", imei);
				PolicyBreachNotification policyBreachNotification
						= new PolicyBreachNotification("SMS", message, "", msisdn, imei);
				policyBreachNotificationDao.insertNotification(conn, policyBreachNotification);
			}
		} catch (Exception e) {
			logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(MessageConfigurationDbDao.class.getName())).collect(Collectors.toList()).get(0) + "]");
		}
	}






    public Optional<MessageConfigurationDb> getMessageDbTag(Connection conn, String tag, String appdbName) {
		ResultSet rs = null;
		Statement stmt = null;
		String query = "select id, description, tag, value, channel, active, subject "
				+ "from " + appdbName + ".msg_cfg where tag='" + tag + "'";
		logger.info("Query ["+query+"]");
		try{
			stmt  = conn.createStatement();
			rs = stmt.executeQuery(query);
			if(rs.next()){
				return Optional.of(new MessageConfigurationDb(rs.getLong("id"), rs.getString("tag"), 
						rs.getString("value"), rs.getString("description"), rs.getInt("channel"), 
						rs.getString("subject")));
			}
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		finally{
			try {
				if(Objects.nonNull(stmt))
                 rs.close();
					stmt.close();
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
		}
		return Optional.empty();
	}
}
