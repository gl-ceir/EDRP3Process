package com.glocks.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

import static com.glocks.EdrP3Process.auddbName;
import static com.glocks.EdrP3Process.serverName;

public class ModuleAuditTrail {

    static Logger logger = LogManager.getLogger(ModuleAuditTrail.class);

    public static int insertModuleAudit(Connection conn, String featureName, String processName) {
        int generatedKey = 0;
        String query = " insert into  " + auddbName + ".modules_audit_trail " + "(status_code,status,feature_name,"
                + "info, count2,action,"
                + "server_name,execution_time,module_name,failure_count) "
                + "values('201','Initial', '" + featureName + "', '" + processName + "' ,'0','Insert', '"
                + serverName + "','0','EDR','0')";
        logger.info(query);
        try {

            PreparedStatement ps = null;
            if (conn.toString().contains("oracle")) {
                ps = conn.prepareStatement(query, new String[]{"ID"});
            } else {
                ps = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            }
            ps.execute();
            //   logger.debug("Going for getGenerated key  ");
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                generatedKey = rs.getInt(1);
            }
            logger.info("Inserted record's ID: " + generatedKey);
            rs.close();
        } catch (Exception e) {
            logger.error(query + " :: Failed  " + e);
        }
        return generatedKey;
    }

    public static void updateModuleAudit(Connection conn, int statusCode, String status, String errorMessage, int id, long executionStartTime, long numberOfRecord, int failureCount) {
        String exec_time = " TIMEDIFF(now() ,created_on) ";
        String query = null;
        if (conn.toString().contains("oracle")) {
            long milliseconds = (new Date().getTime()) - executionStartTime;
            String executionFinishTiime = (((milliseconds / 1000) / 60) / 60) + ":" + (((milliseconds / 1000) / 60) % 60) + ":" + ((milliseconds / 1000) % 60);
            exec_time = " '" + executionFinishTiime + "' ";
        }
        try (Statement stmt = conn.createStatement()) {
            query = "update   " + auddbName + ".modules_audit_trail set status_code='" + statusCode + "',status='" + status + "',error_message='" + errorMessage + "', count='" + (numberOfRecord - 1) + "',"
                    + "action='update',execution_time=" + exec_time + " ,failure_count='" + failureCount + "' ,modified_on=CURRENT_TIMESTAMP where  id = " + id;
            logger.info(query);
            stmt.executeUpdate(query);
        } catch (Exception e) {
            logger.error(query + "Failed  " + e);
        }
    }
}
