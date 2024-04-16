package com.glocks.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.glocks.EdrP3Process.edrappdbName;

public class ProcessP2_5DbDao {
    static Logger logger = LogManager.getLogger(ProcessP2_5DbDao.class);

    public static void insertIntoDbForP2_5(Connection conn, String csvFilePath) {

        String sdfTime = new SimpleDateFormat("yyyyMMdd").format(new Date());

        String tableName = edrappdbName + ".edr_" + sdfTime;
        StringBuilder loadQuery = new StringBuilder();

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("create table if not exists " + tableName + " like  " + edrappdbName + ".edr_20240416");
        } catch (Exception e) {
            logger.error("Not able to create Table " + tableName);
        }

        loadQuery.append("LOAD DATA LOCAL INFILE '")
                .append(csvFilePath)
                .append("' INTO TABLE ")
                .append(tableName)
                .append(" FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' " +
                        " IGNORE 1 LINES  (imei,imsi, msisdn,timestamp, protocol , source, fileName,imei_arrival_time)  ");  //'\r\n'
        logger.info("tableName" + tableName + ";; Query :: " + loadQuery);
        try (PreparedStatement statement = conn.prepareStatement(loadQuery.toString())) {
            statement.execute(loadQuery.toString());
            logger.info("CSV file loaded into MySQL table successfully.");

        } catch (SQLException e) {
            logger.error(e + e.getMessage());

        }
    }
}
//String csvFilePath = "/u02/ceirdata/processed_cdr/seatel/edr1/output/SEATEL_EDR1202403012009.csv";

//String query = "insert into  " + tableName + " ( imei,imsi, msisdn,timestamp, protocol , source, file_name,created_on) values "
//        + " (?, ?,?, ?,? ,?,?, " + dateFunction + ")";
//        try (
//PreparedStatement preparedStatement = conn.prepareStatement(query);) {
//        preparedStatement.setString(1, device_info.get("imei").toString());
//        preparedStatement.setString(2, device_info.get("imsi").toString());
//        preparedStatement.setString(3, device_info.get("msisdn").toString());
//        preparedStatement.setString(4, device_info.get("timestamp").toString());
//        preparedStatement.setString(5, device_info.get("protocol").toString());
//        preparedStatement.setString(6, device_info.get("source").toString());
//        preparedStatement.setString(7, device_info.get("file_name").toString());
//        //  logger.info("Query " + preparedStatement);
//        preparedStatement.execute();
////      logger.info("Inserted in " + tableName + " succesfully.");
//        } catch (SQLException e) {
//        logger.error("Error while executing " + e.getMessage(), e);
//        }

//PreparedStatement preparedStmt = null;
//        try (Connection connection = getConnection()){
//preparedStmt = connection.prepareStatement(loadQuery.toString());
//
//        connection.setAutoCommit(false);
//             preparedStmt.execute(loadQuery.toString());
//        connection.commit();PreparedStatement preparedStmt = null;
//        try (Connection connection = getConnection()){
//preparedStmt = connection.prepareStatement(loadQuery.toString());
//
//        connection.setAutoCommit(false);
//             preparedStmt.execute(loadQuery.toString());
//        connection.commit();