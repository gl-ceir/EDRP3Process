package com.glocks.parser.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.glocks.EdrP3Process.*;
import static com.glocks.EdrP3Process.dateFunction;
import static com.glocks.util.Util.defaultStringtoDate;


public class SelectStatementResponse {
    static Logger logger = LogManager.getLogger(SelectStatementResponse.class);

    public static Map getActualOperator(Connection conn) {
        Map<String, String> operatorSeries = new HashMap<String, String>();
        String query = "select  series_start, operator_name from " + appdbName + ".operator_series";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query);) {
            while (rs.next()) {
                operatorSeries.put(rs.getString("series_start"), rs.getString("operator_name"));
            }
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(SelectStatementResponse.class.getName())).collect(Collectors.toList()).get(0) + "]");
        }
        return operatorSeries;
    }

    public static String getTestImeis(Connection conn) {
        String value = "";
        String query = "select value from " + appdbName + ".sys_param where tag= 'TEST_IMEI_SERIES' ";
        logger.info(" ----" + query);
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query);) {
            while (rs.next()) {
                value = rs.getString("value");
            }
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(SelectStatementResponse.class.getName())).collect(Collectors.toList()).get(0) + "]");
        }
        return value;
    }

    public static String getSystemConfigDetailsByTag(Connection conn, String tag) {
        String value = null;
        String query = "select value from " + appdbName + ".sys_param where tag='" + tag + "'";
        logger.info("Query " + query);
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query);) {
            while (rs.next()) {
                value = rs.getString("value");
            }
            return value;
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(SelectStatementResponse.class.getName())).collect(Collectors.toList()).get(0) + "]");
        }
        return value;
    }

    public static String checkGraceStatus(Connection conn) {
        String period = "";
        String query = null;
        ResultSet rs1 = null;
        Statement stmt = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date currentDate = new Date();
        Date graceDate = null;
        try {
            query = "select value from " + appdbName + ".sys_param where tag='GRACE_PERIOD_END_DATE'";
            stmt = conn.createStatement();
            rs1 = stmt.executeQuery(query);
            while (rs1.next()) {
                graceDate = sdf.parse(rs1.getString("value"));
                if (currentDate.compareTo(graceDate) > 0) {
                    period = "post_grace";
                } else {
                    period = "grace";
                }
            }
            logger.info("Period is " + period);
        } catch (Exception e) {
            logger.error("" + e);
        } finally {
            try {
                rs1.close();
                stmt.close();
            } catch (SQLException e) {
                logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(SelectStatementResponse.class.getName())).collect(Collectors.toList()).get(0) + "]");
            }
        }
        return period;
    }

    public static String getOperatorTag(Connection conn, String operator) {
        String operator_tag = "";
        String query = null;
        ResultSet rs1 = null;
        Statement stmt = null;
        try {
            query = "select * from " + appdbName + ".sys_param_list_value where tag='OPERATORS' and interpretation='" + operator + "'";
            logger.debug("get operator tag [" + query + "]");
            stmt = conn.createStatement();
            rs1 = stmt.executeQuery(query);
            while (rs1.next()) {
                operator_tag = rs1.getString("tag_id");
            }
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(SelectStatementResponse.class.getName())).collect(Collectors.toList()).get(0) + "]");
            operator_tag = "GSM"; // if no opertor found
        } finally {
            try {
                rs1.close();
                stmt.close();
            } catch (SQLException e) {
                logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(SelectStatementResponse.class.getName())).collect(Collectors.toList()).get(0) + "]");
            }
        }
        return operator_tag;
    }

    public static BufferedWriter getSqlFileWriter(Connection conn, String operator, String source, String file) {
        BufferedWriter bw1 = null;
        try {
            String foldrName = getSystemConfigDetailsByTag(conn, "EDR_Sql_Query_Folder") + "/" + operator.toLowerCase() + "/"; //
            File file1 = new File(foldrName);
            if (!file1.exists()) {
                file1.mkdir();
            }
            foldrName += source + "/";
            file1 = new File(foldrName);
            if (!file1.exists()) {
                file1.mkdir();
            }
            String fileNameInput1 = foldrName + file + ".sql";
            logger.info("SQL_LOADER NAME ..  " + fileNameInput1);
            File fout1 = new File(fileNameInput1);
            FileOutputStream fos1 = new FileOutputStream(fout1, true);
            bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
        } catch (Exception e) {
            logger.error("error:: " + e);
        }
        return bw1;
    }

    public static int getExsistingSqlFileDetails(Connection conn, String operator, String source, String file) {
        int fileCount = 1;
        File file1 = null;
        try {
            String foldrName = getSystemConfigDetailsByTag(conn, "EDR_Sql_Query_Folder") + "/" + operator.toLowerCase() + "/"; //
            foldrName += source + "/";
            String fileNameInput1 = foldrName + file + ".sql";
            try {
                logger.info("SQL " + fileNameInput1);
                file1 = new File(fileNameInput1);

                // BufferedReader reader = new BufferedReader(new FileReader(fileNameInput1));
                // int lines = 0;
                // while (reader.readLine() != null) {
                // lines++;
                // }
                // reader.close();
                File myObj = new File(fileNameInput1);
                if (myObj.delete()) ;

                // try (Stream<String> lines = Files.lines(file1.toPath())) {
                // fileCount = (int) lines.count();
                // logger.info("File Count of Sql File: " + fileCount);
                // }
            } catch (Exception e) {
                logger.error("File not   exist : " + e);
            }

        } catch (Exception e) {
            logger.error("Err0r : " + e);
        }
        return fileCount;
    }

    public static HashMap<String, Date> getValidTac(Connection conn) {
        HashMap<String, Date> validTacMap = new HashMap<>();
        String timePeriod = getSystemConfigDetailsByTag(conn, "IS_USED_EXTENDED_DAYS");
        logger.info("Time Period in days  : ----" + timePeriod);
        Calendar calendar = Calendar.getInstance();
        String query = "select device_id , allocation_date from " + appdbName + ".mobile_device_repository   ";
        logger.info("Query ----" + query);
        // Get the new date after adding 50 days
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query);) {
            while (rs.next()) {
                //    var newDate = rs.getDate("allocation_date").toLocalDate().plusDays(Integer.valueOf(timePeriod));
                calendar.setTime(rs.getDate("allocation_date"));
                calendar.add(Calendar.DAY_OF_MONTH, Integer.valueOf(timePeriod));
                validTacMap.put(rs.getString("device_id"), calendar.getTime());
            }
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(SelectStatementResponse.class.getName())).collect(Collectors.toList()).get(0) + "]");
        }
        logger.info("Query completed");
        return validTacMap;
    }

}
