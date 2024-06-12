package com.glocks.parser;

import com.glocks.util.FileList;
import com.glocks.parser.service.InsertDbDao;

import static com.glocks.dao.MessageConfigurationDbDao.sendMessageToMsisdn;
import static com.glocks.dao.ModuleAuditTrail.insertModuleAudit;
import static com.glocks.dao.ModuleAuditTrail.updateModuleAudit;
import static com.glocks.dao.ProcessP2_5DbDao.insertIntoDbForP2_5;
import static com.glocks.parser.service.insertUpdateQueryBuilder.*;
import com.glocks.rule.Rule;
import com.glocks.rule.RuleFilter;
import com.glocks.util.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static com.glocks.parser.service.SelectStatementResponse.*;
import static com.glocks.EdrP3Process.*;
import static com.glocks.util.Util.defaultStringtoDate;


//ETl-Class
@Component
public class ParserProcess {
    //
    static Logger logger = LogManager.getLogger(ParserProcess.class);

    public static void CdrParserProces(Connection conn, String filePath) {
        logger.debug(" FilePath :" + filePath);
        String source = null;
        String operator = null;
        if (filePath != null) {
            String[] arrOfStr = filePath.split("/", 0);
            int val = 0;
            for (int i = (arrOfStr.length - 1); i >= 0; i--) {
                if (val == 1) {
                    source = arrOfStr[i];
                }
                if (val == 2) {
                    operator = arrOfStr[i].toUpperCase();
                }
                val++;
            }
        }
        String fileName = new FileList().readOldestOneFile(filePath);
        if (fileName == null) {
            logger.debug(" No File Found");
            return;
        }
        logger.debug(" FilePath :" + filePath + "; FileName:" + fileName + ";source : " + source + " ; Operator : " + operator);
        String operator_tag = getOperatorTag(conn, operator);
        logger.debug("Operator tag is [" + operator_tag + "] ");
        ArrayList rulelist = new ArrayList<Rule>();
        String period = checkGraceStatus(conn);
        logger.debug("Period is [" + period + "] ");
        rulelist = getRuleDetails(operator, conn, operator_tag, period);
        logger.debug("rule list to be  " + rulelist);
        addCDRInProfileWithRule(operator, conn, rulelist, operator_tag, period, filePath, source, fileName);
    }

    private static void addCDRInProfileWithRule(String operator, Connection conn, ArrayList<Rule> rulelist, String operator_tag, String period, String filePath, String source, String fileName) {
        ExecutorService executorService = Executors.newCachedThreadPool();

        int insertedKey = insertModuleAudit(conn, "P3", operator + "_" + source);
        long executionStartTime = new Date().getTime();
        int output = 0;
        String my_query = "";
        HashMap<String, String> my_rule_detail;
        String failed_rule_name = "";
        int failed_rule_id = 0;
        String finalAction = "";
        int usageInsert = 0;
        int usageUpdate = 0;
        int duplicateInsert = 0;
        int duplicateUpdate = 0;

        int usageInsertForeign = 0;
        int usageUpdateForeign = 0;
        int duplicateInsertForeign = 0;
        int duplicateUpdateForeign = 0;

        int nullInsert = 0;
        int nullUpdate = 0;
        File file = null;
        String line = null;
        String[] data = null;
        BufferedReader br = null;
        FileReader fr = null;
        BufferedWriter bw1 = null;
        int counter = 1;
        int foreignMsisdn = 0;
        int fileParseLimit = 1;

        int errorCount = 0;
        int fileCount = 0;
        try {
            String server_origin = propertiesReader.serverName;
            file = new File(filePath + fileName);
            try (Stream<String> lines = Files.lines(file.toPath())) {
                fileCount = (int) lines.count();
                logger.debug("File Count: " + fileCount);
            } catch (Exception e) {
                logger.warn("" + e);
            }
            insertIntoDbForP2_5(conn,file.getAbsolutePath() , operator);
            String enableForeignSimHandling = getSystemConfigDetailsByTag(conn, "enableForeignSimHandling");
            fileParseLimit = getExsistingSqlFileDetails(conn, operator, source, fileName);
            fr = new FileReader(file);
            br = new BufferedReader(fr);

            bw1 = getSqlFileWriter(conn, operator, source, fileName);
            Date p2Starttime = new Date();
            HashMap<String, String> device_info = new HashMap<String, String>();
            RuleFilter rule_filter = new RuleFilter();
            // CDR File Writer
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = new Date();
            String sdfTime = sdf.format(date);

             br.readLine();

           @SuppressWarnings("unchecked")
            Map<String, String> operatorSeries = getActualOperator(conn);
            String[] testImies = getTestImeis(conn).split(",");
            HashMap<String, Date> validTacMap = getValidTac(conn);
            while ((line = br.readLine()) != null) {

                data = line.split(propertiesReader.commaDelimiter, -1);

                logger.info(" Line Started " + Arrays.toString(data));
                try {
                    device_info.put("file_name", fileName.trim());
                    device_info.put("IMEI", data[0].trim());
                    device_info.put("imsi", data[1].trim());
                    device_info.put("msisdn", ((data[2].trim().startsWith("19") || data[2].trim().startsWith("00")) ? data[2].substring(2) : data[2]));
                    device_info.put("timestamp", data[3].trim());  // timestamp //record_type
                    device_info.put("protocol", data[4].trim());  //protocol  // system_type
                    device_info.put("source", data[5].trim());
                    device_info.put("raw_cdr_file_name", data[6].trim());
                    device_info.put("modified_imei", data[0].length() > 14 ? data[0].substring(0, 14) : data[0]);
                    device_info.put("tac", data[0].length() > 8 ? data[0].substring(0, 8) : data[0]);
                    device_info.put("imei_arrival_time", data[7]);

                    device_info.put("operator", operator.trim());
                    device_info.put("record_time", sdfTime);
                    device_info.put("operator_tag", operator_tag);

              //      insertIntoDbForP2_5(conn,device_info);

                    device_info.put("actual_operator", operatorSeries.get(device_info.get("msisdn").substring(0, 5)));
                    boolean anyMatch = Arrays.stream(testImies).anyMatch(imei -> device_info.get("modified_imei").startsWith(imei));
                    if (anyMatch) {
                        executorService.execute(new InsertDbDao(conn, getTestImeiString(device_info)));
                    }
                    device_info.put("testImeiFlag", String.valueOf(anyMatch));
                    String failedRuleDate = null;
                    counter++;
                    if (device_info.get("msisdn").startsWith(propertiesReader.localMsisdnStartSeries)
                            && device_info.get("imsi").startsWith(propertiesReader.localISMIStartSeries)) {
                        logger.debug("Local Sim  " + Arrays.toString(data));
                        device_info.put("msisdn_type", "LocalSim");
                        my_rule_detail = rule_filter.getMyRule(conn, device_info, rulelist);
                        logger.debug("getMyRule done with rule name " + my_rule_detail.get("rule_name") + " rule ID " + my_rule_detail.get("rule_id"));
                        if (my_rule_detail.get("rule_name") != null) {
                            failed_rule_name = my_rule_detail.get("rule_name");
                            failed_rule_id = my_rule_detail.get("rule_id") == null ? 0
                                    : Integer.valueOf(my_rule_detail.get("rule_id"));
                            period = my_rule_detail.get("period");
                            failedRuleDate = dateFunction;
                        }
                        if (failed_rule_name == null || failed_rule_name.equals("")
                                || failed_rule_name.equalsIgnoreCase("EXISTS_IN_ALL_EDR_ACTIVE_DB")) {
                            finalAction = "ALLOWED";
                            failed_rule_name = null;
                            failed_rule_id = 0;
                        } else {
                            logger.debug("FailedRule Categorization");
                            if (failed_rule_name.equalsIgnoreCase("EXIST_IN_GSMABLACKLIST_DB")
                                    || failed_rule_name.equalsIgnoreCase("EXIST_IN_BLACKLIST_DB")) {
                                finalAction = "BLOCKED";
                            } else if (period.equalsIgnoreCase("Grace")) {
                                finalAction = "SYS_REG";
                            } else if (period.equalsIgnoreCase("Post_Grace")) {
                                finalAction = "USER_REG";
                                sendMessageToMsisdn(conn, device_info.get("msisdn"), device_info.get("IMEI"));
                            }
                        }
                    } else {
                        logger.debug("Foreign Sim Started " + Arrays.toString(data));
                        foreignMsisdn++;
                        device_info.put("msisdn_type", "ForeignSim");
                        if (enableForeignSimHandling.equals("False")) {
                            logger.debug("Foreign Sim Without Enable Foreign Sim ");
                            continue;
                        }
                    }
                    String gsmaTac = null;
                    if (validTacMap.containsKey(device_info.get("tac"))) {
                        gsmaTac = "V";
                        logger.debug("allocation_date after   : ----" + validTacMap.get(device_info.get("tac")));
                        if (validTacMap.get(device_info.get("tac")).before(new Date())) {
                            device_info.put("isUsedFlag", "false");
                        } else {
                            device_info.put("isUsedFlag", "true");
                        }
                    } else {
                        gsmaTac = "I";
                        logger.debug("allocation_date is null returning false  ");
                        device_info.put("isUsedFlag", "false");
                    }
                     output = checkDeviceUsageDB(conn, device_info.get("modified_imei"), device_info.get("msisdn"), device_info.get("imei_arrival_time"), device_info.get("msisdn_type"), device_info);
                    if (output == 0) { // imei not found in usagedb
                        logger.debug("imei not found in usagedb");
                        my_query = getInsertUsageDbQuery(device_info, dateFunction, failed_rule_name, failed_rule_id, period, finalAction, failedRuleDate, server_origin, gsmaTac);
                        if (device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")) {
                            usageInsert++;
                        } else {
                            usageInsertForeign++;
                        }
                    } else if (output == 1) { // new ArrivalTime came from file > arrival time in db already // imei found// // with same msisdn update_raw_cdr_file_name , update_imei_arrival_time//    logger.debug("new ArrivalTime  came  from file  >  arrival time in db already");
                        my_query = getUpdateUsageDbQueryWithRawCdrFileName(device_info, dateFunction, failed_rule_name, failed_rule_id, period, finalAction, failedRuleDate, server_origin, gsmaTac);
                        if (device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")) {
                            usageUpdate++;
                        } else {
                            usageUpdateForeign++;
                        }
                    } else if (output == 3) { // imei found with same msisdn update_raw_cdr_file_name  update_imei_arrival_time
                        //       logger.debug("imei found with same msisdn  update_raw_cdr_file_name , update_imei_arrival_time");
                        my_query = getUpdateUsageDbQuery(device_info, dateFunction, failed_rule_name, failed_rule_id, period, finalAction, failedRuleDate, server_origin, gsmaTac);
                        if (device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")) {
                            usageUpdate++;
                        } else {
                            usageUpdateForeign++;
                        }
                    } else if (output == 2) { // imei found with different msisdn
                        logger.debug("imei found with different msisdn");
                        output = checkDeviceDuplicateDB(conn, device_info.get("modified_imei"), device_info.get("msisdn"), device_info.get("imei_arrival_time"), device_info.get("msisdn_type"), device_info);
                        switch (output) {
                            case 0:
                                my_query = getInsertDuplicateDbQuery(device_info, dateFunction, failed_rule_name, failed_rule_id, period, finalAction, failedRuleDate, server_origin, gsmaTac);
                                if (device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")) {
                                    duplicateInsert++;
                                } else {
                                    duplicateInsertForeign++;
                                }
                                break;
                            case 1:
                                my_query = getUpdateDuplicateDbQueryWithRawCdrFileName(device_info, dateFunction, failed_rule_name, failed_rule_id, period, finalAction, failedRuleDate, server_origin, gsmaTac);
                                if (device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")) {
                                    duplicateUpdate++;
                                } else {
                                    duplicateUpdateForeign++;
                                }
                                break;
                            default:
                                my_query = getUpdateDuplicateDbQuery(device_info, dateFunction, failed_rule_name, failed_rule_id, period, finalAction, failedRuleDate, server_origin, gsmaTac);
                                if (device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")) {
                                    duplicateUpdate++;
                                } else {
                                    duplicateUpdateForeign++;
                                }
                                break;
                        }
                    }
                    logger.info("query : " + my_query);
                    if (my_query.contains("insert")) {
                        executorService.execute(new InsertDbDao(conn, my_query));
                    } else {
                        logger.info(" writing query in file== " + my_query);
                        bw1.write(my_query + ";");
                        bw1.newLine();
                    }
                    logger.info("Remaining List :: " + (fileCount - (counter + errorCount)));
                } catch (Exception e) {
                    logger.error("Error in line -- " + Arrays.toString(data) + " [] Error " + e.getLocalizedMessage() + " [] " + e.getMessage() + "Total ErrorCount -- " + errorCount++);
                }

            } // While End
            executorService.shutdown();
            Date p2Endtime = new Date();
            cdrFileDetailsUpdate(conn, operator, device_info.get("file_name"), usageInsert, usageUpdate, duplicateInsert, duplicateUpdate, nullInsert, nullUpdate, p2Starttime, p2Endtime, "all", counter, device_info.get("raw_cdr_file_name"),
                    foreignMsisdn, server_origin, usageInsertForeign, usageUpdateForeign, duplicateInsertForeign, duplicateUpdateForeign, errorCount);
            new FileList().moveCDRFile(conn, fileName, operator, filePath, source, getSystemConfigDetailsByTag(conn, "EdrProcessedFileStoragePath"));
            updateModuleAudit(conn, 200, "Success", "", insertedKey, executionStartTime, fileCount, errorCount);
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(ParserProcess.class.getName())).collect(Collectors.toList()).get(0) + "]");

            new Util().raiseAnAlertJar("alert1111", "" + e.getLocalizedMessage(), "EDR_P3_" + operator + "_" + source, 0);

            updateModuleAudit(conn, 500, "Failure", e.getLocalizedMessage(), insertedKey, executionStartTime, fileCount, errorCount);
        } finally {
            try {
                br.close();
                bw1.close();
            } catch (Exception e) {
                logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(ParserProcess.class.getName())).collect(Collectors.toList()).get(0) + "]");
            }
        }
    }

    private static int checkDeviceDuplicateDB(Connection conn, String imei, String msisdn, String imeiArrivalTime, String msisdnType, HashMap<String, String> device_info) {
        String dbName
                = msisdnType.equalsIgnoreCase("LocalSim")
                ? "" + edrappdbName + ".active_imei_with_different_imsi"
                : "" + edrappdbName + ".active_foreign_imei_with_different_imsi";

        int status = 0;
        String query = "select * from " + dbName + " where imei ='" + imei + "' and imsi = '" + device_info.get("imsi") + "'";//*****
        try (Statement stmt = conn.createStatement(); ResultSet rs1 = stmt.executeQuery(query)) {
            Date imeiArrival = new SimpleDateFormat("yyyyMMdd").parse(imeiArrivalTime);
            logger.debug("Checking duplicate  db" + query);
            while (rs1.next()) {
                if ((rs1.getString("update_imei_arrival_time") == null
                        || rs1.getString("update_imei_arrival_time").equals(""))
                        || (imeiArrival.compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(rs1.getString("update_imei_arrival_time"))) > 0)) { // imei   found  with same msisdn
                    status = 1; // update_raw_cdr_file_name='" + device_info.get("raw_cdr_file_name")
                } else {
                    status = 3;
                }
                if (!rs1.getString("msisdn").equalsIgnoreCase(device_info.get("msisdn"))) {  //*****
                    logger.debug(" Different msisdn, New  " + device_info.get("msisdn") + ", Db old msisdn " + rs1.getString("msisdn"));
                    insertIntoMISISDNChangeDB(conn, device_info, rs1.getString("msisdn"), rs1.getString("update_imei_arrival_time"), msisdnType, dbName  );
                }
            }
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(ParserProcess.class.getName())).collect(Collectors.toList()).get(0) + "]");
        }
        return status;
    }

    private static int checkDeviceUsageDB(Connection conn, String imeiIndex, String msisdn, String imeiArrivalTime, String msisdnType, HashMap<String, String> device_info) {
        String dbName = msisdnType.equalsIgnoreCase("LocalSim")
                ? "" + edrappdbName + ".active_unique_imei"
                : "" + edrappdbName + ".active_unique_foreign_imei";
        int status = 0; // imei not found
        String query = "select * from  " + dbName + " where imei ='" + imeiIndex + "'     ";
        try (Statement stmt = conn.createStatement(); ResultSet rs1 = stmt.executeQuery(query)) {
            Date imeiArrival = new SimpleDateFormat("yyyyMMdd").parse(imeiArrivalTime);
            while (rs1.next()) {
                if (rs1.getString("imsi").equalsIgnoreCase( device_info.get("imsi"))) {

                    if (!rs1.getString("msisdn").equalsIgnoreCase(device_info.get("msisdn"))) {
                        insertIntoMISISDNChangeDB(conn, device_info, rs1.getString("msisdn"), rs1.getString("update_imei_arrival_time"), msisdnType, dbName );
                    }
                    if ((rs1.getString("update_imei_arrival_time") == null || rs1.getString("update_imei_arrival_time").equals("")) || (imeiArrival.compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(rs1.getString("update_imei_arrival_time"))) > 0)) {
                        status = 1; // update_raw_cdr_file_name='" + device_info.get("raw_cdr_file_name")
                    } else {
                        status = 3; // not to update as UPDATE_IMEI_ARRIVAL_TIME is greater already
                    }
                } else {
                    status = 2; // // imei found with different msisdn
                }
            }
//            if (!msisdnType.equalsIgnoreCase("LocalSim")) {
//                logger.info("unique dbQuery " + query + "[0-Notfound,1,3-Found with same msisdn,2-Found with Diff msisdn] Result:" + status);
//            }
            rs1.close();
            stmt.close();
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(ParserProcess.class.getName())).collect(Collectors.toList()).get(0) + "]");
        }
        return status;
    }


    private static ArrayList getRuleDetails(String operator, Connection conn, String operator_tag, String period) {
        ArrayList rule_details = new ArrayList<Rule>();
        String query = null;
        ResultSet rs1 = null;
        Statement stmt = null;
        try {
            query = "select a.id as rule_id,a.name as rule_name,b.output as output,b.grace_action, b.post_grace_action, b.failed_rule_action_grace, b.failed_rule_action_post_grace " + " from " + appdbName + ".rule a, " + appdbName + ".feature_rule b where  a.name=b.name  and a.state='Enabled' and b.feature='EDR' and   b." + period
                    + "_action !='NA' order by b.rule_order asc";
            logger.info("Query is " + query);
            stmt = conn.createStatement();
            rs1 = stmt.executeQuery(query);
            while (rs1.next()) {
                    Rule rule = new Rule(
                            rs1.getString("rule_name"),
                            rs1.getString("output"),
                            rs1.getString("rule_id"),
                            period,
                            rs1.getString(period + "_action"),
                            rs1.getString("failed_rule_action_" + period));
                    rule_details.add(rule);
            }
            rs1.close();
            stmt.close();
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(ParserProcess.class.getName())).collect(Collectors.toList()).get(0) + "]");
        } finally {
            try {
                rs1.close();
                stmt.close();

            } catch (SQLException e) {
                logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(ParserProcess.class.getName())).collect(Collectors.toList()).get(0) + "]");
            }
        }
        return rule_details;
    }

    static void cdrFileDetailsUpdate(Connection conn, String operator, String fileName, int usageInsert, int usageUpdate, int duplicateInsert, int duplicateUpdate, int nullInsert, int nullUpdate, Date P2StartTime, Date P2EndTime, String source, int counter, String raw_cdr_file_name,
                                     int foreignMsisdn, String server_origin, int usageInsertForeign, int usageUpdateForeign, int duplicateInsertForeign, int duplicateUpdateForeign, int errorCount) {
        String query = null;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Statement stmt = null;
        query = "insert into  " + edrappdbName + ".cdr_file_processed_detail (total_inserts_in_usage_db,total_updates_in_usage_db ,total_insert_in_dup_db , total_updates_in_dup_db , total_insert_in_null_db , total_update_in_null_db , startTime , endTime ,operator , file_name, total_records_count ,"
                + " raw_cdr_file_name  ,source  ,foreignMsisdn  , STATUS , server_origin , total_inserts_in_foreignusage_db,total_updates_in_foreignusage_db ,total_insert_in_foreigndup_db , total_updates_in_foreigndup_db,total_error_record_count ) "
                + "values(   '" + usageInsert + "' , '" + usageUpdate + "'  , '" + duplicateInsert + "' , '" + duplicateUpdate + "' " + " ,'" + nullInsert + "' ,'" + nullUpdate + "', " + defaultStringtoDate(df.format(P2StartTime)) + " , " + defaultStringtoDate(df.format(P2EndTime)) + " ,   '" + operator + "', '" + fileName + "' , '"
                + (counter - 1) + "' , '" + raw_cdr_file_name + "' , '" + source + "'  , '" + foreignMsisdn + "' , 'End' ,  '" + server_origin + "'   ,   '" + usageInsertForeign + "' , '" + usageUpdateForeign + "'  , '" + duplicateInsertForeign + "' , '" + duplicateUpdateForeign + "'  , '" + errorCount + "'     )  ";
        logger.info(" query is " + query);
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            stmt.close();
        } catch (SQLException e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(ParserProcess.class.getName())).collect(Collectors.toList()).get(0) + "]");
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(ParserProcess.class.getName())).collect(Collectors.toList()).get(0) + "]");
            }
        }
    }




}
