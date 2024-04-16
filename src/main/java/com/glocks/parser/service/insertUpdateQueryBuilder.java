package com.glocks.parser.service;

import com.glocks.parser.ParserProcess;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

import static com.glocks.EdrP3Process.*;
import static com.glocks.EdrP3Process.dateFunction;
import static com.glocks.util.Util.defaultStringtoDate;


public class insertUpdateQueryBuilder {

    static Logger logger = LogManager.getLogger(insertUpdateQueryBuilder.class);


    public static String getInsertUsageDbQuery(HashMap<String, String> device_info, String dateFunction, String failed_rule_name, int failed_rule_id, String period, String finalAction, String failedRuleDate, String server_origin, String gsmaTac) {
        String dbName = device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")
                ? "  " + edrappdbName + ".active_unique_imei"
                : " " + edrappdbName + ".active_unique_foreign_imei";
        return " insert into " + dbName
                + " (actual_imei,msisdn,imsi,create_filename,update_filename,"
                + "updated_on,created_on,protocol,failed_rule_id,failed_rule_name,tac,period,action "
                + " , mobile_operator , timestamp , failed_rule_date,  modified_on ,record_time, imei , raw_cdr_file_name , imei_arrival_time , "
                + "source, update_source, feature_name , server_origin , update_imei_arrival_time ,update_raw_cdr_file_name ,actual_operator,test_imei, is_used) "
                + " values('" + device_info.get("IMEI") + "'," + "'" + device_info.get("msisdn") + "'," + "'" + device_info.get("imsi") + "'," + "'" + device_info.get("file_name") + "'," + "'" + device_info.get("file_name") + "',"
                + "" + dateFunction + "," + "" + dateFunction + "," + "'" + device_info.get("protocol") + "'," + "'" + failed_rule_id + "'," + "'" + failed_rule_name + "'," + "'" + device_info.get("tac") + "'," + "'" + period + "'," + "'" + finalAction + "' , " + "'" + device_info.get("operator") + "' , "
                + "'" + device_info.get("timestamp") + "'," + " " + failedRuleDate + " ," + "" + dateFunction + "," + " " + defaultStringtoDate(device_info.get("record_time")) + " , " + "'" + device_info.get("modified_imei") + "', " + "'" + device_info.get("raw_cdr_file_name") + "',"
                + " " + defaultStringtoDate(device_info.get("imei_arrival_time")) + " ," + "'" + device_info.get("source") + "' , " + "'" + device_info.get("source") + "' , " + "'" + gsmaTac + "' , " + "'" + server_origin
                + "' , " + " " + defaultStringtoDate(device_info.get("imei_arrival_time")) + " ," + "'" + device_info.get("raw_cdr_file_name") + "' , '" + device_info.get("actual_operator") + "'   , '" + device_info.get("testImeiFlag") + "'  , '" + device_info.get("isUsedFlag") + "'           )";
    }

    public static String getUpdateUsageDbQueryWithRawCdrFileName(HashMap<String, String> device_info, String dateFunction, String failed_rule_name, int failed_rule_id, String period, String finalAction, String failedRuleDate, String server_origin, String gsmaTac) {
        String dbName
                = device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")
                ? "" + edrappdbName + ".active_unique_imei"
                : "" + edrappdbName + ".active_unique_foreign_imei";
        return "update " + dbName + " set " + "update_filename = '" + device_info.get("file_name") + "', updated_on=" + dateFunction + "" + ", modified_on=" + dateFunction + "" + ", failed_rule_date=" + failedRuleDate + "" + ", failed_rule_id='" + failed_rule_id + "', failed_rule_name='" + failed_rule_name + "',"
                + "period='" + period + "',update_raw_cdr_file_name='" + device_info.get("raw_cdr_file_name") + "',update_imei_arrival_time= " + defaultStringtoDate(device_info.get("imei_arrival_time")) + " ,update_source ='" + device_info.get("source") + "',server_origin ='" + server_origin + "',action='" + finalAction + "' , imsi = '" + device_info.get("imsi")
                + "' , is_used = '" + device_info.get("isUsedFlag") + "'  , test_imei = '" + device_info.get("testImeiFlag") + "'  , msisdn = '" + device_info.get("msisdn") + "'    "
                + "      where imei ='" + device_info.get("modified_imei") + "'";
    }

    public static String getUpdateUsageDbQuery(HashMap<String, String> device_info, String dateFunction, String failed_rule_name, int failed_rule_id, String period, String finalAction, String failedRuleDate, String server_origin, String gsmaTac) {
        String dbName
                = device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")
                ? "" + edrappdbName + ".active_unique_imei"
                : "" + edrappdbName + ".active_unique_foreign_imei";
        return "update " + dbName + " set " + "update_filename = '" + device_info.get("file_name") + "', updated_on=" + dateFunction + "" + ", modified_on=" + dateFunction + "" + ", failed_rule_date=" + failedRuleDate + ""
                + ", failed_rule_id='" + failed_rule_id + "', failed_rule_name='" + failed_rule_name + "',period='" + period + "',"
                + " update_raw_cdr_file_name='" + device_info.get("raw_cdr_file_name") + "',update_imei_arrival_time=" + defaultStringtoDate(device_info.get("imei_arrival_time"))
                + ",     update_source ='" + device_info.get("source") + "',server_origin ='" + server_origin + "',action='" + finalAction
                + "', imsi = '" + device_info.get("imsi") + "'   , msisdn = '" + device_info.get("msisdn") + "'  , is_used = '" + device_info.get("isUsedFlag") + "'   , test_imei = '" + device_info.get("testImeiFlag") + "'      where imei ='" + device_info.get("modified_imei") + "'  ";
    }

    public static String getInsertDuplicateDbQuery(HashMap<String, String> device_info, String dateFunction, String failed_rule_name, int failed_rule_id, String period, String finalAction, String failedRuleDate, String server_origin, String gsmaTac) {
        String dbName
                = device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")
                ? "" + edrappdbName + ".active_imei_with_different_imsi"
                : "" + edrappdbName + ".active_foreign_imei_with_different_imsi";

        return "insert into  " + dbName
                + "  (actual_imei,msisdn,imsi,create_filename,update_filename,"
                + "updated_on,created_on,protocol,failed_rule_id,failed_rule_name,tac,period,action  "
                + " , mobile_operator , timestamp , failed_rule_date,  modified_on  ,record_time, imei ,raw_cdr_file_name , imei_arrival_time , source ,update_source, feature_name ,server_origin "
                + "  , update_raw_cdr_file_name ,update_imei_arrival_time,actual_operator ,test_imei ,is_used ) "
                + "values('" + device_info.get("IMEI") + "'," + "'" + device_info.get("msisdn") + "'," + "'" + device_info.get("imsi") + "'," + "'" + device_info.get("file_name") + "',"
                + "'" + device_info.get("file_name") + "'," + "" + dateFunction + "," + "" + dateFunction + "," + "'" + device_info.get("protocol") + "'," + "'" + failed_rule_id + "'," + "'" + failed_rule_name + "'," + "'" + device_info.get("tac") + "'," + "'" + period + "',"
                + "'" + finalAction + "' , " + "'" + device_info.get("operator") + "' , " + "'" + device_info.get("timestamp") + "' , " + "" + failedRuleDate + " , " + "" + dateFunction + ",  " + " " + defaultStringtoDate(device_info.get("record_time")) + " , " + "'" + device_info.get("modified_imei") + "', " + "'" + device_info.get("raw_cdr_file_name") + "',"
                + " " + defaultStringtoDate(device_info.get("imei_arrival_time")) + "," + "'" + device_info.get("source") + "' , " + "'" + device_info.get("source") + "' , " + "'" + gsmaTac + "' , " + "'"
                + server_origin + "' , " + "'" + device_info.get("raw_cdr_file_name") + "'," + "" + defaultStringtoDate(device_info.get("imei_arrival_time")) + "   ,  '" + device_info.get("actual_operator") + "'    ,  '" + device_info.get("testImeiFlag") + "'  ,  '" + device_info.get("isUsedFlag") + "'             )";
    }

    public static String getUpdateDuplicateDbQueryWithRawCdrFileName(HashMap<String, String> device_info, String dateFunction, String failed_rule_name, int failed_rule_id, String period, String finalAction, String failedRuleDate, String server_origin, String gsmaTac) {
        String dbName
                = device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")
                ? "" + edrappdbName + ".active_imei_with_different_imsi"
                : "" + edrappdbName + ".active_foreign_imei_with_different_imsi";


        return "update " + dbName + " set " + "update_filename = '" + device_info.get("file_name") + "', updated_on=" + dateFunction + "" + ", modified_on=" + dateFunction
                + "" + ", failed_rule_id='" + failed_rule_id + "', failed_rule_name='" + failed_rule_name + "',period='" + period + "',update_raw_cdr_file_name='" + device_info.get("raw_cdr_file_name")
                + "',update_source ='" + device_info.get("source") + "',update_imei_arrival_time= " + defaultStringtoDate(device_info.get("imei_arrival_time")) + ",server_origin='" + server_origin + "'      ,action='"
                + finalAction + "'  ,is_used='" + device_info.get("isUsedFlag") + "'   , test_imei = '" + device_info.get("testImeiFlag") + "'       , msisdn = '" + device_info.get("msisdn") + "'    where imsi='" + device_info.get("imsi") + "'  and imei='" + device_info.get("modified_imei") + "'";
    }

    public static String getUpdateDuplicateDbQuery(HashMap<String, String> device_info, String dateFunction, String failed_rule_name, int failed_rule_id, String period, String finalAction, String failedRuleDate, String server_origin, String gsmaTac) {
        String dbName = device_info.get("msisdn_type").equalsIgnoreCase("LocalSim")
                ? "" + edrappdbName + ".active_imei_with_different_imsi"
                : "" + edrappdbName + ".active_foreign_imei_with_different_imsi";

        return "update " + dbName + " set " + "update_filename = '" + device_info.get("file_name") + "', updated_on=" + dateFunction + "" + ", modified_on=" + dateFunction + "" + ", failed_rule_id='" + failed_rule_id + "', failed_rule_name='"
                + failed_rule_name + "',period='" + period + "'  ,"
                + " update_raw_cdr_file_name='" + device_info.get("raw_cdr_file_name") + "',update_imei_arrival_time=" + defaultStringtoDate(device_info.get("imei_arrival_time"))
                + " , msisdn='" + device_info.get("msisdn") + "'  ,  update_source ='" + device_info.get("source") + "',   server_origin='" + server_origin + "',action='" + finalAction + "' ,is_used='" + device_info.get("isUsedFlag") + "' , test_imei = '"
                + device_info.get("testImeiFlag") + "'        where imsi='" + device_info.get("imsi") + "' and    imei='" + device_info.get("modified_imei") + "'";
    }

    public static String getTestImeiString(HashMap<String, String> device_info) {
        return " insert into " + edrappdbName + " .test_imei_details  " + "(imei ,imsi, timestamp , protocol , source,raw_cdr_file_name,imei_arrival_time ,operator, file_name ,msisdn, created_on , modified_on    )  values "
                + "('" + device_info.get("modified_imei") + "' , '" + device_info.get("imsi") + "', '" + device_info.get("timestamp") + "' ,'" + device_info.get("protocol") + "' , '" + device_info.get("source") + "',  '" + device_info.get("raw_cdr_file_name") + "', "
                + "" + defaultStringtoDate(device_info.get("imei_arrival_time")) + ", '" + device_info.get("operator") + "', '" + device_info.get("file_name") + "',   '" + device_info.get("msisdn") + "', " + dateFunction + ", " + dateFunction + "  ) ";

    }

    public static void insertIntoMISISDNChangeDB(Connection conn, HashMap<String, String> device_info, String oldmsisdn, String oldImsiDate, String msisdnType, String dbTable ) {
        String dbName = msisdnType.equalsIgnoreCase("LocalSim")
                ? "" + edrappdbName + ".active_imei_with_same_imsi_different_msisdn"
                : "" + edrappdbName + ".active_foreign_imei_with_same_imsi_different_msisdn";
        String value = " insert into " + dbName + " (imei ,imsi,old_msisdn,old_msisdn_date,new_msisdn, new_msisdn_date,operator,file_name,created_on ,db_table ) values  ("
                + " '" + device_info.get("IMEI") + "', '" + device_info.get("imsi") + "', '" + oldmsisdn + "', " + defaultStringtoDate(oldImsiDate) + ", '" + device_info.get("msisdn") + "', "
                + " " + defaultStringtoDate(device_info.get("imei_arrival_time")) + " , '" + device_info.get("operator") + "',  '" + device_info.get("file_name") + "',  " + dateFunction + " ,  '" + dbTable + "'   ) ";
         try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(value);
        } catch (Exception e) {
            logger.error( "QUERY [[" + value + "]]"  + e + "in   [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(insertUpdateQueryBuilder.class.getName())).collect(Collectors.toList()).get(0) + "]");
        }
    }

}
