package com.glocks.rule;

import com.gl.rule_engine.RuleEngineApplication;
import com.gl.rule_engine.RuleInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

import static com.glocks.EdrP3Process.*;
import static com.glocks.util.Util.defaultStringtoDate;

public class RuleFilter {

    private static Logger logger = LogManager.getLogger(RuleFilter.class);

    public HashMap getMyRule(Connection conn, HashMap<String, String> device_info, ArrayList<Rule> rulelist ) {
        BufferedWriter bw = null;
        HashMap<String, String> rule_detail = new HashMap<String, String>();    // CDR
        String output = "Yes";
        String action_output = "";
        for (Rule rule : rulelist) {
            logger.debug("  MyRule  " + rule.rule_name);
            device_info.put("rule_name", rule.rule_name);
            device_info.put("output", rule.output);
            device_info.put("ruleid", rule.ruleid);
            device_info.put("period", rule.period);
            device_info.put("action", rule.action);
            device_info.put("failed_rule_aciton", rule.failed_rule_aciton);
            try {
                RuleInfo re = new RuleInfo( edrappdbName, appdbName, auddbName, repdbName, device_info.get("rule_name"), "executeRule", "CDR", device_info.get("IMEI"), "0", device_info.get("IMEI").length() > 14 ? device_info.get("IMEI").substring(0, 14) : device_info.get("IMEI"),
                        "0", device_info.get("operator"), "imei", device_info.get("operator_tag"), device_info.get("msisdn"), device_info.get("action"),
                        device_info.get("imsi"), device_info.get("timestamp"), device_info.get("protocol"), device_info.get("source"),
                        device_info.get("raw_cdr_file_name"), device_info.get("imei_arrival_time"), "txnId", "fileArray", device_info.get("period"), conn, bw);

                logger.debug("Rule Started .." + device_info.get("rule_name"));
                output = RuleEngineApplication.startRuleEngine(re);
                logger.debug("Rule End .." + device_info.get("rule_name") + "; Rule Output:" + output + " ;  Expected O/T : " + device_info.get("output"));
            } catch (Exception e) {
                logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(RuleFilter.class.getName())).collect(Collectors.toList()).get(0) + "]");
            }
            if (device_info.get("output").equalsIgnoreCase(output)) {
                rule_detail.put("rule_name", null);
            } else {
                RuleInfo re = new RuleInfo(edrappdbName, appdbName, auddbName, repdbName, device_info.get("rule_name"), "executeAction", "CDR", device_info.get("IMEI"), "0", device_info.get("file_name"),
                        "0", device_info.get("operator"), "error", device_info.get("operator_tag"), device_info.get("msisdn"), device_info.get("action"),
                        device_info.get("imsi"), device_info.get("timestamp"), device_info.get("protocol"), device_info.get("source"),
                        device_info.get("raw_cdr_file_name"), device_info.get("imei_arrival_time"), "txnId", "fileArray", device_info.get("period"), conn, bw);
                try {
                    action_output = RuleEngineApplication.startRuleEngine(re);
                } catch (Exception e) {
                    logger.error(e + "in [[[" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(RuleFilter.class.getName())).collect(Collectors.toList()).get(0) + "]");
                }
                if (device_info.get("rule_name").equalsIgnoreCase("TEST_IMEI")) {
                    rule_detail.put("test_imei", "true");
                }
                if (device_info.get("failed_rule_aciton").equalsIgnoreCase("rule")) {
                    if (!device_info.get("rule_name").equalsIgnoreCase("EXISTS_IN_ALL_ACTIVE_DB")) {
                        insertInDeviceInvalidDb(conn, device_info);
                    }
                    rule_detail.put("rule_name", null);
                } else {
                    rule_detail.put("period", device_info.get("period"));
                    rule_detail.put("action", device_info.get("action"));
                    rule_detail.put("output", "Yes");
                    rule_detail.put("rule_name", device_info.get("rule_name"));
                    rule_detail.put("rule_id", device_info.get("ruleid"));
                    if (!device_info.get("rule_name").equalsIgnoreCase("EXISTS_IN_ALL_ACTIVE_DB")) {
                        insertInDeviceInvalidDb(conn, device_info);
                    }
                    break;
                }
            }
        }
        return rule_detail;
    }

    public void insertInDeviceInvalidDb(Connection conn, HashMap<String, String> device_info) {
        try (Statement stmt = conn.createStatement();) {
            String query = "insert into " + edrappdbName + ".invalid_imei (IMEI_ESN_MEID ,RULE_NAME ,OPERATOR_NAME, SN_OF_DEVICE ,OPERATOR_TYPE ,  FILE_NAME ,RECORD_DATE, imsi, msisdn ) "
                    + " values ( '" + device_info.get("IMEI") + "','" + device_info.get("rule_name") + "','" + device_info.get("operator") + "', ' ' ,'" + device_info.get("operator_tag") + "' ,'" + device_info.get("file_name") + "'  , " + defaultStringtoDate(device_info.get("record_time")) + " , '" + device_info.get("imsi") + "' ,   '" + device_info.get("msisdn") + "' ) ";
            logger.debug("Qury " + query);
            stmt.executeUpdate(query);
        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(RuleFilter.class.getName())).collect(Collectors.toList()).get(0) + "]");
        }
    }
}

