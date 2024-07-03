/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.glocks.parser.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.glocks.EdrP3Process.*;
import static com.glocks.parser.service.insertUpdateQueryBuilder.*;


public class InsertDbDao implements Runnable {
    static Logger logger = LogManager.getLogger(InsertDbDao.class);
    String query;
    Connection conn;
    HashMap<String, String> deviceInfo;
    BufferedWriter bw;
    public InsertDbDao(Connection conn, String query) {
        this.conn = conn;
        this.query = query;
    }

    public InsertDbDao(Connection conn, String query, HashMap<String, String> deviceInfo, BufferedWriter bw) {
        this.conn = conn;
        this.query = query;
        this.deviceInfo = deviceInfo;
        this.bw = bw;
    }

    @Override
    public void run() {
        logger.info("[RUNNABLE Query]" + query);
        try (Statement stmtNew = conn.createStatement()) {
            stmtNew.executeUpdate(query);
        } catch (Exception e) {
            logger.error("[]" + query + "[] Error occured in Thread while inserting query  -- " + e.getLocalizedMessage() + "At ---" + e);
            imeiCheck();
        }
    }

    private void imeiCheck() {
        String my_query = null;
        usageInsert--;
        String qury = "select imsi from  active_unique_imei  where imei ='" + deviceInfo.get("modified_imei") + "'    ";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(qury)) {
            boolean avail = false;
            while (rs.next()) {
                if (rs.getString("imsi").equalsIgnoreCase(deviceInfo.get("imsi"))) {
                    avail = true;
                }
            }
            if (avail) {
                my_query = getUpdateUsageDbQueryWithRawCdrFileName(deviceInfo);
                usageUpdate++;
            } else {
                String qry = "select imsi from active_imei_with_different_imsi  where imei ='" + deviceInfo.get("modified_imei") + "' and imsi ='" + deviceInfo.get("imsi") + "'    ";
                try (ResultSet rs1 = stmt.executeQuery(qry)) {
                    List<String> list = new ArrayList<>();
                    while (rs1.next()) {
                        list.add(rs1.getString("imsi"));
                    }
                    if (list.isEmpty() || !list.stream().anyMatch(a -> a.equalsIgnoreCase(deviceInfo.get("imsi")))) {  // insert
                        my_query = getInsertDuplicateDbQuery(deviceInfo);
                        duplicateInsert++;
                    } else {
                        my_query = getUpdateDuplicateDbQuery(deviceInfo);
                        duplicateUpdate++;
                    }
                }
            }
            logger.info("Final* Statement: " + my_query);
            if (my_query.contains("insert")) {
                stmt.executeUpdate(my_query);
            } else {
                bw.write(my_query + ";");
                bw.newLine();
            }
        } catch (Exception e) {
            logger.error("[]" + query + "[] Error InSert Dao  query  -- " + e.getLocalizedMessage() + "At ---" + e);
        }
    }
}
