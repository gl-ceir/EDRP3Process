package com.glocks.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

//ETl-Class
public class FileList {

    static Logger logger = LogManager.getLogger(FileList.class);

    public String readOldestOneFile(String basePath) {
        File oldestFile = null;
        try {
            logger.info(" basePath  :" + basePath);
            File logDir = new File(basePath);
            File[] logFiles = logDir.listFiles();
            long oldestDate = Long.MAX_VALUE;
            for (File f : logFiles) {
                if (f.lastModified() < oldestDate) {
                    oldestDate = f.lastModified();
                    oldestFile = f;
                }
            }
            if (oldestFile != null) {
                logger.debug("oldestFile " + oldestFile);
            } else {
                logger.info("No File Found");
            }
        } catch (Exception e) {
            logger.trace("No File :" +e);
        }
        return oldestFile.getName().toString();
    }

    public void moveCDRFile(Connection conn, String fileName, String opertorName1, String fileFolderPath, String source, String storagePath) {
        String opertorName = opertorName1.toLowerCase();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");
        String date = df.format(new Date());
        try {
            //    File  folder = new File(storagePath + opertorName);
//            if (!folder.exists()) {
//                folder.mkdir();
//            }
//            logger.debug("folder Created ::" + folder);
//            folder = new File(storagePath + opertorName + "/" + source);
//            if (!folder.exists()) {
//                folder.mkdir();
//            }
//            folder = new File(storagePath + opertorName + "/" + source + "/" + date);    //+ "/" + datewithTime
//            if (!folder.exists()) {
//                folder.mkdir();
//            }
            Files.createDirectories(Paths.get(storagePath + opertorName + "/" + source + "/" + date));
            Path path = Paths.get(storagePath + opertorName + "/" + source + "/" + date + "/" + fileName);
            Files.deleteIfExists(path);
            logger.info(" File Move From ::" + fileFolderPath + fileName);
            Path temp = Files.move(Paths.get(fileFolderPath + fileName),
                    Paths.get(storagePath + opertorName + "/" + source + "/" + date + "/" + fileName));
            if (temp != null) {
                logger.info("File renamed and moved successfully to {} " ,storagePath + opertorName + "/" + source + "/" + date + "/" + fileName);
            }
        } catch (Exception e) {
            logger.error("Error :" + e);
        }
    }

}
