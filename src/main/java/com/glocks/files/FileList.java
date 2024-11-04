package com.glocks.files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
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
            } else {
                logger.info("No File Found");
            }
        } catch (Exception e) {
            logger.debug("No File Found");
        }
        return oldestFile.getName().toString();
    }

    public void moveCDRFile(Connection conn, String fileName, String opertorName1, String fileFolderPath, String source, String storagePath) {

        String opertorName = opertorName1.toLowerCase();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");
        String date = df.format(new Date());

        LocalDateTime myObj = LocalDateTime.now();

        String timeSec = myObj.toString().substring((myObj.toString().length() - 3), (myObj.toString().length()));

        File folder = new File(storagePath + opertorName);
        try {
            if (!folder.exists()) {
                folder.mkdir();
            }
            logger.debug("folder Created ::" + folder);
            folder = new File(storagePath + opertorName + "/" + source);
            if (!folder.exists()) {
                folder.mkdir();
            }
            folder = new File(storagePath + opertorName + "/" + source + "/" + date);    //+ "/" + datewithTime
            if (!folder.exists()) {
                folder.mkdir();
            }

            try {
                Path path = Paths.get(folder + "/" + fileName);
                Files.deleteIfExists(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info(" File Move From ::" + fileFolderPath + fileName);

            Path temp = Files.move(Paths.get(fileFolderPath + fileName),
                    Paths.get(storagePath + opertorName + "/" + source + "/" + date + "/" + fileName));

            if (temp != null) {
                logger.info("File renamed and moved successfully");
            } else {
                logger.warn("Failed to move the file");
            }
        } catch (IOException e) {
            logger.error("Error :" + e);
        }
    }

}
