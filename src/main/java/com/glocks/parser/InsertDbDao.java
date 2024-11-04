
package com.glocks.parser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

//ETl-Class
public class InsertDbDao implements Runnable {

    static Logger logger = LogManager.getLogger(InsertDbDao.class);

    String query;
    Connection conn;
    Map<String, String> map;

    public InsertDbDao() {
    }

    public InsertDbDao(Connection conn, String query) {
        this.conn = conn;
        this.query = query;
    }

    public InsertDbDao(Connection conn, String query, Map<String, String> map) {
        this.conn = conn;
        this.query = query;
        this.map = map;
    }

    public void insertIntoTable(Connection conn, String query) {
        try (Statement stmtNew = conn.createStatement()) {
            stmtNew.executeUpdate(query);
        } catch (Exception e) {
            logger.error("[]" + query + "[] Error occured in Thread while inserting query  -- " + e.getLocalizedMessage() + "At ---" + e);

        }
    }

    @Override
    public void run() {
        logger.info("[RUNNABLE Query]" + query);
        try (Statement stmtNew = conn.createStatement()) {
            stmtNew.executeUpdate(query);
        } catch (Exception e) {
            logger.error("[]" + query + "[] Error occured in Thread while inserting query  -- " + e.getLocalizedMessage() + "At ---" + e);
        }
    }

}
