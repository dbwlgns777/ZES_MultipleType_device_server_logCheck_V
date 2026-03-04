package com.zes.device;

import com.zes.device.models.ZES_Data;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ZES_SQLGenerator
{
    private static final String ZES_gv_networkCheckRealtimeTable = "pms_network_check_realtime";

    public static String addQuote(Object value)
    {
        return "'" + value + "'";
    }

    public static PreparedStatement findByIctNumber(Connection conn, String ictNumber, String tableName) throws SQLException {
        String ZES_lv_selectQuery = String.format("SELECT * FROM %s WHERE ict_number=?", tableName);
        PreparedStatement ZES_lv_prepStmt = conn.prepareStatement(ZES_lv_selectQuery);
        ZES_lv_prepStmt.setString(1, ictNumber);
        return ZES_lv_prepStmt;
    }

    public static String getInsertErrorNumQuery(String ictNumber, long flag, long errorNum, String tableName, long timestamp) {
        return String.format("INSERT INTO %s VALUES(%s, %s, %s, %s)", tableName, addQuote(ictNumber), addQuote(flag), addQuote(errorNum), convertTimestampToMySQLTimestamp(timestamp) );
    }

    public static String getInsertQuery(ZES_Data[] dataMap, String ictNumber, String tableName, long timestamp) {
        return String.format("INSERT INTO %s VALUES(%s, %s)", tableName, addQuote(ictNumber), formatInsertValues(dataMap, timestamp) );
    }

    public static String getUpdateQuery(ZES_Data[] dataMap, String ictNumber, String tableName, long timestamp) {
        return String.format("UPDATE %s SET %s WHERE ict_number=%s", tableName, formatUpdateValues(dataMap, timestamp), addQuote(ictNumber));
    }

    public static String getNetworkCheckRealtimeUpsertQuery(String ictNumber, long timestamp)
    {
        String ZES_lv_currentTimestamp = convertTimestampToMySQLTimestamp(timestamp);
        return String.format(
                "INSERT INTO %s (ict_number, collection_interval_sec, connection_count, modified_date) " +
                "VALUES (%s, 0, 1, %s) " +
                "ON DUPLICATE KEY UPDATE " +
                "collection_interval_sec = CASE " +
                "WHEN DATE(modified_date) = DATE(%s) THEN GREATEST(ROUND(TIMESTAMPDIFF(MICROSECOND, modified_date, %s) / 1000000, 2), 0) " +
                "ELSE 0 END, " +
                "connection_count = CASE " +
                "WHEN DATE(modified_date) = DATE(%s) THEN connection_count + 1 " +
                "ELSE 1 END, " +
                "modified_date = %s",
                ZES_gv_networkCheckRealtimeTable,
                addQuote(ictNumber),
                ZES_lv_currentTimestamp,
                ZES_lv_currentTimestamp,
                ZES_lv_currentTimestamp,
                ZES_lv_currentTimestamp,
                ZES_lv_currentTimestamp
        );
    }

    public static void insert(Connection conn, ZES_Data[] dataMap, String ictNumber, String tableName, long timestamp) throws SQLException
    {
        String insertQuery = getInsertQuery(dataMap, ictNumber, tableName, timestamp);
        executeQuery(conn, insertQuery);
    }

    public static void update(Connection conn, ZES_Data[] dataMap, String tableName, String ictNumber, long timestamp) throws SQLException
    {
        String updateQuery = getUpdateQuery(dataMap, ictNumber, tableName, timestamp);
        executeQuery(conn, updateQuery);
    }

    public static void executeQuery(Connection conn, String query) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            statement.executeUpdate(query);
        }
        // Connection은 호출하는 쪽(try-with-resources)에서 관리
    }

    public static void executeBatchQuery(Connection conn, List<String> queries) throws SQLException
    {
        try (Statement statement = conn.createStatement())
        {
            for (String query: queries)
            {
                statement.addBatch(query);
            }
            statement.executeBatch();
        }
        // Connection은 호출하는 쪽(try-with-resources)에서 관리
    }

    public static String formatInsertValues(ZES_Data[] dataMap, long timestamp)
    {
        List<String> values = Arrays.stream(dataMap).map(data -> addQuote(String.valueOf(data.ZES_gv_value))).collect(Collectors.toList());
        values.add(convertTimestampToMySQLTimestamp(timestamp));
        values.add(convertTimestampToMySQLTimestamp(timestamp));
        return String.join(", ", values);
    }

    public static String formatUpdateValues(ZES_Data[] dataMap, long timestamp)
    {
        List<String> values = Arrays.stream(dataMap).map(data -> data.ZES_gv_key + "=" + addQuote(data.ZES_gv_value)).collect(Collectors.toList());
        values.add("modified_date=" + convertTimestampToMySQLTimestamp(timestamp));
        return String.join(", ", values);
    }

    public static String convertTimestampToMySQLTimestamp(long timestamp)
    {
        return addQuote(convertTimestampToDateFormat(timestamp, "yyyy-MM-dd HH:mm:ss.SSS"));
    }

    public static String convertTimestampToDateFormat(long timestamp, String dateFormat) {
        // Convert Unix timestamp to LocalDateTime
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.ofHours(9));
        // Create a DateTimeFormatter for the desired format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat);
        // Format the LocalDateTime as a MySQL timestamp string
        return localDateTime.format(formatter);
    }
}
