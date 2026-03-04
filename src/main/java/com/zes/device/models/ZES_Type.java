package com.zes.device.models;

import com.zes.device.ZES_SQLGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.ConcurrentHashMap;

import static com.zes.device.ZES_DeviceApplication.*;

public abstract class ZES_Type
{
    protected final long ZES_gv_timestamp;
    protected final byte[] ZES_gv_bytes;
    public String ZES_gv_ictNumber;
    protected Boolean ZES_gv_hasAnyNewValue = null;
    protected boolean ZES_gv_hasPrevData = false;
    private static final ConcurrentHashMap<String, ZES_NetworkCheckRealtimeCache> ZES_gv_networkCheckRealtimeCache = new ConcurrentHashMap<>();

    private static class ZES_NetworkCheckRealtimeCache
    {
        private final long modifiedTimestamp;
        private final int connectionCount;

        private ZES_NetworkCheckRealtimeCache(long modifiedTimestamp, int connectionCount)
        {
            this.modifiedTimestamp = modifiedTimestamp;
            this.connectionCount = connectionCount;
        }
    }
    protected abstract void ZES_parseData(ZES_Data data, ResultSet resultSet) throws SQLException;

    abstract public ZES_Type ZES_saveRealTime() throws SQLException;
    abstract public void ZES_saveLog();

    public ZES_Type(long timestamp, byte[] bytes, String ictNumber) {
        this.ZES_gv_timestamp = timestamp;
        this.ZES_gv_bytes = bytes.clone(); // Create a defensive copy of the byte array
        this.ZES_gv_ictNumber = ictNumber;
    }

    static long ZES_getLong(byte[] bytes, int offset, int size) {
        return ZES_convertByteArrayToLong(bytes, offset, size);
    }

    static double ZES_getDouble(byte[] bytes, int offset, int delimitSize) {
        int ZES_lv_intSize = 4 - delimitSize;
        long ZES_lv_intValue = ZES_convertByteArrayToLong(bytes, offset, ZES_lv_intSize);
        if(delimitSize == 0) {
            return (double) ZES_lv_intValue / 100;
        } else {
            long ZES_lv_fractionValue = ZES_convertByteArrayToLong(bytes, offset + ZES_lv_intSize, delimitSize);
            return Double.parseDouble(ZES_lv_intValue + "." + ZES_lv_fractionValue);
        }
    }

    static String ZES_getTime(byte[] bytes, int offset) {
        long ZES_lv_hour = ZES_convertByteArrayToLong(bytes, offset, 2);
        long ZES_lv_min = ZES_convertByteArrayToLong(bytes, offset + 2, 2);
        long ZES_lv_sec = ZES_convertByteArrayToLong(bytes, offset + 4, 2);
        String[] ZES_lv_time = {ZES_addLeadingZero(ZES_lv_hour), ZES_addLeadingZero(ZES_lv_min), ZES_addLeadingZero(ZES_lv_sec)};
//        long ZES_lv_milliSec = ZES_convertByteArrayToLong(ZES_gv_bytes, offset + 2, 2);
        return String.join(":", ZES_lv_time);
    }

    static String ZES_addLeadingZero(long number) {
        return String.format("%02d", number);
    }

    protected void ZES_handleParseException(Exception e)
    {
        ZES_handleException(e);
        Thread.yield();
    }

    protected void ZES_handleException(Exception e)
    {
        ZES_gv_logger.severe("This error occurs from " + ZES_gv_ictNumber);
        e.printStackTrace();
    }


    protected void ZES_updateNetworkCheckRealtime(Connection conn) throws SQLException
    {
        long ZES_lv_nowTimestamp = System.currentTimeMillis();
        String ZES_lv_ictNumberLock = ("NETWORK_CHECK_" + ZES_gv_ictNumber).intern();

        synchronized (ZES_lv_ictNumberLock)
        {
            ZES_NetworkCheckRealtimeCache ZES_lv_cached = ZES_gv_networkCheckRealtimeCache.get(ZES_gv_ictNumber);
            if (ZES_lv_cached == null)
            {
                ZES_lv_cached = ZES_loadNetworkCheckRealtimeCache(conn, ZES_gv_ictNumber, ZES_lv_nowTimestamp);
            }

            String ZES_lv_nowDate = ZES_SQLGenerator.convertTimestampToDateFormat(ZES_lv_nowTimestamp, "yyyy-MM-dd");
            String ZES_lv_prevDate = ZES_SQLGenerator.convertTimestampToDateFormat(ZES_lv_cached.modifiedTimestamp, "yyyy-MM-dd");

            double ZES_lv_collectionIntervalSec;
            int ZES_lv_connectionCount;

            if (ZES_lv_nowDate.equals(ZES_lv_prevDate))
            {
                double ZES_lv_intervalInSec = (ZES_lv_nowTimestamp - ZES_lv_cached.modifiedTimestamp) / 1000.0;
                ZES_lv_collectionIntervalSec = Math.max(Math.round(ZES_lv_intervalInSec * 100.0) / 100.0, 0.0);
                ZES_lv_connectionCount = ZES_lv_cached.connectionCount + 1;
            }
            else
            {
                ZES_lv_collectionIntervalSec = 0.0;
                ZES_lv_connectionCount = 1;
            }

            String ZES_lv_networkCheckRealtimeUpsertQuery = ZES_SQLGenerator.getNetworkCheckRealtimeUpsertQuery(
                    ZES_gv_ictNumber,
                    ZES_lv_collectionIntervalSec,
                    ZES_lv_connectionCount,
                    ZES_lv_nowTimestamp
            );
            ZES_SQLGenerator.executeQuery(conn, ZES_lv_networkCheckRealtimeUpsertQuery);

            ZES_gv_networkCheckRealtimeCache.put(
                    ZES_gv_ictNumber,
                    new ZES_NetworkCheckRealtimeCache(ZES_lv_nowTimestamp, ZES_lv_connectionCount)
            );
        }
    }

    private ZES_NetworkCheckRealtimeCache ZES_loadNetworkCheckRealtimeCache(Connection conn, String ictNumber, long nowTimestamp) throws SQLException
    {
        try
        (
            PreparedStatement ZES_lv_stmt = ZES_SQLGenerator.findNetworkCheckRealtimeByIctNumber(conn, ictNumber);
            ResultSet ZES_lv_resultSet = ZES_lv_stmt.executeQuery()
        )
        {
            if (ZES_lv_resultSet.next())
            {
                Timestamp ZES_lv_modifiedDate = ZES_lv_resultSet.getTimestamp("modified_date");
                long ZES_lv_modifiedTimestamp = ZES_lv_modifiedDate == null ? nowTimestamp : ZES_lv_modifiedDate.getTime();
                int ZES_lv_connectionCount = ZES_lv_resultSet.getInt("connection_count");
                return new ZES_NetworkCheckRealtimeCache(ZES_lv_modifiedTimestamp, ZES_lv_connectionCount);
            }
        }

        return new ZES_NetworkCheckRealtimeCache(nowTimestamp, 0);
    }

    protected void ZES_parse(ZES_Data[] dataMap, ResultSet resultSet) {
        try {
            ZES_gv_hasPrevData = resultSet != null && resultSet.next();
            for (ZES_Data data : dataMap)
            {
                ZES_parseData(data, resultSet);
                if (ZES_gv_hasAnyNewValue == null || !ZES_gv_hasAnyNewValue)
                {
                    ZES_gv_hasAnyNewValue = data.isNewValue();
                }
            }
        }
        catch (Exception e)
        {
            ZES_handleParseException(e);
        }
    }
}
