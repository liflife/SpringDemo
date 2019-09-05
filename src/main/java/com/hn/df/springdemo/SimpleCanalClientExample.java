package com.lxf.spring.demo.demo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author: create by xingfeng.luo
 * @version: v1.0
 * @description: SimpleCanalClientExample
 * @date:2019/9/3
 **/
public class SimpleCanalClientExample {


    public static void main(String args[]) throws IOException {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.100.102"/*AddressUtils.getHostIp()*/,
                11111), "example", "canal", "canal");
        int batchSize = 1000;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    parseEntry(message.getEntries());
                }
                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
        } finally {
            connector.disconnect();
        }
    }

    private static void parseEntry(List<Entry> entrys) {

        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            // 可以获取到数据库实例名称、日志文件、当前操作的表以及执行的增删改查的操作
            String logFileName = entry.getHeader().getLogfileName();
            long logFileOffset = entry.getHeader().getLogfileOffset();
            String dbName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();
            System.out.println(String.format("=======&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    logFileName, logFileOffset,
                    dbName, tableName,
                    eventType));
            List<String> strings;
            for (RowData rowData : rowChage.getRowDatasList()) {
                CanalMysqlEntry canalMysqlEntry = new CanalMysqlEntry();
                canalMysqlEntry.setDataBaseName(dbName);
                canalMysqlEntry.setTableName(tableName);
                canalMysqlEntry.setType(eventType.name());
                Map<String, Object> dataBefore = new HashMap<>();
                Map<String, Object> dataAfter = new HashMap<>();
                if (eventType == EventType.DELETE) {
                    // 删除
                    dataAfter = printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    // 新增
                    dataAfter = printColumn(rowData.getAfterColumnsList());
                } else {
                    dataBefore = printColumn(rowData.getBeforeColumnsList());
                    dataAfter = printColumn(rowData.getAfterColumnsList());
                }
                canalMysqlEntry.setDataAfter(dataAfter);
                canalMysqlEntry.setDataBefore(dataBefore);
                System.out.println(canalMysqlEntry.toString());
                RabbitMqProducer.sendMsg(canalMysqlEntry.toString());
            }
        }
    }

    private static Map<String, Object> printColumn(List<Column> columns) {
        Map<String, Object> map = new HashMap<>();
        for (Column column : columns) {
            String str = column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated();
            map.put(column.getName(), column.getValue());
        }
        return map;
    }

}
