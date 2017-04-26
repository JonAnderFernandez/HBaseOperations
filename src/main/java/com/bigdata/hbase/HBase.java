/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.hbase;

import java.util.Date;
import java.util.Map;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.commons.pool2.ObjectPool;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *
 * @author jafernandez
 */
public class HBase{        
    private ObjectPool<Connection> pool;      
    private String table;
    
    public HBase(ObjectPool<Connection> pool, String table){
        this.pool = pool;
        this.table = table;
    }   
    // Insertar un registro    
    public void writeIntoTable(String colFamily, String colQualifier, String value) throws Exception{
        Connection c = pool.borrowObject();
        TableName tableName = TableName.valueOf(this.table); 
        Table t = c.getTable(tableName);
        TimeStamp ts = new TimeStamp(new Date());        
        Date d = ts.getDate();        
        Put p = new Put(Bytes.toBytes(d.toString()));
        p.addColumn(
            Bytes.toBytes(colFamily),
            Bytes.toBytes(colQualifier),
            Bytes.toBytes(value));
        t.put(p);
        t.close();
        pool.returnObject(c);   
    }     
    ////////////////////////////////////////////////////////////////////////////
    // Leer descripcion de la tabla
    public void getDescription() throws Exception{
        Connection c = pool.borrowObject();
        Admin admin = c.getAdmin(); 
        HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(this.table));
        System.out.println(Bytes.toString(htd.toByteArray()));
        pool.returnObject(c);    
    }    
    // Leer los primeros i registros de la tabla
    public void getLimitRows(int i) throws Exception{
        Connection c = pool.borrowObject();              
        Table t = c.getTable(TableName.valueOf(this.table));
        Scan s = new Scan();
        ResultScanner scanner = t.getScanner(s);
        int j = 0;
        for (Result result = scanner.next(); result != null && j < i; result = scanner.next()){            
            Map<byte[],byte[]> qualifiers = result.getFamilyMap(Bytes.toBytes("msg"));
            for(int k = 0; k < qualifiers.size(); k++){
                Object[] values = qualifiers.values().toArray();
                Object[] keys = qualifiers.keySet().toArray();                
                String key = Bytes.toString((byte[]) keys[k]);
                String value = Bytes.toString((byte[]) values[k]);
                System.out.println("[msg:" + key + "] =  " + value);            
            }
            System.out.println("------------------------------------------------------------------------");            
            j++;
        }        
        scanner.close();
        t.close();
        pool.returnObject(c);   
    } 
    // Leer un valor de las filas de la tabla
    public void getValue(String colF, String colQ) throws Exception{
        Connection c = pool.borrowObject();             
        Table t = c.getTable(TableName.valueOf(this.table));
        Scan s = new Scan();
        ResultScanner scanner = t.getScanner(s);         
        for (Result result = scanner.next(); result != null; result = scanner.next()){  
            String value = Bytes.toString(result.getValue(Bytes.toBytes(colF), Bytes.toBytes(colQ)));
            System.out.println("[" + colF + ":" + colQ + "] value= " + value);                                           
        }        
        scanner.close();
        t.close();
        pool.returnObject(c);   
    } 
    // Leer filtrando los registros de la tabla
    public void getFilterRows() throws Exception{
        Connection c = pool.borrowObject();                 
        Table t = c.getTable(TableName.valueOf(this.table));
        Scan scan = new Scan();        
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);        
        Filter f1 = new ValueFilter(
                CompareOp.EQUAL, 
                new BinaryComparator(Bytes.toBytes("4")));
        list.addFilter(f1);       
        Filter f2 = new ValueFilter(
                CompareOp.EQUAL, 
                new BinaryComparator(Bytes.toBytes("5")));
        list.addFilter(f2);      
        scan.setFilter(list);        
        ResultScanner scanner = t.getScanner(scan);        
        for (Result result = scanner.next(); result != null; result = scanner.next()){   
            Map<byte[],byte[]> qualifiers = result.getFamilyMap(Bytes.toBytes("msg"));
            for(int k = 0; k < qualifiers.size(); k++){
                Object[] values = qualifiers.values().toArray();
                Object[] keys = qualifiers.keySet().toArray();                
                String key = Bytes.toString((byte[]) keys[k]);
                String value = Bytes.toString((byte[]) values[k]);
                System.out.println("KEY = " + Bytes.toString(result.getRow()) + " [msg:" + key + "] =  " + value);            
            }
            System.out.println("------------------------------------------------------------------------");          
        }        
        scanner.close();
        t.close();
        pool.returnObject(c);   
    }  
    // Actualizar row
    public void updateRow(String row, String colFamily, String colQualifier, String value) throws Exception{
        Connection c = pool.borrowObject();
        Table t = c.getTable(TableName.valueOf(this.table));
        Put p = new Put(Bytes.toBytes(row));
        p.addColumn(
            Bytes.toBytes(colFamily),
            Bytes.toBytes(colQualifier),
            Bytes.toBytes(value));
        t.put(p);
        t.close();
        pool.returnObject(c);   
    }
}