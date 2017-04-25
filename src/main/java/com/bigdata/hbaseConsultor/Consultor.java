/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.hbaseConsultor;

import com.bigdata.conexion.ConexionFactory;
import com.bigdata.hbase.HBase;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.impl.GenericObjectPool;
/**
 *
 * @author jafernandez
 */
public class Consultor {
    private String colFamily = "";
    private String colQualifier = "";
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {  
        String table = "syslog";
        HBase hb = new HBase(
                new GenericObjectPool<>(new ConexionFactory()), 
                "syslog");
        try {
            hb.getFilterRows("msg","severity");
        } catch (Exception ex) {
            Logger.getLogger(Consultor.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
}