package com.dodoca.miniprogram.util;

public class Constant {
    public static final String JDBC_DRIVER = "com.cloudera.impala.jdbc4.Driver";
    public static final String CONNECTION_URL = "jdbc:impala://47.94.228.38:21050/small_program;auth=noSasl;"
            + "jdbc:impala://47.94.218.244:21050/small_program;auth=noSasl;jdbc:impala://47.94.225.163:21050/small_program;auth=noSasl";
//    public static final String KUDU_MASTER = System.getProperty("kuduMaster","data02.diandianke.sa");
//
//    public static final String KUDU_SERVER_HOST = "data02.diandianke.sa:7051";
//
//    public static String ETL_BATCH_DATABASE = "magic_cube";
//
//    public static void setEtlBatchDatabase(String etlBatchbase){
//        if(null != etlBatchbase && !"".equals(etlBatchbase)){
//            ETL_BATCH_DATABASE = etlBatchbase;
//        }
//    }
}
