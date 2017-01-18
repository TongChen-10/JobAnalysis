package com.chentong;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by tongchen on 2016-08-14.
 */
public class SparkSQLUserLogsOps {
    public static void main(String[] args) {
        //SparkConf conf = new SparkConf().setMaster("spark://192.168.0.201:7077").setAppName("SparkSQLCity");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLCity");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.read().json("/Users/tongchen/Desktop/data.json");
        dataFrame.registerTempTable("positionInfo");
        String top10LanguageSQL = "SELECT keyword ,count(1) count FROM positionInfo GROUP BY keyword ORDER BY count DESC LIMIT 10";
        DataFrame top10LanguageResult = sqlContext.sql(top10LanguageSQL);
        //top10LanguageResult.show();
        top10LanguageResult.write().format("json").save("/Users/tongchen/Desktop/top10");
        /*
            Java
         */
//        dataFrame.filter(dataFrame.col("keyword").equalTo(top10LanguageResult.first().getString(0))).registerTempTable("cityofLanguage1");
//        String cityofLanguage1SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage1" +
//                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
//        DataFrame cityofLanguage1SQLResult = sqlContext.sql(cityofLanguage1SQL);
//        //cityofLanguage1SQLResult.show();
//        cityofLanguage1SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10");
//        dataFrame.filter(dataFrame.col("keyword").equalTo("Java")).registerTempTable("cityofLanguage1");
        String cityofLanguage1SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage1" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage1SQLResult = sqlContext.sql(cityofLanguage1SQL);
        cityofLanguage1SQLResult.show();
        cityofLanguage1SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/Java");
         /*
            C
         */
//        dataFrame.filter(dataFrame.col("keyword").equalTo(top10LanguageResult.head(2)[1].getString(0))).registerTempTable("cityofLanguage2");
//        String cityofLanguage2SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage2" +
//                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
//        DataFrame cityofLanguage2SQLResult = sqlContext.sql(cityofLanguage2SQL);
//        cityofLanguage2SQLResult.show();
        dataFrame.filter(dataFrame.col("keyword").equalTo("C")).registerTempTable("cityofLanguage2");
        String cityofLanguage2SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage2" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage2SQLResult = sqlContext.sql(cityofLanguage2SQL);
        cityofLanguage2SQLResult.show();
        cityofLanguage2SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/C");
        /*
            Num 3
         */
        dataFrame.filter(dataFrame.col("keyword").equalTo("C++")).registerTempTable("cityofLanguage3");
        String cityofLanguage3SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage3" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage3SQLResult = sqlContext.sql(cityofLanguage3SQL);
        cityofLanguage3SQLResult.show();
        cityofLanguage3SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/C++");
        /*
            Num4
         */
        dataFrame.filter(dataFrame.col("keyword").equalTo("C#")).registerTempTable("cityofLanguage4");
        String cityofLanguage4SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage4" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage4SQLResult = sqlContext.sql(cityofLanguage4SQL);
        cityofLanguage4SQLResult.show();
        cityofLanguage4SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/C#");
        /*
            Num5
         */
        dataFrame.filter(dataFrame.col("keyword").equalTo("PHP")).registerTempTable("cityofLanguage5");;
        String cityofLanguage5SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage5" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage5SQLResult = sqlContext.sql(cityofLanguage5SQL);
        cityofLanguage5SQLResult.show();
        cityofLanguage5SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/PHP");
        /*
            Num6
         */
        dataFrame.filter(dataFrame.col("keyword").equalTo("Python")).registerTempTable("cityofLanguage6");;
        String cityofLanguage6SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage6" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage6SQLResult = sqlContext.sql(cityofLanguage6SQL);
        cityofLanguage6SQLResult.show();
        cityofLanguage6SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/Python");
        /*
            Num7
         */
        dataFrame.filter(dataFrame.col("keyword").equalTo("Ruby")).registerTempTable("cityofLanguage7");;
        String cityofLanguage7SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage7" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage7SQLResult = sqlContext.sql(cityofLanguage7SQL);
        cityofLanguage7SQLResult.show();
        cityofLanguage7SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/Ruby");
        /*
            Num8
         */
        dataFrame.filter(dataFrame.col("keyword").equalTo("R")).registerTempTable("cityofLanguage8");;
        String cityofLanguage8SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage8" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage8SQLResult = sqlContext.sql(cityofLanguage8SQL);
        cityofLanguage8SQLResult.show();
        cityofLanguage8SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/R");
        /*
            Num9
         */
        dataFrame.filter(dataFrame.col("keyword").equalTo("JavaScript")).registerTempTable("cityofLanguage9");;
        String cityofLanguage9SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage9" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage9SQLResult = sqlContext.sql(cityofLanguage9SQL);
        cityofLanguage9SQLResult.show();
        cityofLanguage9SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/JavaScript");
        /*
            Num10
         */
        dataFrame.filter(dataFrame.col("keyword").equalTo("Visual Basic")).registerTempTable("cityofLanguage10");
        String cityofLanguage10SQL = "SELECT keyword,workPlace,count(1) count FROM cityofLanguage10" +
                " GROUP BY keyword,workPlace ORDER BY count DESC limit 5";
        DataFrame cityofLanguage10SQLResult = sqlContext.sql(cityofLanguage10SQL);
        cityofLanguage10SQLResult.show();
        cityofLanguage10SQLResult.write().format("json").save("/Users/tongchen/Desktop/top10/Visual Basic");
//        //cityofLanguage1SQLResult.write().format("json").save("/Users/tongchen/Desktop/");
    }
}
