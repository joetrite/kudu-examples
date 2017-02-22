package org.kududb.examples.sample;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;

import java.util.ArrayList;
import java.util.List;

public class Sample {

  private static final String KUDU_MASTER = System.getProperty(
		  "kuduMaster", "quickstart.cloudera");

  public static void main(String[] args) {
    System.out.println("-----------------------------------------------");
    System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
    System.out.println("Run with -DkuduMaster=myHost:port to override.");
    System.out.println("-----------------------------------------------");
    String tableName = "java_sample-" + System.currentTimeMillis();
    KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
    

    try {
    	
      //******************************	CREATE KUDU TABLE  ********************************
      System.out.println("Creating kudu schema for table " + tableName);

      List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
      columns.add(new ColumnSchema.ColumnSchemaBuilder("cust_num", Type.INT64)
          .key(true)
          .build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("cust_name", Type.STRING)
          .build());
      columns.add(new ColumnSchema.ColumnSchemaBuilder("cust_phone", Type.STRING)
              .build());
      List<String> rangeKeys = new ArrayList<String>();
      rangeKeys.add("cust_num");

      
      System.out.println("Submitting create table command for " + tableName);
      Schema schema = new Schema(columns);
      client.createTable(tableName, schema, new CreateTableOptions()
    		  .setRangePartitionColumns(rangeKeys)
    		  .setNumReplicas(1)); 

      System.out.println("Table creation complete for " + tableName);

      
      //******************************	INSERT DATA  ********************************
      System.out.println("Inserting row into " + tableName);

      KuduTable table = client.openTable(tableName);
      KuduSession session = client.newSession();
      for (long i = 0; i < 3; i++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addLong("cust_num", i); 
        row.addString("cust_name", "John Doe"); 
        row.addString("cust_phone", "555-555-5555");
        
        System.out.println("Inserting row for cust_num " + i);

        session.apply(insert);
        System.out.println("Insert complete");

      }
            
      //******************************	SELECT DATA ********************************
      System.out.println("Selecting data that was just inserted");

      List<String> selectColumns = new ArrayList<String>();
      selectColumns.add("cust_num");
      selectColumns.add("cust_phone");

      System.out.println("Defining the column we will use in the selection criteria");
      ColumnSchema cs = new ColumnSchemaBuilder("cust_num", Type.INT64).encoding(ColumnSchema.Encoding.AUTO_ENCODING).key(true).build();
      
      KuduScanner scanner = client.newScannerBuilder(table)
          .setProjectedColumnNames(selectColumns)
          .addPredicate(KuduPredicate.newComparisonPredicate(cs, ComparisonOp.EQUAL, 1)) //get the row where cust_num == 1
          .build();
      
      while (scanner.hasMoreRows()) {
        RowResultIterator results = scanner.nextRows();
        while (results.hasNext()) {
          RowResult result = results.next();
          System.out.println("cust_num=" + result.getLong(0) + ", cust_phone=" + result.getString(1));
        }
      }
      
      System.out.println("Select complete");
      
      //******************************	UPSERT DATA  ********************************
      System.out.println("Upserting rows in " + tableName);
      for (long i = 0; i < 3; i++) {
          Upsert upsert = table.newUpsert();
          PartialRow row = upsert.getRow();
          row.addLong("cust_num", i); 
          row.addString("cust_name", "Jane Doe"); 
          row.addString("cust_phone", "777-777-7777");
          
          System.out.println("Upserting row for cust_num " + i);

          session.apply(upsert);
          System.out.println("Upsert complete");

        }
              
      //******************************	SELECT DATA AGAIN  ********************************
        System.out.println("Selecting data that was just upserted.");
        
        selectColumns = new ArrayList<String>();
        selectColumns.add("cust_num");
        selectColumns.add("cust_phone");

        System.out.println("Defining the column we will use in the selection criteria");
        cs = new ColumnSchemaBuilder("cust_num", Type.INT64).encoding(ColumnSchema.Encoding.AUTO_ENCODING).key(true).build();
        
        scanner = client.newScannerBuilder(table)
            .setProjectedColumnNames(selectColumns)
            .addPredicate(KuduPredicate.newComparisonPredicate(cs, ComparisonOp.EQUAL, 1)) //get the row where cust_num == 1
            .build();
        while (scanner.hasMoreRows()) {
          RowResultIterator results = scanner.nextRows();
          while (results.hasNext()) {
            RowResult result = results.next();
            System.out.println("cust_num=" + result.getLong(0) + ", cust_phone=" + result.getString(1));
          }
        }
        
        System.out.println("Select complete");

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
      	//******************************	DELETE KUDU TABLE   ********************************
    	System.out.println("Deleting table " + tableName);
        client.deleteTable(tableName);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          client.shutdown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
}
