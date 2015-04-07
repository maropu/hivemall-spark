/**
 * A bootstrap script to register UDF on spark-shell
 */

val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

/**
 * Regression functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_logregr")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_logregr AS 'hivemall.regression.LogressUDTF'")


/**
 * Misc functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS add_bias")
sqlContext.sql("CREATE TEMPORARY FUNCTION add_bias AS 'hivemall.ftvec.AddBiasUDFWrapper'")

