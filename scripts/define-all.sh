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

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS extract_feature")
sqlContext.sql("CREATE TEMPORARY FUNCTION extract_feature AS 'hivemall.ftvec.amplify.ExtractFeatureUDFWrapper'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS extract_weight")
sqlContext.sql("CREATE TEMPORARY FUNCTION extract_weight AS 'hivemall.ftvec.amplify.ExtractWeightUDFWrapper'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS add_feature_index")
sqlContext.sql("CREATE TEMPORARY FUNCTION add_feature_index AS 'hivemall.ftvec.amplify.AddFeatureIndexUDFWrapper'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS amplify")
sqlContext.sql("CREATE TEMPORARY FUNCTION amplify AS 'hivemall.ftvec.amplify.AmplifierUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS rand_amplify")
sqlContext.sql("CREATE TEMPORARY FUNCTION rand_amplify AS 'hivemall.ftvec.amplify.RandomAmplifierUDTF'")

