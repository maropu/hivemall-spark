/**
 * A bootstrap script to register Hivemall UDFs on a spark context
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS hivemall_version")
sqlContext.sql("CREATE TEMPORARY FUNCTION hivemall_version AS 'hivemall.HivemallVersionUDF'")

/**
 * Binary classification
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_perceptron")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_perceptron AS 'hivemall.classifier.PerceptronUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_pa")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_pa AS 'hivemall.classifier.PassiveAggressiveUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_pa1")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_pa1 AS 'hivemall.classifier.PassiveAggressiveUDTF$PA1'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_pa2")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_pa2 AS 'hivemall.classifier.PassiveAggressiveUDTF$PA2'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_cw")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_cw AS 'hivemall.classifier.ConfidenceWeightedUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_arow")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_arow AS 'hivemall.classifier.AROWClassifierUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_arowh")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_arowh AS 'hivemall.classifier.AROWClassifierUDTF$AROWh'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_adagrad_rda")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_arowh AS 'hivemall.classifier.AdaGradRDAUDTF'")

/**
 * Multiclass classification
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_multiclass_perceptron")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_multiclass_perceptron AS 'hivemall.classifier.multiclass.MulticlassPerceptronUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_multiclass_pa")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_multiclass_pa AS 'hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_multiclass_pa1")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_multiclass_pa1 AS 'hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA1'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_multiclass_pa2")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_multiclass_pa2 AS 'hivemall.classifier.multiclass.MulticlassPassiveAggressiveUDTF$PA2'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_multiclass_cw")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_multiclass_cw AS 'hivemall.classifier.multiclass.MulticlassConfidenceWeightedUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_multiclass_scw")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_multiclass_scw AS 'hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW1'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_multiclass_scw2")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_multiclass_scw2 AS 'hivemall.classifier.multiclass.MulticlassSoftConfidenceWeightedUDTF$SCW2'")

/**
 * Regression functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_logregr")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_logregr AS 'hivemall.regression.LogressUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_pa1_regr")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_pa1_regr AS 'hivemall.regression.PassiveAggressiveRegressionUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_pa1a_regr")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_pa1a_regr AS 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA1a'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_pa2_regr")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_pa2_regr AS 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA2'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_pa2a_regr")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_pa2a_regr AS 'hivemall.regression.PassiveAggressiveRegressionUDTF$PA2a'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_adadelta")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_adadelta AS 'hivemall.regression.AdaDeltaUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_adagrad")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_adagrad AS 'hivemall.regression.AdaGradUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_arow_regr")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_arow_regr AS 'hivemall.regression.AROWRegressionUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_arowe_regr")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_arow_regr AS 'hivemall.regression.AROWRegressionUDTF$AROWe'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_arowe2_regr")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_arow_regr AS 'hivemall.regression.AROWRegressionUDTF$AROWe2'")

/**
 * Distance functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS cosine_sim")
sqlContext.sql("CREATE TEMPORARY FUNCTION cosine_sim AS 'hivemall.knn.distance.CosineSimilarityUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS homming_distance")
sqlContext.sql("CREATE TEMPORARY FUNCTION hamming_distance AS 'hivemall.knn.distance.HammingDistanceUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS jaccard")
sqlContext.sql("CREATE TEMPORARY FUNCTION jaccard AS 'hivemall.knn.distance.JaccardIndexUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS popcnt")
sqlContext.sql("CREATE TEMPORARY FUNCTION popcnt AS 'hivemall.knn.distance.PopcountUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS kld")
sqlContext.sql("CREATE TEMPORARY FUNCTION kld AS 'hivemall.knn.distance.KLDivergenceUDF'")

/**
 * LSH functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS minhashes")
sqlContext.sql("CREATE TEMPORARY FUNCTION minhashes AS 'hivemall.knn.lsh.MinHashesUDFWrapper'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS minhash")
sqlContext.sql("CREATE TEMPORARY FUNCTION minhash AS 'hivemall.knn.lsh.MinHashUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS bbit_minhash")
sqlContext.sql("CREATE TEMPORARY FUNCTION bbit_minhash AS 'hivemall.knn.lsh.bBitMinHashUDF'")

/**
 * Voting functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS voted_avg")
sqlContext.sql("CREATE TEMPORARY FUNCTION voted_avg AS 'hivemall.ensemble.bagging.VotedAvgUDAF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS weight_voted_avg")
sqlContext.sql("CREATE TEMPORARY FUNCTION weight_voted_avg AS 'hivemall.ensemble.bagging.WeightVotedAvgUDAF'")

/**
 * mapred functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS rowid")
sqlContext.sql("CREATE TEMPORARY FUNCTION rowid AS 'hivemall.tools.mapred.RowIdUDFWrapper'")

/**
 * Dataset generator function
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS lr_datagen")
sqlContext.sql("CREATE TEMPORARY FUNCTION lr_datagen AS 'hivemall.dataset.LogisticRegressionDataGeneratorUDTFWrapper'")

/**
 * scaling functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS rescale")
sqlContext.sql("CREATE TEMPORARY FUNCTION rescale AS 'hivemall.ftvec.scaling.RescaleUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS zscore")
sqlContext.sql("CREATE TEMPORARY FUNCTION zscore AS 'hivemall.ftvec.scaling.ZScoreUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS normalize")
sqlContext.sql("CREATE TEMPORARY FUNCTION normalize AS 'hivemall.ftvec.scaling.L2NormalizationUDF'")

/**
 * hashing functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS mhash")
sqlContext.sql("CREATE TEMPORARY FUNCTION mhash AS 'hivemall.ftvec.hashing.MurmurHash3UDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS sha1")
sqlContext.sql("CREATE TEMPORARY FUNCTION sha1 AS 'hivemall.ftvec.hashing.Sha1UDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS array_hash_values")
sqlContext.sql("CREATE TEMPORARY FUNCTION array_hash_values AS 'hivemall.ftvec.hashing.ArrayHashValuesUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS prefixed_hash_values")
sqlContext.sql("CREATE TEMPORARY FUNCTION prefixed_hash_values AS 'hivemall.ftvec.hashing.ArrayPrefixedHashValuesUDF'")

/**
 * amplifier functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS amplify")
sqlContext.sql("CREATE TEMPORARY FUNCTION amplify AS 'hivemall.ftvec.amplify.AmplifierUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS rand_amplify")
sqlContext.sql("CREATE TEMPORARY FUNCTION rand_amplify AS 'hivemall.ftvec.amplify.RandomAmplifierUDTF'")

/**
 * ftvec/text functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS tf")
sqlContext.sql("CREATE TEMPORARY FUNCTION tf AS 'hivemall.ftvec.text.TermFrequencyUDAF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS tokenize")
sqlContext.sql("CREATE TEMPORARY FUNCTION tokenize AS 'hivemall.ftvec.text.TokenizeUDF'")

/**
 * Array functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS float_array")
sqlContext.sql("CREATE TEMPORARY FUNCTION float_array AS 'hivemall.tools.array.AllocFloatArrayUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS array_remove")
sqlContext.sql("CREATE TEMPORARY FUNCTION array_remove AS 'hivemall.tools.array.ArrayRemoveUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS sort_and_uniq_array")
sqlContext.sql("CREATE TEMPORARY FUNCTION sort_and_uniq_array AS 'hivemall.tools.array.SortAndUniqArrayUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS subarray_endwith")
sqlContext.sql("CREATE TEMPORARY FUNCTION subarray_endwith AS 'hivemall.tools.array.SubarrayEndWithUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS subarray_startwith")
sqlContext.sql("CREATE TEMPORARY FUNCTION subarray_startwith AS 'hivemall.tools.array.SubarrayStartWithUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS collect_all")
sqlContext.sql("CREATE TEMPORARY FUNCTION collect_all AS 'hivemall.tools.array.CollectAllUDAF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS concat_array")
sqlContext.sql("CREATE TEMPORARY FUNCTION concat_array AS 'hivemall.tools.array.ConcatArrayUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS subarray")
sqlContext.sql("CREATE TEMPORARY FUNCTION subarray AS 'hivemall.tools.array.SubarrayUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS array_avg")
sqlContext.sql("CREATE TEMPORARY FUNCTION array_avg AS 'hivemall.tools.array.ArrayAvgGenericUDAF'")

/**
 * Map functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS map_get_sum")
sqlContext.sql("CREATE TEMPORARY FUNCTION map_get_sum AS 'hivemall.tools.map.MapGetSumUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS map_tail_n")
sqlContext.sql("CREATE TEMPORARY FUNCTION map_tail_n AS 'hivemall.tools.map.MapTailNUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS to_map")
sqlContext.sql("CREATE TEMPORARY FUNCTION to_map AS 'hivemall.tools.map.UDAFToMap'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS to_ordered_map")
sqlContext.sql("CREATE TEMPORARY FUNCTION to_ordered_map AS 'hivemall.tools.map.UDAFToOrderedMap'")

/**
 * Math functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS sigmoid")
sqlContext.sql("CREATE TEMPORARY FUNCTION sigmoid AS 'hivemall.tools.math.SigmodUDF'")

/**
 * String functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS is_stopword")
sqlContext.sql("CREATE TEMPORARY FUNCTION is_stopword AS 'hivemall.tools.string.StopwordUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS split_words")
sqlContext.sql("CREATE TEMPORARY FUNCTION split_words AS 'hivemall.tools.string.SplitWordsUDF'")

/**
 * Evaluating functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS f1score")
sqlContext.sql("CREATE TEMPORARY FUNCTION f1score AS 'hivemall.evaluation.FMeasureUDAF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS mae")
sqlContext.sql("CREATE TEMPORARY FUNCTION mae AS 'hivemall.evaluation.MeanAbsoluteErrorUDAF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS mse")
sqlContext.sql("CREATE TEMPORARY FUNCTION mse AS 'hivemall.evaluation.MeanSquaredErrorUDAF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS rmse")
sqlContext.sql("CREATE TEMPORARY FUNCTION rmse AS 'hivemall.evaluation.RootMeanSquaredErrorUDAF'")

/**
 * Matrix Factorization functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS mf_predict")
sqlContext.sql("CREATE TEMPORARY FUNCTION mf_predict AS 'hivemall.mf.MFPredictionUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_mf_sgd")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_mf_sgd AS 'hivemall.mf.MatrixFactorizationSGDUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_mf_adagrad")
sqlContext.sql("CREATE TEMPORARY FUNCTION train_mf_adagrad AS 'hivemall.mf.MatrixFactorizationAdaGradUDTF'")

/**
 * Misc functions
 */

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS add_bias")
sqlContext.sql("CREATE TEMPORARY FUNCTION add_bias AS 'hivemall.ftvec.AddBiasUDFWrapper'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS sort_by_feature")
sqlContext.sql("CREATE TEMPORARY FUNCTION sort_by_feature AS 'hivemall.ftvec.SortByFeatureUDF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS extract_feature")
sqlContext.sql("CREATE TEMPORARY FUNCTION extract_feature AS 'hivemall.ftvec.ExtractFeatureUDFWrapper'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS extract_weight")
sqlContext.sql("CREATE TEMPORARY FUNCTION extract_weight AS 'hivemall.ftvec.ExtractWeightUDFWrapper'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS add_feature_index")
sqlContext.sql("CREATE TEMPORARY FUNCTION add_feature_index AS 'hivemall.ftvec.AddFeatureIndexUDFWrapper'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS conv2dense")
sqlContext.sql("CREATE TEMPORARY FUNCTION conv2dense AS 'hivemall.ftvec.ConvertToDenseModelUDAF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS generate_series")
sqlContext.sql("CREATE TEMPORARY FUNCTION generate_series AS 'hivemall.tools.GenerateSeriesUDTF'")

// sqlContext.sql("DROP TEMPORARY FUNCTION IF EXISTS convert_label")
sqlContext.sql("CREATE TEMPORARY FUNCTION convert_label AS 'hivemall.tools.ConvertLabelUDF'")

