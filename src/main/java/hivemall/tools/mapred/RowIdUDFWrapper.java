/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hivemall.tools.mapred;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.UUID;

/**
 * A wrapper of [[hivemall.tools.mapred.RowIdUDF]].
 *
 * TODO: This is needed because Spark throws an exception below
 * when it calls UDF with no argument, and why?
 *
 * org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 8.0 failed 1 times,
 *     most recent failure: Lost task 0.0 in stage 8.0 (TID 18, localhost):
 *     org.apache.hadoop.hive.ql.metadata.HiveException: Unable to execute method
 *     public org.apache.hadoop.io.Text hivemall.tools.mapred.RowIdUDF.evaluate() on
 *     object hivemall.tools.mapred.RowIdUDF@77549ef6 of class hivemall.tools.mapred.RowIdUDF
 *     with arguments {} of size 0
 *   at org.apache.hadoop.hive.ql.exec.FunctionRegistry.invoke(FunctionRegistry.java:1243)
 *   at org.apache.spark.sql.hive.HiveSimpleUdf.eval(hiveUdfs.scala:118)
 */
@UDFType(deterministic = false, stateful = true)
public class RowIdUDFWrapper extends GenericUDF {
    /**
     * TODO: This class does not work because spark cannot
     * handle HadoopUtils#getTaskId().
     */
    // private RowIdUDF udf = new RowIdUDF();

    private long sequence;
    private long taskId;

    public RowIdUDFWrapper() {
        this.sequence = 0L;
        this.taskId = Thread.currentThread().getId();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 0) {
            throw new UDFArgumentLengthException("row_number() has no argument.");
        }

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert(arguments.length == 0);
        sequence++;
        /**
         * TODO: Check if it is unique over all tasks
         * in executors of Spark.
         */
        return taskId + "-" + UUID.randomUUID() + "-" + sequence;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "row_number()";
    }
}
