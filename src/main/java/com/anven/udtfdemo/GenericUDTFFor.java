package com.anven.udtfdemo;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;

public class GenericUDTFFor extends GenericUDTF {
    IntWritable start;
    IntWritable end;
    IntWritable inc; // 递增量

    Object[] forwardObj = null; // 存放要返回的结果行

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        start = ((WritableConstantIntObjectInspector) argOIs[0]).getWritableConstantValue();
        end = ((WritableConstantIntObjectInspector) argOIs[1]).getWritableConstantValue();
        if(argOIs.length == 3) {
            inc = ((WritableConstantIntObjectInspector) argOIs[2]).getWritableConstantValue();
        } else {
            inc = new IntWritable(1); // 默认递增量为1
        }

        this.forwardObj = new Object[1];
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("col0");
        fieldOIs.add(
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.INT
                )
        );

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        for(int i = start.get(); i < end.get(); i = i + inc.get()) {
            this.forwardObj[0] = new Integer(i);
            forward(forwardObj);
        }
    }

    @Override
    public void close() throws HiveException {
        if(forwardObj != null) {
            forwardObj = null;
        }
    }
}
