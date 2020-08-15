package com.anven.udtfdemo;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class UDTFBook extends GenericUDTF {
    private PrimitiveObjectInspector targetColumnOI = null;
    Object[] forwardObj = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<String>(); // 字段名称
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("isbn");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.INT));

        fieldNames.add("title");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING));

        fieldNames.add("authors");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING
        ));

        /**
         * 输入列必须在initialize初始化，否则报错：
         * org.apache.hadoop.hive.serde2.lazy.LazyString cannot be cast to
         * org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
         */
        targetColumnOI = (PrimitiveObjectInspector) args[0];

        if (targetColumnOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("The target column isn't string.");
        }

        forwardObj = new Object[3];
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String targetColumn = (String) targetColumnOI.getPrimitiveJavaObject(args[0]); // 输入列的值转化为String
        if (StringUtils.isBlank(targetColumn)) {
            return;
        }

        String parts = new String(targetColumn.getBytes());
        String [] part = parts.split("\\|");
        forwardObj[0] = Integer.parseInt(part[0]);
        forwardObj[1] = part[1];
        forwardObj[2] = part[2];
        this.forward(forwardObj);
    }

    @Override
    public void close() throws HiveException {

    }
}
