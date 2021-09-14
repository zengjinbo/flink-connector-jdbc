package org.apache.flink.connector.jdbc.split;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * @author 曾进波
 * @ClassName: TableKeySelector
 * @Description: TODO(一句话描述这个类)
 * @date
 * @Copyright ? 北京滴普科技有限公司
 */
public class TableKeySelector implements KeySelector<Tuple2<Boolean, Row>, Object> {
private final String[] keys;

    public TableKeySelector(String[] key) {
        this.keys=key;
    }

    @Override
    public Object getKey(Tuple2<Boolean, Row> value)  {

    	if (value.f0)
		{
			StringBuffer buffer=new StringBuffer();
			for (int i =0 ;i<keys.length;i++
				 ) {
				buffer.append(value.getField(i).toString());
			}
			return buffer.toString();
		}
    	return null;
    }
}
