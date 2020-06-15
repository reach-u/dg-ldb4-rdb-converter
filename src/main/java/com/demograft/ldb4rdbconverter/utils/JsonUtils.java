package com.demograft.ldb4rdbconverter.utils;

import lombok.experimental.UtilityClass;
import org.apache.avro.Schema;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@UtilityClass
public class JsonUtils {
    public JSONArray getNullableFloatType() {
        return getNullableType("float");
    }

    public JSONArray getNullableLongType() {
        return getNullableType("long");
    }

    public JSONArray getNullableDoubleType() {
        return getNullableType("double");
    }

    public JSONArray getNullableStringType() {
        return getNullableType("string");
    }

    public JSONObject getDefaultNullableObject(String name, Schema.Type type) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", name);
        switch (type) {
            case DOUBLE:
                jsonObject.put("type", getNullableDoubleType());
                break;
            case FLOAT:
                jsonObject.put("type", getNullableFloatType());
                break;
            case STRING:
                jsonObject.put("type", getNullableStringType());
                break;
            default:
                break;
        }
        jsonObject.put("default", null);
        return jsonObject;
    }

    private JSONArray getNullableType(String type) {
        JSONArray typeList = new JSONArray();
        typeList.add("null");
        typeList.add(type);
        return typeList;
    }
}
