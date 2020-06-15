package com.demograft.ldb4rdbconverter.utils;

import org.apache.avro.Schema;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class JsonUtilsTest {

    @Test
    public void testGetNullableFloatType() {
        JSONArray result = JsonUtils.getNullableFloatType();

        Assert.assertEquals("[\"null\",\"float\"]", result.toJSONString());
    }

    @Test
    public void testGetNullableLongType() {
        JSONArray result = JsonUtils.getNullableLongType();

        Assert.assertEquals("[\"null\",\"long\"]", result.toJSONString());
    }

    @Test
    public void testGetNullableDoubleType() {
        JSONArray result = JsonUtils.getNullableDoubleType();

        Assert.assertEquals("[\"null\",\"double\"]", result.toJSONString());
    }

    @Test
    public void testGetNullableStringType() {
        JSONArray result = JsonUtils.getNullableStringType();

        Assert.assertEquals("[\"null\",\"string\"]", result.toJSONString());
    }

    @Test
    public void testGetDefaultNullableObjectForString() {
        JSONObject testName = JsonUtils.getDefaultNullableObject("testName", Schema.Type.STRING);

        Assert.assertEquals("{\"default\":null,\"name\":\"testName\",\"type\":[\"null\",\"string\"]}",
                testName.toJSONString());
    }

    @Test
    public void testGetDefaultNullableObjectForDouble() {
        JSONObject testName = JsonUtils.getDefaultNullableObject("testName", Schema.Type.DOUBLE);

        Assert.assertEquals("{\"default\":null,\"name\":\"testName\",\"type\":[\"null\",\"double\"]}",
                testName.toJSONString());
    }

    @Test
    public void testGetDefaultNullableObjectForFloat() {
        JSONObject testName = JsonUtils.getDefaultNullableObject("testName", Schema.Type.FLOAT);

        Assert.assertEquals("{\"default\":null,\"name\":\"testName\",\"type\":[\"null\",\"float\"]}",
                testName.toJSONString());
    }
}