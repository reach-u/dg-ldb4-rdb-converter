package com.demograft.ldb4rdbconverter;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class AppData {
    private static File inputFile = null;
    private static File configFile = null;
    private static String longitude = "";
    private static String latitude = "";
    private static String time = "";
    private static List<ArrayList<String>> examples;
    private static HashMap<String, String> baseTypes = new HashMap<>();
    private static List<DataRow> GUIexamples;
    private static HashMap<String, String> typeMap = new HashMap<>();
    private static List<String> headerList;
    private static List<String> stringNulls = new ArrayList<>();
    private static List<Double> doubleNulls = new ArrayList<>();
    private static List<Long> longNulls = new ArrayList<>();
    private static List<Float> floatNulls = new ArrayList<>();
    private static List<String> timeRows = new ArrayList<>();
    private static List<String> hashedRows = new ArrayList<>();
    private static int parquetSize = 0;
    private static int stringLimit = 0;

    public static int getParquetSize() {
        return parquetSize;
    }

    public static void setParquetSize(int parquetSize) {
        AppData.parquetSize = parquetSize;
    }

    public static int getStringLimit() {
        return stringLimit;
    }

    public static void setStringLimit(int stringLimit) {
        AppData.stringLimit = stringLimit;
    }

    public static List<String> getTimeRows() {
        return timeRows;
    }

    public static void setTimeRows(List<String> timeRows) {
        AppData.timeRows = timeRows;
    }

    public static List<String> getHashedRows() {
        return hashedRows;
    }

    public static void setHashedRows(List<String> hashedRows) {
        AppData.hashedRows = hashedRows;
    }

    public static String getTimeExample() {
        return timeExample;
    }

    public static void setTimeExample(String timeExample) {
        AppData.timeExample = timeExample;
    }

    private static String timeExample = "";


    public static List<String> getStringNulls() {
        return stringNulls;
    }

    public static void setStringNulls(List<String> stringNulls) {
        AppData.stringNulls = stringNulls;
    }

    public static List<Double> getDoubleNulls() {
        return doubleNulls;
    }

    public static void setDoubleNulls(List<Double> doubleNulls) {
        AppData.doubleNulls = doubleNulls;
    }

    public static List<Long> getLongNulls() {
        return longNulls;
    }

    public static void setLongNulls(List<Long> longNulls) {
        AppData.longNulls = longNulls;
    }

    public static List<Float> getFloatNulls() {
        return floatNulls;
    }

    public static void setFloatNulls(List<Float> floatNulls) {
        AppData.floatNulls = floatNulls;
    }

    public static HashMap<String, String> getBaseTypes() {
        return baseTypes;
    }

    public static void setBaseTypes(HashMap<String, String> baseTypes) {
        AppData.baseTypes = baseTypes;
    }

    static List<String> getHeaderList() {
        return headerList;
    }

    static void setHeaderList(List<String> headerList) {
        AppData.headerList = headerList;
    }

    static File getInputFile() {
        return inputFile;
    }

    static void setInputFile(File inputFile) {
        AppData.inputFile = inputFile;
    }

    static File getConfigFile() {
        return configFile;
    }

    static void setConfigFile(File configFile) {
        AppData.configFile = configFile;
    }

    public static String getLongitude() {
        return longitude;
    }

    public static void setLongitude(String longitude) {
        AppData.longitude = longitude;
    }

    public static String getLatitude() {
        return latitude;
    }

    public static void setLatitude(String latitude) {
        AppData.latitude = latitude;
    }

    public static String getTime() {
        return time;
    }

    public static void setTime(String time) {
        AppData.time = time;
    }

    public static List<ArrayList<String>> getExamples() {
        return examples;
    }

    public static void setExamples(List<ArrayList<String>> examples) {
        AppData.examples = examples;
    }

    public static List<DataRow> getGUIexamples() {
        return GUIexamples;
    }

    public static void setGUIexamples(List<DataRow> GUIexamples) {
        AppData.GUIexamples = GUIexamples;
    }

    public static HashMap<String,String> getTypeMap() {
        return typeMap;
    }

    public static void setTypeMap(HashMap<String, String> typeMap) {
        AppData.typeMap = typeMap;
    }

    public static boolean mainSet(){
        if(longitude.equals("") || latitude.equals("") || time.equals("")){
            return false;
        }
        else{
            return true;
        }
    }
}
