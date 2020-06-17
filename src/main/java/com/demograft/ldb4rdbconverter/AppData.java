package com.demograft.ldb4rdbconverter;

// Data class for holding information gathered from the GUI and used to construct the configuration file.

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class AppData {
    private static File inputFile = null;
    private static File previousConfig = null;
    private static File previousData = null;
    private static File configFile = null;
    private static String defaultType = "string";
    private static String uncertainty = "false";
    private static String radiusField = "";
    private static String cellId = "";
    private static String configName = "";
    private static String longitude = "";
    private static String latitude = "";
    private static String time = "";
    private static String timeExample = "";
    private static List<String> removed = new ArrayList<>();
    private static List<ArrayList<String>> examples;
    private static HashMap<String, String> baseTypes = new HashMap<>();
    private static List<DataRow> GUIexamples;
    private static HashMap<String, String> typeMap = new HashMap<>();
    private static List<String> headerList;
    private static List<String> stringNulls = new ArrayList<>();
    private static List<Double> doubleNulls = new ArrayList<>();
    private static List<Long> longNulls = new ArrayList<>();
    private static List<Float> floatNulls = new ArrayList<>();
    private static List<String> floatColumns = new ArrayList<>();
    private static List<String> doubleColumns = new ArrayList<>();
    private static List<String> stringColumns = new ArrayList<>();
    private static List<String> timeRows = new ArrayList<>();
    private static List<String> hashedRows = new ArrayList<>();
    private static int parquetSize = 0;
    private static int stringLimit = 0;

    public static String getDefaultType() {
        return defaultType;
    }

    public static void setDefaultType(String defaultType) {
        AppData.defaultType = defaultType;
    }

    public static File getPreviousData() {
        return previousData;
    }

    public static void setPreviousData(File previousData) {
        AppData.previousData = previousData;
    }

    public static int getParquetSize() {
        return parquetSize;
    }

    public static String getConfigName() {
        return configName;
    }

    public static void setConfigName(String configName) {
        AppData.configName = configName;
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

    public static String getUncertainty() {
        return uncertainty;
    }

    public static void setUncertainty(String uncertainty) {
        AppData.uncertainty = uncertainty;
    }

    public static String getRadiusField() {
        return radiusField;
    }

    public static void setRadiusField(String radiusField) {
        AppData.radiusField = radiusField;
    }

    public static String getCellId() {
        return cellId;
    }

    public static void setCellId(String cellId) {
        AppData.cellId = cellId;
    }

    public static void addRemoved(String header){
        removed.add(header);
    }
    public static void addFloatColumn(String header){
        floatColumns.add(header);
    }
    public static void addDoubleColumn(String header){
        doubleColumns.add(header);
    }
    public static void addStringColumn(String header){
        stringColumns.add(header);
    }

    public static List<String> getFloatColumns() {
        return floatColumns;
    }

    public static void setFloatColumns(List<String> floatColumns) {
        AppData.floatColumns = floatColumns;
    }

    public static List<String> getDoubleColumns() {
        return doubleColumns;
    }

    public static void setDoubleColumns(List<String> doubleColumns) {
        AppData.doubleColumns = doubleColumns;
    }

    public static List<String> getStringColumns() {
        return stringColumns;
    }

    public static void setStringColumns(List<String> stringColumns) {
        AppData.stringColumns = stringColumns;
    }

    public static List<String> getRemoved() {
        return removed;
    }

    public static void setRemoved(List<String> removed) {
        AppData.removed = removed;
    }

    public static File getPreviousConfig() {
        return previousConfig;
    }

    public static void setPreviousConfig(File previousConfig) {
        AppData.previousConfig = previousConfig;
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
