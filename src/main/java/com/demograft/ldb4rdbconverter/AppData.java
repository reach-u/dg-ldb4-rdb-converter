package com.demograft.ldb4rdbconverter;

import java.io.File;
import java.util.HashMap;
import java.util.List;

public class AppData {
    private static File inputFile = null;
    private static File configFile = null;
    private static String longitude = "";
    private static String latitude = "";
    private static String time = "";
    private static List<List<String>> examples;
    private static HashMap<String, String> baseTypes = new HashMap<>();
    private static List<DataRow> GUIexamples;
    private static HashMap<String, String> typeMap = new HashMap<>();
    private static List<String> headerList;

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

    public static List<List<String>> getExamples() {
        return examples;
    }

    public static void setExamples(List<List<String>> examples) {
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
