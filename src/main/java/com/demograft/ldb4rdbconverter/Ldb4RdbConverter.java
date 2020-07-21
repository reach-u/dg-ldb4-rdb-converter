package com.demograft.ldb4rdbconverter;

import com.demograft.ldb4rdbconverter.geometry.Cell;
import com.demograft.ldb4rdbconverter.geometry.GeometryTransformer;
import com.demograft.ldb4rdbconverter.geometry.WKTGeometry;
import com.demograft.ldb4rdbconverter.parser.CsvInputParser;
import com.demograft.ldb4rdbconverter.parser.InputParser;
import com.demograft.ldb4rdbconverter.parser.InputRecord;
import com.demograft.ldb4rdbconverter.parser.ParquetInputParser;
import com.demograft.ldb4rdbconverter.utils.JsonUtils;
import com.demograft.ldb4rdbconverter.utils.TimeUtils;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.*;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Ldb4RdbConverter {

    private File inputFile;
    private String outputFile;

    @Option(name = "--config-file", required = true, usage = "Configuration file to configure generator")
    private String configFile = "";

    private String longitude = "";
    private String latitude = "";
    private String time = "";
    private String statsFile = "stats.txt";

    private int files = 0;

    private String[] columnsToRemove = new String[0];

    private List<String> columnsToRemoveList = new ArrayList<>();

    private DateTimeFormatter timeFormatter;

    private boolean inputEpochInMilliseconds = false;

    private StringBuilder statistics = new StringBuilder();

    private StringBuilder csvStatistics = new StringBuilder();

    private Map<String,String> databaseHeaderTypes = new HashMap<>();

    private List<String> wktGeometryHeaders = new ArrayList<>();

    private Map<String, WKTGeometry> wktMap = new HashMap<>();

    private final String[] propertyNames = new String[]{"input-file", "output-file", "stats-file", "latitude", "longitude",
            "time", "start-time", "end-time", "columns-to-map-long", "headers", "long-null-values", "double-null-values",
            "float-null-values", "long-columns", "float-columns", "double-columns", "string-columns", "time-columns", "list-columns",
            "parquet-size", "excluded", "unique-strings", "timezone","headers","retain-hashes","trajectoryID","tokenFiles",
            "is-coordinate-randomized-in-uncertainty", "radius", "cell-location-identifier", "cell-location-equality-tolerance",
            "default-type","non-negative","list-rows","database-files","wkt-geometry-files"};

    Set<String> derivedFields = Stream.of("geometryType", "geometryLatitude", "geometryLongitude", "orientationMajorAxis",
        "innerSemiMajorRadius", "innerSemiMinorRadius", "outerSemiMajorRadius", "outerSemiMinorRadius",
        "startAngle", "stopAngle").collect(Collectors.toSet());

    private List<String> listLists = new ArrayList<>();

    private final Set<String> propertySet = new HashSet<>(Arrays.asList(propertyNames));

    private Map<String, List<String>> arithmetics = new HashMap<>();

    private LocalDate beginDate = LocalDate.MAX;

    private LocalDate endDate = LocalDate.MIN;

    private String[] headerArray;

    private String trajectory = "";

    private String defaultType = "string";

    private List<String> examples = new LinkedList<>();

    private List<String> headers = new LinkedList<>();

    private List<String> hashColumns = new ArrayList<>();

    private List<Float> floatNullValues = new ArrayList<>();

    private List<Double> doubleNullValues = new ArrayList<>();

    private List<Long> longNullValues = new ArrayList<>();

    private List<String> stringNullValues = new ArrayList<>();

    private long[] hashMapCounters;

    private List<String> longColumns = new ArrayList<>();

    private List<String> floatColumns = new ArrayList<>();

    private List<String> doubleColumns = new ArrayList<>();

    private List<String> stringColumns = new ArrayList<>();

    private List<String> tokenFiles = new LinkedList<>();

    private List<String> databaseFiles = new LinkedList<>();

    private List<String> databaseHeaders = new LinkedList<>();

    private Map<String, DB> databases = new HashMap<>();

    private Map<String, String> databaseSources =  new HashMap<>();

    private Map<String, String> tokenHeaders = new HashMap<>();           //    They are formed as follows:   tokenHeader -> originalHeader

    private Map<String, Map<String, StringBuffer>> tokenMaps = new TreeMap<>();   //    They are formed as follows:  tokenHeader -> {originalRow -> tokenRow}

    private Map<String, Map<String, Long>> longMaps = new HashMap<>();

    private Map<String, String> tokenTypes = new HashMap<>();

    private List<String> timeColumns = new ArrayList<>();

    private List<String> retainHashes = new LinkedList<>();

    private List<String> nonNegativeRows = new ArrayList<>();

    private List<HashMap<String, Long>> hashTables = new ArrayList<>();

    private Map<String, Integer> databaseHashes = new HashMap<>();

    /* Statistics look as follows: 4 numbers for each row, they indicate:
        1. Number of non-null values
        2. Number of malformed values
        3. Number of NULL values
        4. Number of zero-values (0.0 for float, double; 0 for long)

        Minmax values table as follows:

        1. Min value of this column
        2. Max value of this column

       */

    public static TreeMap<String, Set<String>> listsUniques = new TreeMap<>();

    public static TreeMap<String, Integer[]> statsTable = new TreeMap<>();

    public static HashMap<String, Float[]> minMaxTable = new HashMap<>();

    public static HashMap<String, Set<String>> uniqueStrings = new HashMap<>();

    private int parquetSize = 0;

    private int uniqueMax = Integer.MAX_VALUE;

    private boolean predefinedHeaders = false;

    private HashMap<String, Schema.Type> typeTable = new HashMap<>();

    private int totalRecords = 0;

    private int timeGated = 0;

    private int writtenRecords = 0;

    private String startTime = "";

    private String endTime = "";

    private long startTimeEpoch = 0L;

    private long endTimeEpoch = 0L;

    private String radius = "";

    private boolean isCoordinateRandomizedInUncertainty = false;

    private String cellLocationIdentifier = "";

    private double cellLocationEqualityTolerance = 0.00002;

    private Map<String, Integer> timeData = new TreeMap<>();

    private Map<String, List<String>> rowNulls = new HashMap<>();

    private static final int COL_NON_NULL_VALUES = 0;

    private static final int COL_MALFORMED_VALUES = 1;

    private static final int COL_NULL_VALUES = 2;

    private static final int COL_ZERO_VALUES = 3;

    private static final int COL_MIN_VALUE = 0;

    private static final int COL_MAX_VALUE = 1;

    private static final String PARQUET_FILE_EXTENSION = ".parquet";

    private static String timeZone = "Z";

    private String formatName(String name) {
        name = name.replace("_", " ");
        StringBuilder newName = new StringBuilder();
        for (int i = 0; i < name.length() - 2; i++) {
            if (Character.isLowerCase(name.charAt(i)) && Character.isUpperCase(name.charAt(i + 1)) && Character.isLowerCase(name.charAt(i + 2))) {
                newName.append(name.charAt(i));
                newName.append(" ");
            } else {
                newName.append(name.charAt(i));
            }
        }
        newName.append(name.charAt(name.length() - 2));
        newName.append(name.charAt(name.length() - 1));
        return newName.toString();
    }

    public static void main(String[] args) {
        Ldb4RdbConverter converter = new Ldb4RdbConverter();
        CmdLineParser parser = new CmdLineParser(converter);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            System.exit(1);
        }
        converter.run();
    }

    private Schema.Type getSchemaType(Schema subschema, String field) {
        if (subschema.getType() == Schema.Type.UNION) {
            if (field == null) {
                return subschema.getTypes().get(0).getType();
            } else {
                return subschema.getTypes().get(1).getType();
            }
        } else {
            return subschema.getType();
        }
    }

    private Schema.Type getSchemaType(Schema subschema) {
        if (subschema.getType() == Schema.Type.UNION) {
            return subschema.getTypes().get(1).getType();
        } else {
            return subschema.getType();
        }
    }

    private JSONObject determineType(String headername, String example) {
        JSONObject jo = new JSONObject();
        //Make all rows that get created with retainHashes as longs.

        if (headername.endsWith("_IDs")){
            jo.put("name", headername);
            JSONArray typelist = new JSONArray();
            typelist.add("null");
            typelist.add("long");
            jo.put("type", typelist);
        }
        else {

            // Try to determine type

            try {
                TimeUtils.timeToMillisecondsConverter(example, timeFormatter, inputEpochInMilliseconds, timeZone);
                jo.put("name", headername);
                jo.put("type", JsonUtils.getNullableLongType());
                timeColumns.add(headername);
            } catch (DateTimeParseException e1) {
                try {
                    Float.parseFloat(example);
                    jo.put("name", headername);
                    jo.put("type", JsonUtils.getNullableFloatType());
                } catch (NumberFormatException e2) {
                    JSONArray typelist = new JSONArray();
                    jo.put("name", headername);
                    typelist.add("null");
                    if (hashColumns.contains(headername)) {
                        typelist.add("long");
                    } else {
                        typelist.add(defaultType);
                    }
                    jo.put("type", typelist);
                }
            }

            // All undefinable data types are marked as strings.

            catch (NullPointerException e1) {
                jo.put("name", headername);
                jo.put("type", JsonUtils.getNullableStringType());
            }
        }
        return jo;
    }

    private JSONObject createJSONFromCSVRecords(List<String> headerrow, List<String> examplerow) {
        JSONObject mainjson = new JSONObject();
        mainjson.put("name", "locationrecord");
        mainjson.put("type", "record");
        mainjson.put("namespace", "com.demograft.ldb4rdbconverter.generated");
        JSONArray list = new JSONArray();
        GeometryTransformer.addGeometryJsonObjectsToList(list);
        for (int i = 0; i < headerrow.size(); i++) {
            String headername = headerrow.get(i).trim();
            String example = examplerow.get(i);
            JSONObject jo = new JSONObject();

            // Check if it is one of the three important rows: longitude, latitude and time.

            if (headername.equals(longitude)) {
                jo.put("name", "lon");
                jo.put("type", "double");
            } else if (headername.equals(latitude)) {
                jo.put("name", "lat");
                jo.put("type", "double");
            } else if (headername.equals(time)) {
                jo.put("name", "time");
                jo.put("type", "long");
            }

            // Check if it is a field required for geometry calculations
            else if (headername.equals(radius)) {
                jo = new JSONObject();
                jo.put("name", "radius");
                jo.put("type", JsonUtils.getNullableFloatType());
            }
            // Then check if it is from predefined columns

            else if (longColumns.contains(headername) || timeColumns.contains(headername)) {
                jo.put("name", headername);
                jo.put("type", JsonUtils.getNullableLongType());
            } else if (floatColumns.contains(headername)) {
                jo.put("name", headername);
                jo.put("type", JsonUtils.getNullableFloatType());
            } else if (doubleColumns.contains(headername)) {
                jo.put("name", headername);
                jo.put("type", JsonUtils.getNullableDoubleType());
            } else if (stringColumns.contains(headername)) {
                jo.put("name", headername);
                jo.put("type", JsonUtils.getNullableStringType());
            } else if (tokenTypes.keySet().contains(headername)){
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add(tokenTypes.get(headername));
                jo.put("type", typelist);
            } else if (databaseHeaders.contains(headername)){
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add(databaseHeaderTypes.get(headername));
                jo.put("type", typelist);
            } else if (wktGeometryHeaders.contains(headername)){
                jo.put("name", headername);
                JSONArray typelist = new JSONArray();
                typelist.add("null");
                typelist.add("string");
                jo.put("type", typelist);
            }

            // Then try to determine column type

            else {
                jo = determineType(headername, example);
            }

            if (!columnsToRemoveList.contains(headername)) {  // If not manually excluded
                    list.add(jo);
            }
        }

        mainjson.put("fields", list);
        return mainjson;
    }

    private void writeAttributeFile(Schema schema, String fileName) {
        JSONObject attributeTr = new JSONObject();
        JSONArray data = new JSONArray();
        for (Schema.Field field : schema.getFields()) {
            JSONObject row = new JSONObject();
            row.put("hidden", false);
            String formattedName = formatName(field.name());
            if (field.name().equals("lat")) {
                row.put("attributeName", "Latitude");
                row.put("attributeTooltip", "Refined location of the record, used by heatmapping and optionally by trajectory visualisation function" +
                        "\nGeographic WGS84 coordinate in decimal degrees." +
                        "\nIn the current version equals to " + latitude);
                row.put("group", "Geographic Location");
                row.put("unit", "degrees");
                row.put("attributeId", field.name());
                data.add(row);
            } else if (field.name().equals("lon")) {
                row.put("attributeName", "Longitude");
                row.put("attributeTooltip", "Refined location of the record, used by heatmapping and optionally by trajectory visualisation function" +
                        "\\nGeographic WGS84 coordinate in decimal degrees." +
                        "\\nIn the current version equals to " + longitude);
                row.put("group", "Geographic Location");
                row.put("unit", "degrees");
                row.put("attributeId", field.name());
                data.add(row);
            } else if (getSchemaType(field.schema()) == Schema.Type.LONG && field.name().equals("time")) {

                // guiType:dateTime

                row.put("attributeName", "Time");
                row.put("attributeTooltip", formattedName);
                row.put("group", "Generic");
                row.put("attributeId", field.name());
                row.put("guiType", "dateTime");
                data.add(row);
            } else if (getSchemaType(field.schema()) == Schema.Type.LONG) {
                row.put("attributeName", formattedName);
                row.put("attributeTooltip", formattedName);
                row.put("group", "Generic");
                row.put("attributeId", field.name());
                if (timeColumns.contains(field.name())) {
                    row.put("guiType", "dateTime");
                }
                data.add(row);
            } else if(!derivedFields.contains(field.name())){
                if(hashColumns.contains(field.name()) || retainHashes.contains(field.name() + "_IDs")){
                    row.put("guiType", "ID");
                }
                row.put("attributeName", formattedName);
                row.put("attributeTooltip", formattedName);
                row.put("group", "Generic");
                row.put("attributeId", field.name());
                data.add(row);
            } else if(wktGeometryHeaders.contains(field.name())){
                row.put("attributeName", formattedName);
                row.put("attributeTooltip", formattedName);
                row.put("group", "Geographic Location");
                row.put("attributeId", field.name());
                data.add(row);
            }
        }
        if (!radius.equals("") || !cellLocationIdentifier.equals("")) {
            for (String geometryHeader : derivedFields) {
                JSONObject geoData = new JSONObject();
                geoData.put("group", "Geographic Location");
                geoData.put("attributeName", geometryHeader);
                geoData.put("attributeId", geometryHeader);
                geoData.put("attributeTooltip", geometryHeader);
                data.add(geoData);
            }
        }
        if(!trajectory.equals("")){
            JSONObject traj = new JSONObject();
            traj.put("hidden", true);
            traj.put("attributeName", "Trajectory");
            traj.put("idField", trajectory);
            traj.put("lonAttributeId","lon");
            traj.put("latAttributeId","lat");
            traj.put("guiType","trajectory");
            traj.put("group","trajectories");
            JSONArray infoList = new JSONArray();
            traj.put("infoAttributes",infoList);
            traj.put("attributeId","trajectory_1");
            data.add(traj);
        }
        attributeTr.put("data", data);

        try (FileWriter fw = new FileWriter(fileName)) {
            fw.write(attributeTr.toJSONString());
        } catch (IOException e) {
            log.info("Error writing the attributeTranslation file. Error message: " + e.getMessage());
        }
    }

    private String typeToString(Schema.Type type) {
        if (type == Schema.Type.LONG) {
            return "long";
        }
        if (type == Schema.Type.FLOAT) {
            return "float";
        }
        if (type == Schema.Type.DOUBLE) {
            return "double";
        } else {
            return "string";
        }
    }

    private Float checkForNullFloat(Float value, String headerName) {
        if (!rowNulls.containsKey(headerName)) {
            return value;
        } else {
            List<String> comparables = rowNulls.get(headerName);
            for (String comparable : comparables) {
                if (Float.parseFloat(comparable) == value) {
                    return null;
                }
            }
        }
        return value;
    }

    private Double checkForNullDouble(Double value, String headerName) {
        if (!rowNulls.containsKey(headerName)) {
            return value;
        } else {
            List<String> comparables = rowNulls.get(headerName);
            for (String comparable : comparables) {
                if (Double.parseDouble(comparable) == value) {
                    return null;
                }
            }
        }
        return value;
    }

    private Long checkForNullLong(Long value, String headerName) {
        if (!rowNulls.containsKey(headerName)) {
            return value;
        } else {
            List<String> comparables = rowNulls.get(headerName);
            for (String comparable : comparables) {
                if (Long.parseLong(comparable) == value) {
                    return null;
                }
            }
        }
        return value;
    }

    private String checkForNullString(String value, String headerName) {
        if (!rowNulls.containsKey(headerName)) {
            return value;
        } else {
            List<String> comparables = rowNulls.get(headerName);
            for (String comparable : comparables) {
                if (comparable.equals(value)) {
                    return null;
                }
            }
        }
        return value;
    }

    private void createTokenMaps(List<String> tokenFiles){
        for(String file: tokenFiles){
            File tokenFile = new File(file);
            CsvParserSettings settings = new CsvParserSettings();
            settings.detectFormatAutomatically();
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(tokenFile);
            String[] headers = parser.parseNext();
            tokenHeaders.put(headers[0], headers[1]);
            tokenHeaders.put(headers[2], headers[1]);
            tokenTypes.put(headers[0], headers[3]);
            tokenTypes.put(headers[2], headers[4]);
            Map<String, Long> tokenMap = new HashMap<>();
            longMaps.put(headers[0], tokenMap);
            Map<String, StringBuffer> newMap = new HashMap<>();
            tokenMaps.put(headers[2], newMap);
            String[] record;
            while ((record = parser.parseNext()) != null) {
                String source = record[1];
                String token = record[0];
                StringBuffer tv = new StringBuffer(record[2]);
                tv.trimToSize();
                longMaps.get(headers[0]).put(source, Long.parseLong(token));
                tokenMaps.get(headers[2]).put(source, tv);
            }
            parser.stopParsing();
        }
    }


    private void checkValidity(List<String> headers) {
        for (String configValue : rowNulls.keySet()) {
            if (!headers.contains(configValue)) {
                log.error("Configuration file contains a property (" + configValue + ") that isn't a configuration property or a header in the data.");
                throw new RuntimeException("Configuration file contains a property (" + configValue + ") that isn't a configuration property or a header in the data.");
            }
        }
    }

    private void readConfigFile() {
        Properties defaultProp = new Properties();

        try (FileReader fileReader = new FileReader(configFile)) {
            defaultProp.load(fileReader);
        } catch (IOException e) {
            log.info("Error reading the configuration file, make sure your configuration file is named correctly.");
        }
        if (!defaultProp.containsKey("input-file")) {
            throw new RuntimeException("Missing input-file property in configuration file");
        }
        inputFile = new File(defaultProp.getProperty("input-file"));
        if (!defaultProp.containsKey("output-file")) {
            throw new RuntimeException("Missing output-file property in configuration file");
        }
        outputFile = defaultProp.getProperty("output-file");
        if (!defaultProp.containsKey("longitude")) {
            throw new RuntimeException("Missing longitude property in configuration file");
        }
        longitude = defaultProp.getProperty("longitude");
        if (!defaultProp.containsKey("latitude")) {
            throw new RuntimeException("Missing latitude property in configuration file");
        }
        latitude = defaultProp.getProperty("latitude");
        if (!defaultProp.containsKey("time")) {
            throw new RuntimeException("Missing time property in configuration file");
        }
        time = defaultProp.getProperty("time");
        if (defaultProp.containsKey("excluded")) {
            String propInfo = defaultProp.getProperty("excluded");
            columnsToRemove = propInfo.split(",");
            columnsToRemoveList = new ArrayList<>(Arrays.asList(columnsToRemove));
        }
        if (defaultProp.containsKey("parquet-size")) {
            parquetSize = Integer.parseInt(defaultProp.getProperty("parquet-size"));
        }

        if (defaultProp.containsKey("default-type")) {
            if(!defaultProp.getProperty("default-type").equals("float") || !defaultProp.getProperty("default-type").equals("double")){
                log.info("Wrong default type chosen in the configuration file. Defaulting to string");
            }
            else{
                defaultType=defaultProp.getProperty("default-type");
            }
        }

        if(defaultProp.containsKey("columns-to-map-long")) {
            String propInfo = defaultProp.getProperty("columns-to-map-long");
            for (String column : propInfo.split(",")) {
                hashColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("time-columns")) {
            String propInfo = defaultProp.getProperty("time-columns");
            for (String column : propInfo.split(",")) {
                timeColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("long-null-values")) {
            String propInfo = defaultProp.getProperty("long-null-values");
            for (String column : propInfo.split(",")) {
                longNullValues.add(Long.parseLong(column.trim()));
            }
        }
        if (defaultProp.containsKey("double-null-values")) {
            String propInfo = defaultProp.getProperty("double-null-values");
            for (String column : propInfo.split(",")) {
                doubleNullValues.add(Double.parseDouble(column.trim()));
            }
        }
        if (defaultProp.containsKey("float-null-values")) {
            String propInfo = defaultProp.getProperty("float-null-values");
            for (String column : propInfo.split(",")) {
                floatNullValues.add(Float.parseFloat(column.trim()));
            }
        }
        if (defaultProp.containsKey("unique-strings")) {
            String propInfo = defaultProp.getProperty("unique-strings");
            uniqueMax = Integer.parseInt(propInfo);
        }
        if (defaultProp.containsKey("string-null-values")) {
            String propInfo = defaultProp.getProperty("string-null-values");
            for (String column : propInfo.split(",")) {
                stringNullValues.add(column);
            }
        }
        if (defaultProp.containsKey("long-columns")) {
            String propInfo = defaultProp.getProperty("long-columns");
            for (String column : propInfo.split(",")) {
                longColumns.add(column.trim());
            }
        }
        if(defaultProp.containsKey("database-files")){
            String propInfo = defaultProp.getProperty("database-files");
            for (String file : propInfo.split(",")){
                databaseFiles.add(file.trim());
            }
        }
        if(defaultProp.containsKey("wkt-geometry-files")){
            String propInfo = defaultProp.getProperty("wkt-geometry-files");
            for (String file : propInfo.split(",")){
                String[] pieces = file.split("\\.");
                wktGeometryHeaders.add(pieces[0]);
                WKTGeometry geo = new WKTGeometry(new File(file), "name");
                wktMap.put(pieces[0], geo);
            }
        }
        if (defaultProp.containsKey("float-columns")) {
            String propInfo = defaultProp.getProperty("float-columns");
            for (String column : propInfo.split(",")) {
                floatColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("double-columns")) {
            String propInfo = defaultProp.getProperty("double-columns");
            for (String column : propInfo.split(",")) {
                doubleColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("tokenFiles")) {
            String propInfo = defaultProp.getProperty("tokenFiles");
            for (String column : propInfo.split(",")){
                tokenFiles.add(column.trim());
            }
        }
        if (defaultProp.containsKey("list-columns")) {
            String propInfo = defaultProp.getProperty("list-columns");
            for (String column : propInfo.split(",")){
                listLists.add(column.trim());
            }
        }
        if (defaultProp.containsKey("string-columns")) {
            String propInfo = defaultProp.getProperty("string-columns");
            for (String column : propInfo.split(",")) {
                stringColumns.add(column.trim());
            }
        }
        if (defaultProp.containsKey("headers")) {
            String headerInfo = defaultProp.getProperty("headers").trim();
            predefinedHeaders = true;
            String[] headerArray = headerInfo.split(",");
            this.headerArray = headerArray;
        }

        if (defaultProp.containsKey("retain-hashes")) {
            String headerInfo = defaultProp.getProperty("retain-hashes").trim();
            String[] retainHashesArray = headerInfo.split(",");
            this.retainHashes = new LinkedList<>(Arrays.asList(retainHashesArray));
        }

        if (defaultProp.containsKey("stats-file")) {
            statsFile = defaultProp.getProperty("stats-file");
        }
        if (defaultProp.containsKey("start-time")) {
            startTime = defaultProp.getProperty("start-time");
        }
        if (defaultProp.containsKey("end-time")) {
            endTime = defaultProp.getProperty("end-time");
        }
        if (defaultProp.containsKey("timezone")) {
            timeZone = defaultProp.getProperty("timezone");
        }
        if (defaultProp.containsKey("trajectoryID")) {
            trajectory = defaultProp.getProperty("trajectoryID");
        }
        if (defaultProp.containsKey("radius")) {
            radius = defaultProp.getProperty("radius");
        }
        if (defaultProp.containsKey("cell-location-identifier")) {
            cellLocationIdentifier = defaultProp.getProperty("cell-location-identifier");
        }
        if (defaultProp.containsKey("cell-location-equality-tolerance")) {
            cellLocationEqualityTolerance = Double.parseDouble(defaultProp.getProperty("cell-location-equality-tolerance"));
        }
        if (defaultProp.containsKey("is-coordinate-randomized-in-uncertainty")) {
            String uncertaintyProperty = defaultProp.getProperty("is-coordinate-randomized-in-uncertainty");
            if (!uncertaintyProperty.trim().isEmpty()) {
                isCoordinateRandomizedInUncertainty = Boolean.parseBoolean(uncertaintyProperty.trim());
            }
        }
        if (defaultProp.contains("non-negative")){
            String nonNegatives = defaultProp.getProperty("non-negative").trim();
            String[] nonNegativesArray = nonNegatives.split(",");
            this.nonNegativeRows = new LinkedList<>(Arrays.asList(nonNegativesArray));
        }
        for (String key : defaultProp.stringPropertyNames()) {
            if (!propertySet.contains(key)) {
                String propInfo = defaultProp.getProperty(key);
                String[] values = propInfo.split(",");
                List<String> nulls = new ArrayList<>(Arrays.asList(values));
                for (String rowValue: nulls){
                    Character first = rowValue.charAt(0);
                    if(first.equals('+') || first.equals('!') || first.equals('*') || first.equals(':')){
                        if(!arithmetics.containsKey(key)){
                            arithmetics.put(key, new ArrayList<>());
                        }
                        arithmetics.get(key).add(rowValue);
                    }
                    else{
                        if(!rowNulls.containsKey(key)){
                            rowNulls.put(key, new ArrayList<>());
                        }
                        rowNulls.get(key).add(rowValue);
                    }
                }
            }
        }
    }

    private Long generateNewHash(String hash, Integer index) {
        if (hashTables.get(index).containsKey(hash)) {
            return hashTables.get(index).get(hash);
        }
        Long hashNumber = hashMapCounters[index] + 1;
        hashTables.get(index).put(hash, hashNumber);
        hashMapCounters[index] = hashNumber;
        return hashNumber;
    }

    private GenericData.Record convertToParquetRecord(Schema schema, InputRecord record) {
        FieldConversionResult fieldConversionResult = new FieldConversionResult(schema);
        for (Schema.Field field : schema.getFields()) {
            Schema subschema = field.schema();
            Schema.Type subSchemaType;
            if (derivedFields.contains(field.name())) {
                continue;
            }
            if (wktGeometryHeaders.contains(field.name())) {
                subSchemaType = Schema.Type.STRING;
            }
            else {
                subSchemaType = getSchemaType(record, field, subschema);
            }

            switch (subSchemaType) {
                case LONG:
                    // Long currently only used for dateTimes and hashes
                    convertLong(record, fieldConversionResult, field);
                    break;
                case DOUBLE:
                    convertDouble(record, fieldConversionResult, field);
                    break;
                case FLOAT:
                    convertFloat(record, fieldConversionResult, field);
                    break;
                case STRING:
                    convertString(record, fieldConversionResult, field);
                    break;
                case NULL:
                    //Union data types always have 2 types, first is null and the second is the needed type.
                    handleNullOrEmpty(fieldConversionResult, field);
                    break;
                default:
                    // TODO: Should we log a warning or throw an exception here?
                    break;
            }
        }

        if (!fieldConversionResult.isFaulty()) {
            totalRecords += 1;
            writtenRecords += 1;
            LocalDate localdate;
            if (timeFormatter != null) {
                ZonedDateTime date;
                try {
                    date = ZonedDateTime.parse(record.getString(time), timeFormatter);
                } catch (DateTimeParseException e) {
                    LocalDateTime localDate = LocalDateTime.parse(record.getString(time), timeFormatter);
                    date = localDate.atZone(ZoneId.of(timeZone));
                }
                localdate = date.toLocalDate();
            } else {
                long epoch = Long.parseLong(record.getString(time));
                if (inputEpochInMilliseconds) {
                    Instant instant = Instant.ofEpochMilli(epoch);
                    ZonedDateTime date = ZonedDateTime.ofInstant(instant, ZoneId.of(timeZone));
                    localdate = date.toLocalDate();
                } else {
                    Instant instant = Instant.ofEpochSecond(epoch);
                    ZonedDateTime date = ZonedDateTime.ofInstant(instant, ZoneId.of(timeZone));
                    localdate = date.toLocalDate();
                }
            }
            if (timeData.containsKey(localdate.toString())) {
                Integer count = timeData.get(localdate.toString());
                count += 1;
                timeData.put(localdate.toString(), count);
            } else {
                timeData.put(localdate.toString(), 1);
            }
            if (localdate.toEpochDay() < beginDate.toEpochDay()){
                beginDate = localdate;
            }
            if (localdate.toEpochDay() > endDate.toEpochDay()){
                endDate = localdate;
            }
            return fieldConversionResult.getGenericRecordBuilder().build();
        } else {
            totalRecords += 1;
            return null;
        }
    }

    private void handleNullOrEmpty(FieldConversionResult fieldConversionResult, Schema.Field field) {
        Integer[] stat2 = statsTable.get(field.name());
        stat2[COL_NULL_VALUES] += 1;
        statsTable.put(field.name(), stat2);
        fieldConversionResult.updateGenericRecordBuilder(field, null);
    }

    private void convertString(InputRecord record, FieldConversionResult fieldConversionResult, Schema.Field field) {
        try {
                String answer;
                String fieldName = field.name();
                if(wktGeometryHeaders.contains(fieldName)){
                    answer = wktMap.get(fieldName).getContainingAreaName(record.getDouble(longitude), record.getDouble(latitude));
                }
                else if(tokenHeaders.keySet().contains(fieldName)){
                    String preToken = record.getString(tokenHeaders.get(fieldName));
                    String nr = tokenMaps.get(fieldName).get(preToken.toLowerCase()).toString();
                    if (nr == null){
                        fieldConversionResult.updateGenericRecordBuilder(field, null);
                        throw new NullPointerException();
                    }
                    answer = nr;
                }
                else{
                    answer = record.getString(field.name());
                    if(answer.equals("")){
                        handleNullOrEmpty(fieldConversionResult, field);
                    }
                }
                checkFieldValidity(field, checkForNullString(answer, fieldName) == null, stringNullValues.contains(answer));
                if(answer == null){
                    throw new NullPointerException();
                }
                if(!listLists.contains(fieldName)) {
                    Set<String> set = uniqueStrings.get(fieldName);
                    set.add(answer);
                    uniqueStrings.put(fieldName, set);
                }
                else{
                    Set<String> set = listsUniques.get(fieldName);
                    String[] answers = answer.split(Character.toString((char)0x1E));
                    List<String> answerList= new ArrayList<>(Arrays.asList(answers));
                    set.addAll(answerList);
                    listsUniques.put(fieldName, set);
                }
                fieldConversionResult.updateGenericRecordBuilder(field, answer);
                Integer[] stat = statsTable.get(fieldName);
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(fieldName, stat);
            } catch (NullPointerException e1) {
                handleNullOrEmpty(fieldConversionResult, field);
            }
    }

    private void convertFloat(InputRecord record, FieldConversionResult fieldConversionResult, Schema.Field field) {
        try {
            if (field.name().equals("radius")) {
                try {
                    String value = record.getString(radius);
                    if (value == null || value.equals("null") || value.equals("")) {
                        throw new NullPointerException();
                    }
                    Float floatValue = Float.valueOf(value);
                    checkFieldValidity(field, checkForNullFloat(floatValue, field.name()) == null, floatNullValues.contains(floatValue));
                    if(floatValue < 0 && nonNegativeRows.contains(field.name())){
                        throw new NumberFormatException();
                    }
                    fieldConversionResult.updateGenericRecordBuilder(field, floatValue);
                    Integer[] stat = statsTable.get(field.name());
                    stat[COL_NON_NULL_VALUES] += 1;
                    statsTable.put(field.name(), stat);
                    if(minMaxTable.get("radius")[0] > floatValue){
                        Float[] stat2 = minMaxTable.get("radius");
                        stat2[0] = floatValue;
                        minMaxTable.put("radius", stat2);
                    }
                    if(minMaxTable.get("radius")[1] < floatValue){
                        Float[] stat2 = minMaxTable.get("radius");
                        stat2[1] = floatValue;
                        minMaxTable.put("radius", stat2);
                    }
                } catch (NullPointerException ex) {
                    handleNullOrEmpty(fieldConversionResult, field);
                }
            } else {
                Float foundValue;
                String fieldName = field.name();
                if (tokenHeaders.keySet().contains(field.name())) {
                    String preToken = record.getString(tokenHeaders.get(fieldName));
                    String nr = tokenMaps.get(fieldName).get(preToken.toLowerCase()).toString();
                    if (nr == null) {
                        fieldConversionResult.updateGenericRecordBuilder(field, null);
                        throw new NullPointerException();
                    }
                    foundValue = Float.parseFloat(nr);
                } else {
                    foundValue = record.getFloat(fieldName);
                }
                // Max values are sometimes used to indicate missing data.
                checkFieldValidity(field, checkForNullFloat(foundValue, fieldName) == null, floatNullValues.contains(foundValue));
                if(arithmetics.containsKey(fieldName)){
                    List<String> operations = arithmetics.get(fieldName);
                    for (String operation: operations){
                        Character operator = operation.charAt(0);
                        String number = operation.substring(1);
                        if(operator.equals('+')){
                            foundValue += Float.parseFloat(number);
                        }
                        if(operator.equals('!')){
                            foundValue -= Float.parseFloat(number);
                        }
                        if(operator.equals('*')){
                            foundValue = foundValue * Float.parseFloat(number);
                        }
                        if(operator.equals(':')){
                            foundValue = foundValue / Float.parseFloat(number);
                        }

                    }
                }
                if(foundValue < 0 && nonNegativeRows.contains(field.name())){
                    throw new NumberFormatException();
                }
                fieldConversionResult.updateGenericRecordBuilder(field, foundValue);
                Integer[] stat = statsTable.get(fieldName);
                Float[] minmax = minMaxTable.get(fieldName);
                stat[COL_NON_NULL_VALUES] += 1;
                if (foundValue == 0.0F) {
                    stat[COL_ZERO_VALUES] += 1;
                }
                statsTable.put(field.name(), stat);
                if (minmax[COL_MIN_VALUE] > foundValue) {
                    minmax[COL_MIN_VALUE] = foundValue;
                }
                if (minmax[COL_MAX_VALUE] < foundValue) {
                    minmax[COL_MAX_VALUE] = foundValue;
                }
                minMaxTable.put(field.name(), minmax);
            }
        } catch (NumberFormatException e1) {
            Integer[] stat2 = statsTable.get(field.name());
            stat2[COL_NON_NULL_VALUES] += 1;
            stat2[COL_MALFORMED_VALUES] += 1;
            statsTable.put(field.name(), stat2);
            fieldConversionResult.updateGenericRecordBuilder(field, null);
        } catch (NullPointerException e) {
            handleNullOrEmpty(fieldConversionResult, field);
        }
    }

    private void convertDouble(InputRecord record, FieldConversionResult fieldConversionResult, Schema.Field field) {
        if (field.name().equals("lon")) {
            try {
                Double number = getValidatedLatitudeLongitude(record, field, longitude);
                if (number > 180 || number < -180) {
                    throw new NullPointerException();
                }
                fieldConversionResult.updateGenericRecordBuilder("lon", number);
                Float[] minmax = minMaxTable.get(field.name());
                Integer[] stat2 = statsTable.get("lon");
                stat2[COL_NON_NULL_VALUES] += 1;
                if (number == 0.0D) {
                    stat2[COL_ZERO_VALUES] += 1;
                }
                statsTable.put("lon", stat2);
                if (minmax[COL_MIN_VALUE] > number) {
                    minmax[COL_MIN_VALUE] = number.floatValue();
                }
                if (minmax[COL_MAX_VALUE] < number) {
                    minmax[COL_MAX_VALUE] = number.floatValue();
                }
                minMaxTable.put(field.name(), minmax);
            } catch (NumberFormatException e) {
                Integer[] stat2 = statsTable.get("lon");
                stat2[COL_NON_NULL_VALUES] += 1;
                stat2[COL_MALFORMED_VALUES] += 1;
                statsTable.put("lon", stat2);
                fieldConversionResult.setFaulty(true);
            } catch (NullPointerException e1) {
                Integer[] stat = statsTable.get("lon");
                stat[COL_NULL_VALUES] += 1;
                statsTable.put("lon", stat);
                fieldConversionResult.setFaulty(true);
            }
        } else if (field.name().equals("lat")) {
            try {
                Double number = getValidatedLatitudeLongitude(record, field, latitude);
                if (number > 90 || number < -90) {
                    throw new NullPointerException();
                }
                fieldConversionResult.updateGenericRecordBuilder("lat", number);
                Integer[] stat2 = statsTable.get("lat");
                Float[] minmax = minMaxTable.get(field.name());
                stat2[COL_NON_NULL_VALUES] += 1;
                if (number == 0.000000D) {
                    stat2[COL_ZERO_VALUES] += 1;
                }
                statsTable.put("lat", stat2);
                if (minmax[COL_MIN_VALUE] > number) {
                    minmax[COL_MIN_VALUE] = number.floatValue();
                }
                if (minmax[COL_MAX_VALUE] < number) {
                    minmax[COL_MAX_VALUE] = number.floatValue();
                }
                minMaxTable.put(field.name(), minmax);
            } catch (NumberFormatException e) {
                Integer[] stat2 = statsTable.get("lat");
                stat2[COL_NON_NULL_VALUES] += 1;
                stat2[COL_MALFORMED_VALUES] += 1;
                statsTable.put("lat", stat2);
                fieldConversionResult.setFaulty(true);
            } catch (NullPointerException e1) {
                Integer[] stat = statsTable.get("lat");
                stat[COL_NULL_VALUES] += 1;
                statsTable.put("lat", stat);
                fieldConversionResult.setFaulty(true);
            }
        } else if (tokenHeaders.keySet().contains(field.name())){
            try {
                String fieldName = field.name();
                String preToken = record.getString(tokenHeaders.get(fieldName));
                String nr = tokenMaps.get(fieldName).get(preToken.toLowerCase()).toString();
                if (nr == null){
                    fieldConversionResult.updateGenericRecordBuilder(field, nr);
                    throw new NullPointerException();
                }
                Double number = Double.parseDouble(nr);
                fieldConversionResult.updateGenericRecordBuilder(field, number);
                Integer[] stat = statsTable.get(fieldName);
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
                Float[] minmax = minMaxTable.get(field.name());
                if (minmax[COL_MIN_VALUE] > number) {
                    minmax[COL_MIN_VALUE] = number.floatValue();
                }
                if (minmax[COL_MAX_VALUE] < number) {
                    minmax[COL_MAX_VALUE] = number.floatValue();
                }
            } catch (NullPointerException e) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        } else {
            try {
                Double number = record.getDouble(field.name());
                checkFieldValidity(field, checkForNullDouble(number, field.name()) == null, doubleNullValues.contains(number));
                if(number < 0D && nonNegativeRows.contains(field.name())){
                    throw new NumberFormatException();
                }
                fieldConversionResult.updateGenericRecordBuilder(field, number);
                Integer[] stat = statsTable.get(field.name());
                Float[] minmax = minMaxTable.get(field.name());
                stat[COL_NON_NULL_VALUES] += 1;
                if (number == 0.000000D) {
                    stat[COL_ZERO_VALUES] += 1;
                }
                statsTable.put(field.name(), stat);
                if (minmax[COL_MIN_VALUE] > number) {
                    minmax[COL_MIN_VALUE] = number.floatValue();
                }
                if (minmax[COL_MAX_VALUE] < number) {
                    minmax[COL_MAX_VALUE] = number.floatValue();
                }
            } catch (NumberFormatException e) {
                handleNullOrEmpty(fieldConversionResult, field);
            } catch (NullPointerException e1) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }
    }

    private void convertLong(InputRecord record, FieldConversionResult fieldConversionResult, Schema.Field field) {
        if (field.name().equals("time")) {
            try {
                if (record.getString(time).equals("")) {
                    throw new NullPointerException();
                }
                Long timeVal = TimeUtils.timeToMillisecondsConverter(record.getString(time), timeFormatter, inputEpochInMilliseconds, timeZone);
                checkFieldValidity(field, checkForNullLong(timeVal, field.name()) == null, longNullValues.contains(timeVal));
                if (startTimeEpoch != 0L && timeVal < startTimeEpoch) {
                    timeGated += 1;
                    fieldConversionResult.setFaulty(true);
                }
                if (endTimeEpoch != 0L && timeVal > endTimeEpoch) {
                    timeGated += 1;
                    fieldConversionResult.setFaulty(true);
                }
                fieldConversionResult.updateGenericRecordBuilder("time", timeVal);
                Integer[] stat2 = statsTable.get("time");
                stat2[COL_NON_NULL_VALUES] += 1;
                statsTable.put("time", stat2);
            } catch (DateTimeParseException e) {
                Integer[] stat2 = statsTable.get("time");
                stat2[COL_NON_NULL_VALUES] += 1;
                stat2[COL_MALFORMED_VALUES] += 1;
                statsTable.put("time", stat2);
                fieldConversionResult.setFaulty(true);
            } catch (NullPointerException e1) {
                Integer[] stat = statsTable.get("time");
                stat[COL_NULL_VALUES] += 1;
                statsTable.put("time", stat);
                fieldConversionResult.setFaulty(true);
            }
        } else if (hashColumns.contains(field.name())) {
            String fieldName = field.name();
            Integer index = hashColumns.indexOf(fieldName);
            try {
                Long hashValue = generateNewHash(record.getString(fieldName), index);
                fieldConversionResult.updateGenericRecordBuilder(field, hashValue);
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            } catch (NullPointerException e) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }
        // check whether it is the newly generated field for retainHashes by checking it's final 4 symbols
        else if(field.name().endsWith("_IDs")){
            String fieldName = field.name();
            Integer index = hashColumns.size() + retainHashes.indexOf(field.name().substring(0, field.name().length() - 4));
            try {
                Long hashValue = generateNewHash(record.getString(fieldName.substring(0,fieldName.length() - 4)), index);
                fieldConversionResult.updateGenericRecordBuilder(field, hashValue);
                Integer[] stat = statsTable.get(fieldName);
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            } catch (NullPointerException e) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }

        // Check whether it's a generated row by one of the token Files.

        else if (tokenHeaders.keySet().contains(field.name())){
            try{
                String fieldName = field.name();
                String preToken = record.getString(tokenHeaders.get(fieldName));
                Long number = null;
                if(tokenMaps.containsKey(fieldName)){
                    number = Long.parseLong(tokenMaps.get(fieldName).get(preToken.toLowerCase()).toString());
                }
                if(longMaps.containsKey(fieldName)){
                    number = longMaps.get(fieldName).get(preToken.toLowerCase());
                }
                if (number == null){
                    fieldConversionResult.updateGenericRecordBuilder(field, null);
                    throw new NullPointerException();
                }
                fieldConversionResult.updateGenericRecordBuilder(field, number);
                Integer[] stat = statsTable.get(fieldName);
                stat[COL_NON_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            } catch (NullPointerException e) {
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }

        else {
            // Could be null or long
            try {
                if(databaseHeaders.contains(field.name())){
                    String sourceHeader = databaseSources.get(field.name());
                    String source = record.getString(sourceHeader).toLowerCase();
                    DB db = databases.get(field.name());
                    HTreeMap map = db.hashMap(field.name()).createOrOpen();
                    if(!map.containsKey(source)) {
                        int hash = databaseHashes.get(field.name());
                        map.put(source, hash);
                        fieldConversionResult.updateGenericRecordBuilder(field, (long) hash);
                        int newhash = hash + 1;
                        databaseHashes.put(source, newhash);
                        Integer[] stat = statsTable.get(field.name());
                        stat[COL_NON_NULL_VALUES] += 1;
                        statsTable.put(field.name(), stat);
                    }
                    else{
                        String result = String.valueOf(map.get(source));
                        if(result == null){
                            throw new NullPointerException();
                        }
                        else{
                            Long res = Long.parseLong(result);
                            fieldConversionResult.updateGenericRecordBuilder(field, res);
                            Integer[] stat = statsTable.get(field.name());
                            stat[COL_NON_NULL_VALUES] += 1;
                            statsTable.put(field.name(), stat);
                        }
                    }

                }
                else {
                    Long date = TimeUtils.timeToMillisecondsConverter(record.getString(field.name()), timeFormatter, inputEpochInMilliseconds, timeZone);
                    checkFieldValidity(field, checkForNullLong(date, field.name()) == null, longNullValues.contains(date));
                    fieldConversionResult.updateGenericRecordBuilder(field, date);
                    Integer[] stat = statsTable.get(field.name());
                    stat[COL_NON_NULL_VALUES] += 1;
                    statsTable.put(field.name(), stat);
                }
            } catch (DateTimeParseException e) {
                try {
                    Long number = record.getLong(field.name());
                    checkFieldValidity(field, checkForNullLong(number, field.name()) == null, longNullValues.contains(number));
                    Integer[] stat = statsTable.get(field.name());
                    stat[COL_NON_NULL_VALUES] += 1;
                    if (number == null || number == 0L) {
                        stat[COL_ZERO_VALUES] += 1;
                    }
                    statsTable.put(field.name(), stat);
                    fieldConversionResult.updateGenericRecordBuilder(field, number);
                } catch (NumberFormatException e1) {
                    fieldConversionResult.updateGenericRecordBuilder(field, null);
                    Integer[] stat2 = statsTable.get(field.name());
                    stat2[COL_NON_NULL_VALUES] += 1;
                    stat2[COL_MALFORMED_VALUES] += 1;
                }
            } catch (NullPointerException e2) {
                fieldConversionResult.updateGenericRecordBuilder(field, null);
                Integer[] stat = statsTable.get(field.name());
                stat[COL_NULL_VALUES] += 1;
                statsTable.put(field.name(), stat);
            }
        }
    }

    private void checkFieldValidity(Schema.Field field, boolean isNullCheckNull, boolean containsNullValues) {
        if (rowNulls.containsKey(field.name())) {
            if (isNullCheckNull) {
                throw new NullPointerException();
            }
        } else {
            if (containsNullValues) {
                throw new NullPointerException();
            }
        }
    }

    private Double getValidatedLatitudeLongitude(InputRecord record, Schema.Field field, String value) {
        if (record.getString(value).equals("")) {
            throw new NullPointerException();
        }
        Double number = record.getDouble(value);
        checkFieldValidity(field, checkForNullDouble(number, field.name()) == null, doubleNullValues.contains(number));
        return number;
    }

    private Schema.Type getSchemaType(InputRecord record, Schema.Field field, Schema subschema) {
        String targetName;
        if (field.name().equals("lat")) {
            targetName = latitude;
        } else if (field.name().equals("lon")) {
            targetName = longitude;
        } else if (field.name().equals("time")) {
            targetName = time;
        } else if (field.name().endsWith("_IDs")){   // Deal with created hash rows
            return Schema.Type.LONG;
        } else if (field.name().equals("radius")) {
            targetName = radius;
        } else if(databaseHeaders.contains(field.name())){
            switch(databaseHeaderTypes.get(field.name())){
                case "long": {
                    return Schema.Type.LONG;
                }
                case "double": {
                    return Schema.Type.DOUBLE;
                }
                case "string": {
                    return Schema.Type.STRING;
                }
                case "float": {
                    return Schema.Type.FLOAT;
                }
                default:{
                    log.info("This should not be possible");
                    return null;
                }
            }
        } else if (tokenHeaders.keySet().contains(field.name())){
            switch(tokenTypes.get(field.name())){
                case "long": {
                    return Schema.Type.LONG;
                }
                case "double": {
                    return Schema.Type.DOUBLE;
                }
                case "string": {
                    return Schema.Type.STRING;
                }
                case "float": {
                    return Schema.Type.FLOAT;
                }
                default:{
                    log.info("This should not be possible");
                    return null;
                }
            }
        } else {
            targetName = field.name();
        }
        return getSchemaType(subschema, record.getString(targetName));
    }

    //  Write the statistics file in a slightly different, a bit more readable format.


    /*private void writeStatsFile() {
        statistics.append("Parquet file generation successful. \n \n \nGeneral statistics: \nFound a total of " + totalRecords + " records. ");
        statistics.append(writtenRecords + " records from " + files + " files parsed into the parquet file. " + (totalRecords - writtenRecords) + " records contained malformed lat, lon or time fields. " + timeGated + " records discarded due to time restrictions.\n");
        statistics.append("Time restrictions set:    \nStart time:  " + startTime + "    \nEnd time:  " + endTime + ". \n\n");
        statistics.append("Field statistics in the form of: field name, non-null values, null values :  \n\n");
        for (Map.Entry<String, Set<String>> entry : uniqueStrings.entrySet()) {
            if (entry.getValue().size() > uniqueMax) {
                statistics.append("WARNING!!! Field " + entry.getKey() + " exceeded maximum allowed unique Classifiers. \n");
            }
        }
        for (Map.Entry<String, Integer[]> entry : statsTable.entrySet()) {
            String key = entry.getKey();
            Integer[] value = entry.getValue();
            DecimalFormat df = new DecimalFormat();

            // Statistics are shown in a comfortable format.

            df.applyPattern("###.###");
            statistics.append(key + " , non-null: " + value[0] + " , invalid: " + value[1] + " , null: " + value[2] + " , zero-values: " + value[3] + " , type: " + typeToString(typeTable.get(key)) + "\n");
            if (minMaxTable.containsKey(key)) {
                statistics.append("     Min: " + df.format(minMaxTable.get(key)[0]) + "       Max: " + df.format(minMaxTable.get(key)[1]) + "\n\n");
            }
            if (uniqueStrings.containsKey(key)) {
                statistics.append("     Unique string count: " + uniqueStrings.get(key).size() + "\n\n");
            }
        }
        statistics.append("The following string headers didn't contain any data:\n\t");
        for(Map.Entry<String, Set<String>> entry : uniqueStrings.entrySet()){
            if (entry.getValue().size() == 0){
                statistics.append(entry.getKey() + ",");
            }
        }
        statistics.append("\n\n The following number headers didn't contain any data:\n\n");
        for (Map.Entry<String, Integer[]> entry : statsTable.entrySet()) {
            String key = entry.getKey();
            Integer[] value = entry.getValue();
            if (minMaxTable.containsKey(key) && value[0]==0){
                statistics.append(key + ",");
            }
        }
        statistics.append("\n\n The following number headers didn't contain any valid data:\n\n");
        for (Map.Entry<String, Integer[]> entry : statsTable.entrySet()) {
            String key = entry.getKey();
            Integer[] value = entry.getValue();
            if (minMaxTable.containsKey(key) && value[0]==0 && value[1]!=0){
                statistics.append(key + ",");
            }
        }

        statistics.append("\n\n Time data.\nDate followed by the number of samples found from that date\n\n\n");
        for (Map.Entry<String, Integer> entry : timeData.entrySet()) {
            statistics.append(entry.getKey() + "   -   " + entry.getValue() + "\n");
        }
        try (FileWriter fw = new FileWriter(statsFile)) {
            fw.write(statistics.toString());
        } catch (IOException e) {
            log.info("Error writing the statistics file. Error message: " + e.getMessage());
        }
    }
    */

    private void writeCsvStats(){
        csvStatistics.append("Start time:" + beginDate.toString() + "\nEnd time:  " + endDate.toString() + ". \n\n");
        csvStatistics.append("Total records:" + totalRecords + ", Faulty records:" + (totalRecords - writtenRecords) + "\n\n");
        csvStatistics.append("Arithmetic operations: ! - subtraction, + - addition, : - divison, * - multiplication \n\n");
        csvStatistics.append("field,type,min,max,zero,non-null,unique,null,invalid,null definition,arithmetics \n\n");
        csvStatistics.append("Base fields: \n");
        DecimalFormat df = new DecimalFormat();
        df.applyPattern("###.##");

        //Determine alphapetical order.
        Set<String> correctSet = statsTable.keySet();
        List<String> correctOrder = new ArrayList<>(correctSet);
        Collections.sort(correctOrder,String.CASE_INSENSITIVE_ORDER);
        for(String column: correctOrder){
            if(!derivedFields.contains(column)) {
                csvStatistics.append(column + ",");
                csvStatistics.append(typeToString(typeTable.get(column)) + ",");
                if (minMaxTable.keySet().contains(column)) {
                    if (minMaxTable.get(column)[0] == Float.MAX_VALUE & minMaxTable.get(column)[1] == Float.MIN_VALUE) {
                        csvStatistics.append("N/A,N/A,");
                    } else {
                        csvStatistics.append(df.format(minMaxTable.get(column)[0]) + "," + df.format(minMaxTable.get(column)[1]) + ",");
                    }
                } else {
                    csvStatistics.append(" , ,");
                }
                csvStatistics.append(statsTable.get(column)[3] + ",");
                csvStatistics.append(statsTable.get(column)[0] + ",");
                if (listLists.contains(column)) {
                    csvStatistics.append("List elements:" + listsUniques.get(column).size() + ",");
                } else if (uniqueStrings.keySet().contains(column)){
                    csvStatistics.append(uniqueStrings.get(column).size() + ",");
                } else {
                    csvStatistics.append(" ,");
                }
                csvStatistics.append(statsTable.get(column)[2] + ",");
                csvStatistics.append(statsTable.get(column)[1] + ",");
                if (typeToString(typeTable.get(column)).equals("float")) {
                    for (float nr : floatNullValues) {
                        csvStatistics.append(nr + ";");
                    }
                    if (rowNulls.keySet().contains(column)) {
                        List<String> nulls = rowNulls.get(column);
                        for (String nullValue : nulls)
                            csvStatistics.append(nullValue + ";");
                    }
                } else if (typeToString(typeTable.get(column)).equals("double")) {
                    for (double nr : doubleNullValues) {
                        csvStatistics.append(nr + ";");
                    }
                    if (rowNulls.keySet().contains(column)) {
                        List<String> nulls = rowNulls.get(column);
                        for (String nullValue : nulls)
                            csvStatistics.append(nullValue + ";");
                    }
                } else if (typeToString(typeTable.get(column)).equals("string")) {
                    csvStatistics.append("[");
                    for (String str : stringNullValues) {
                        csvStatistics.append(str + ";");
                    }
                    if (rowNulls.keySet().contains(column)) {
                        List<String> nulls = rowNulls.get(column);
                        for (String nullValue : nulls)
                            csvStatistics.append(nullValue + ";");
                    }
                    csvStatistics.append("]");
                }
                csvStatistics.append(",");
                if (arithmetics.containsKey(column)) {
                    for (String arith : arithmetics.get(column)) {
                        csvStatistics.append(arith);
                    }
                }
                csvStatistics.append("\n");
            }
        }
        csvStatistics.append("\n");
        csvStatistics.append("Derived fields: \n");
        for(String column: derivedFields){
            csvStatistics.append(column + ",");
            csvStatistics.append(typeToString(typeTable.get(column)) + ",");
            if (minMaxTable.keySet().contains(column)) {
                if (minMaxTable.get(column)[0] == Float.MAX_VALUE & minMaxTable.get(column)[1] == Float.MIN_VALUE) {
                    csvStatistics.append("N/A,N/A,");
                } else {
                    csvStatistics.append(df.format(minMaxTable.get(column)[0]) + "," + df.format(minMaxTable.get(column)[1]) + ",");
                }
            } else {
                csvStatistics.append(" , ,");
            }
            csvStatistics.append(statsTable.get(column)[3] + ",");
            csvStatistics.append(statsTable.get(column)[0] + ",");
            if (uniqueStrings.keySet().contains(column)) {
                csvStatistics.append(uniqueStrings.get(column).size() + ",");
            } else {
                csvStatistics.append(" ,");
            }
            csvStatistics.append(statsTable.get(column)[2] + ",");
            csvStatistics.append(statsTable.get(column)[1] + ",");
            if (typeToString(typeTable.get(column)).equals("float")) {
                for (float nr : floatNullValues) {
                    csvStatistics.append(nr + ";");
                }
                if (rowNulls.keySet().contains(column)) {
                    List<String> nulls = rowNulls.get(column);
                    for (String nullValue : nulls)
                        csvStatistics.append(nullValue + ";");
                }
            } else if (typeToString(typeTable.get(column)).equals("double")) {
                for (double nr : doubleNullValues) {
                    csvStatistics.append(nr + ";");
                }
                if (rowNulls.keySet().contains(column)) {
                    List<String> nulls = rowNulls.get(column);
                    for (String nullValue : nulls)
                        csvStatistics.append(nullValue + ";");
                }
            } else if (typeToString(typeTable.get(column)).equals("string")) {
                csvStatistics.append("[");
                for (String str : stringNullValues) {
                    csvStatistics.append(str + ";");
                }
                if (rowNulls.keySet().contains(column)) {
                    List<String> nulls = rowNulls.get(column);
                    for (String nullValue : nulls)
                        csvStatistics.append(nullValue + ";");
                }
                csvStatistics.append("]");
            }
            csvStatistics.append(",");
            if (arithmetics.containsKey(column)) {
                for (String arith : arithmetics.get(column)) {
                    csvStatistics.append(arith);
                }
            }
            csvStatistics.append("\n");
        }

        csvStatistics.append("\n\nTime data.\nDate followed by the number of samples found from that date\n");
        csvStatistics.append("Used timezone: " + timeZone + "\n\n");
        for (Map.Entry<String, Integer> entry : timeData.entrySet()) {
            csvStatistics.append(entry.getKey() + "   -   " + entry.getValue() + "\n");
        }

        try (FileWriter fw = new FileWriter(statsFile + ".csv")) {
            fw.write(csvStatistics.toString());
        } catch (IOException e) {
            log.info("Error writing the csv statistics file. Error message: " + e.getMessage());
        }
    }

    private String newFileName(Integer number) {
        if (outputFile.contains(".")) {
            String[] pieces = outputFile.split("[.]");
            return pieces[0] + "_" + number + PARQUET_FILE_EXTENSION;
        } else {
            return outputFile + "_" + number + PARQUET_FILE_EXTENSION;
        }
    }

    private void run() {

        if (configFile.equals("")) {
            throw new RuntimeException("Configuration file not set, cannot proceed.");
        }
        readConfigFile();
        List<File> csvFiles = new ArrayList<>();
        List<File> parquetFiles = new ArrayList<>();

        if (!inputFile.isDirectory()) {
            String fileName = inputFile.getName();
            if (fileName.endsWith(".csv") || fileName.endsWith(".gz") || fileName.endsWith(".zip")) {
                csvFiles.add(inputFile);
            } else if (fileName.endsWith(PARQUET_FILE_EXTENSION)) {
                parquetFiles.add(inputFile);
            } else {
                throw new RuntimeException(MessageFormat.format("File \"{0}\" has an unknown extension", fileName));
            }
        } else {
            try {
                for (File file : inputFile.listFiles()) {
                    String fileName = file.getName();
                    if (fileName.endsWith(".csv") || fileName.endsWith(".gz") || fileName.endsWith(".zip")) {
                        csvFiles.add(file);
                    } else if (fileName.endsWith(PARQUET_FILE_EXTENSION)) {
                        parquetFiles.add(file);
                    }
                }
            } catch (NullPointerException e) {
                log.info("Error finding the file/directory, please recheck the file name and make sure this file or directory exists.");
            }
        }
        if (!csvFiles.isEmpty() && !parquetFiles.isEmpty()) {
            throw new RuntimeException("Input data can't be in CSV and Parquet files at the same time. Input directory should contain only one of these types.");
        }
        InputParser parser;
        File exampleFile;
        List<File> inputFiles;
        if (csvFiles.size() > 0) {
            if(predefinedHeaders){
                parser = new CsvInputParser(headerArray);
                headers = new LinkedList<>(Arrays.asList(headerArray));
            }
            else{
                parser = new CsvInputParser();
            }
            exampleFile = csvFiles.get(0);
            inputFiles = csvFiles;
        } else if (!parquetFiles.isEmpty()) {
            parser = new ParquetInputParser();
            exampleFile = parquetFiles.get(0);
            inputFiles = parquetFiles;
        } else {
            throw new RuntimeException(MessageFormat.format("Input file \"{0}\" does not have a valid input type", inputFile));
        }
        parser.beginParsing(exampleFile);
        if(!predefinedHeaders){
            headerArray = parser.getHeader();
            headers = new LinkedList<>(Arrays.asList(headerArray));
        }

        InputRecord exampleRow = parser.parseNextRecord();
        String[] exampleArray = exampleRow.getValues();
        examples = new LinkedList<>(Arrays.asList(exampleArray));

        parser.stopParsing();

        // Otherwise throws concurrentmodificationexception
        List<String> iterHeaders = new LinkedList<>(headers);

        //Remove excluded rows and their examples
        for(String row: iterHeaders){
            if (columnsToRemoveList.contains(row)){
                examples.remove(headers.indexOf(row));
                headers.remove(row);
            }
        }

        String timeString = exampleRow.getString(time);
        timeFormatter = TimeUtils.identifyTimeFormat(timeString, timeZone);
        if (timeFormatter == null && timeString.length() > 10) {
            inputEpochInMilliseconds = true;
        }
        for(String row: retainHashes){
            headers.add(row + "_IDs");
            examples.add("0");
        }
        createTokenMaps(tokenFiles);
        for(String row: tokenHeaders.keySet()){
            headers.add(row);
            examples.add("0"); // The type of this does not count, as token rows are given types based on the entries in tokenTypes
        }
        for(String file: databaseFiles){
            String[] parts = file.split("&");
            String source = parts[0];
            String end = parts[1];
            String[] parts2 = end.split("\\.");
            String target = parts2[0];
            databaseHeaders.add(target);
            DB db = DBMaker.fileDB(file).make();
            HTreeMap map = db.hashMap(target).createOrOpen();
            map.put("map_size", map.size());
            databases.put(target, db);
            databaseSources.put(target, source);
            databaseHashes.put(target, map.size());
        }

        for(String row: databaseHeaders){
            headers.add(row);
            examples.add("0");     // Type needs to be declared in the config file.
            if(longColumns.contains(row)){
                databaseHeaderTypes.put(row, "long");
            }
            else if(stringColumns.contains(row)){
                databaseHeaderTypes.put(row, "string");
            }
            else if(doubleColumns.contains(row)){
                databaseHeaderTypes.put(row, "double");
            }
            else if(floatColumns.contains(row)){
                databaseHeaderTypes.put(row, "float");
            }
        }
        for(String list: listLists){
            listsUniques.put(list, new HashSet<>());
        }

        int count = 1;
        for(String header: wktGeometryHeaders){
            headers.add(header);
            examples.add("0");    // These headers are always lists and as such strings.
            count = count + 1;
        }

        JSONObject mainjson = createJSONFromCSVRecords(headers, examples);
        hashMapCounters = new long[hashColumns.size() + retainHashes.size()];
        for (int i = 0; i < (hashColumns.size() + retainHashes.size()); i++) {
            HashMap<String, Long> toAdd = new HashMap<>();
            hashTables.add(toAdd);
        }


        Arrays.fill(hashMapCounters, 0L);

        Schema avroSchema = new Schema.Parser().parse(mainjson.toString());
        writeAttributeFile(avroSchema, "attributeTranslation.json");

        for (Schema.Field field : avroSchema.getFields()) {
            statsTable.put(field.name(), new Integer[]{0, 0, 0, 0});
            typeTable.put(field.name(), getSchemaType(field.schema()));
            if (getSchemaType(field.schema()) == Schema.Type.FLOAT || getSchemaType(field.schema()) == Schema.Type.DOUBLE) {
                minMaxTable.put(field.name(), new Float[]{Float.MAX_VALUE, Float.MIN_VALUE});
            }
            if (getSchemaType(field.schema()) == Schema.Type.STRING) {
                uniqueStrings.put(field.name(), new HashSet<>());
            }
        }
        if (!startTime.equals("")) {
            startTimeEpoch = TimeUtils.timeToMillisecondsConverter(startTime, timeFormatter, inputEpochInMilliseconds, timeZone);
        }
        if (!endTime.equals("")) {
            endTimeEpoch = TimeUtils.timeToMillisecondsConverter(endTime, timeFormatter, inputEpochInMilliseconds, timeZone);
        }

        /* CSV -> JSON -> Schema -> Record -> Parquetfile
                            |
                            |
                  attributeTranslation.json

        */

        int blockSize = 1024 * 1024;
        int pageSize = 1024 * 1024 * 16;

        int filenumber = 1;
        int written = 0;
        ParquetWriter<GenericData.Record> parquetWriter = null;
        List<Path> writtenFiles = new ArrayList<>();
        Path newFilePath = new Path(newFileName(filenumber));
        List<Cell> cells = new ArrayList<>();
        System.out.println();
        try {
            parquetWriter = AvroParquetWriter
                    .<GenericData.Record>builder(newFilePath)
                    .withSchema(avroSchema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(blockSize)
                    .withPageSize(pageSize)
                    .build();
            writtenFiles.add(newFilePath);
            for (File file : inputFiles) {
                parser.beginParsing(file);
                InputRecord record;
                try {
                    while ((record = parser.parseNextRecord()) != null) {
                        GenericData.Record toWrite;
                        toWrite = convertToParquetRecord(avroSchema, record);
                        if (toWrite != null) {
                            Cell newOne = new Cell(toWrite, cellLocationIdentifier, cellLocationEqualityTolerance);
                            cells.add(newOne);
                            if (parquetSize > 0 && written >= parquetSize) {
                                written = 0;
                                parquetWriter.close();
                                filenumber += 1;
                                newFilePath = new Path(newFileName(filenumber));
                                parquetWriter = AvroParquetWriter
                                        .<GenericData.Record>builder(newFilePath)
                                        .withSchema(avroSchema)
                                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                                        .withRowGroupSize(blockSize)
                                        .withPageSize(pageSize)
                                        .build();
                                writtenFiles.add(newFilePath);
                            }
                            parquetWriter.write(toWrite);
                            written += 1;
                        }
                    }
                } catch (NullPointerException e) {
                    log.info("Error. Encountered an empty file or a file with only a header row. File name is: {}.", file.getName());
                } finally {
                    files += 1;
                    parser.stopParsing();
                }
                cells = cells.stream()
                        .distinct()
                        .collect(Collectors.toList());
            }
        } catch (java.io.IOException e) {
            log.info(String.format("Error writing parquet file %s", e.getMessage()));
        } finally {
            for(String header: databaseHeaders){
                DB db = databases.get(header);
                HTreeMap map = db.hashMap(header).createOrOpen();
                map.put("map_size", map.size());
            }
            for(DB db: databases.values()){
                db.close();
            }
            if (parquetWriter != null) {
                try {
                    parquetWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        GeometryTransformer geometryTransformer = new GeometryTransformer(isCoordinateRandomizedInUncertainty, cells, cellLocationIdentifier);
        for (Path path : writtenFiles) {
            try (ParquetReader<GenericData.Record> reader = new AvroParquetReader(path)) {
                ParquetWriter<GenericData.Record> writer =
                        AvroParquetWriter.<GenericData.Record>builder(path.suffix("_with_geometry"))
                                .withSchema(avroSchema)
                                .withCompressionCodec(CompressionCodecName.SNAPPY)
                                .withRowGroupSize(blockSize)
                                .withPageSize(pageSize)
                                .build();
                GenericData.Record existingRecord = reader.read();
                while (existingRecord != null) {
                    GenericData.Record transformedRecord = geometryTransformer.transformRow(existingRecord);
                    writer.write(transformedRecord);
                    existingRecord = reader.read();
                }
                writer.close();
            } catch (java.io.IOException e) {
                log.info(String.format("Error writing parquet file %s", e.getMessage()));
            }
            try {
                FileSystem fileSystem = FileSystem.get(new Configuration());
                fileSystem.rename(path.suffix("_with_geometry"), path);
            } catch (IOException e) {
                log.error("Exception occurred when renaming output files");
            }
        }

        //writeStatsFile();
        writeCsvStats();
    }
}