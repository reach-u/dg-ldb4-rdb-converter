package com.demograft.ldb4rdbconverter.geometry;

import avro.shaded.com.google.common.collect.Lists;
import com.demograft.ldb4rdbconverter.Ldb4RdbConverter;
import com.demograft.ldb4rdbconverter.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.simple.JSONArray;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class GeometryTransformer {
    private final boolean isCoordinateRandomizedInUncertainty;
    private final String identifierFieldName;
    private final Map<String, Integer> radiuses;
    private static Set<String> derivedFields = Stream.of("geometryType", "geometryLatitude", "geometryLongitude", "orientationMajorAxis",
            "innerSemiMajorRadius", "innerSemiMinorRadius", "outerSemiMajorRadius", "outerSemiMinorRadius",
            "startAngle", "stopAngle").collect(Collectors.toSet());

    private static final int NEAREST_SITE_COUNT = 4;

    /* This is an expensive constructor when cells do not have radiuses in the input data */
    public GeometryTransformer(boolean isCoordinateRandomizedInUncertainty, List<Cell> cellList, String identifierFieldName) {
        this.isCoordinateRandomizedInUncertainty = isCoordinateRandomizedInUncertainty;
        this.identifierFieldName = identifierFieldName;
        this.radiuses = getOrCalculateRadius(cellList);
    }

    public GenericData.Record transformRow(GenericData.Record inputRecord) {
        GenericData.Record record = new GenericRecordBuilder(inputRecord).build();
        if (radiuses == null || radiuses.isEmpty()) {
            log.warn("No radiuses defined, geometry will not be added to record");
            return record;
        }
        String cellIdentifier;
        try {
            cellIdentifier = (String) record.get(identifierFieldName);
        }
        catch(ClassCastException e){
            cellIdentifier = String.valueOf(record.get(identifierFieldName));
        }
        Integer radius = radiuses.get(cellIdentifier);

        if (radius == null) {
            log.warn("Radius not found for record. Geometry will not be added to record");
            return record;
        }

        transformGeometryType(record, radius);
        trasformGeometryLatitudeLongitude(record, radius);

        // Methods below assume that geometryType has been set
        GeometryType type = GeometryType.valueOf((String) record.get("geometryType"));
        transformOrientationMajorAxis(record, type);
        transformInnerRadiuses(record, type);
        transformOuterRadiuses(record, type, radius);
        transformStartAngle(record, type);
        transformStopAngle(record, type);

        // Statistics handling
        for(String property: derivedFields){ // 0 - non-null; 1 - malformed values; null-values 2; zero-values 3;  0- min value; 1 - max value
            if(property.equals("geometryType")){
                String field = (String) record.get("geometryType");
                Set<String> set = Ldb4RdbConverter.uniqueStrings.get(property);
                set.add(field);
                Ldb4RdbConverter.uniqueStrings.put("geometryType", set);
                Integer[] stat = Ldb4RdbConverter.statsTable.get("geometryType");
                stat[0] += 1;
                Ldb4RdbConverter.statsTable.put("geometryType", stat);
            }
            else{
                try {
                    Float fieldValue = Float.parseFloat(String.valueOf(record.get(property)));
                    Integer[] stat = Ldb4RdbConverter.statsTable.get(property);
                    stat[0] += 1;
                    Ldb4RdbConverter.statsTable.put(property, stat);
                    Float[] stat2 = Ldb4RdbConverter.minMaxTable.get(property);
                    if(fieldValue < stat2[0]){
                        stat2[0] = fieldValue;
                    }
                    if(fieldValue > stat2[1]){
                        stat2[1] = fieldValue;
                    }
                    Ldb4RdbConverter.minMaxTable.put(property, stat2);
                } catch(NullPointerException e){
                    Integer[] stat = Ldb4RdbConverter.statsTable.get(property);
                    stat[2] += 1;
                    Ldb4RdbConverter.statsTable.put(property, stat);
                } catch(NumberFormatException e){
                    Integer[] stat = Ldb4RdbConverter.statsTable.get(property);
                    stat[1] += 1;
                    Ldb4RdbConverter.statsTable.put(property, stat);
                }
            }
        }
        return record;
    }

    public static void addGeometryJsonObjectsToList(JSONArray list) {
        list.add(JsonUtils.getDefaultNullableObject("geometryType", Schema.Type.STRING));
        list.add(JsonUtils.getDefaultNullableObject("geometryLatitude", Schema.Type.DOUBLE));
        list.add(JsonUtils.getDefaultNullableObject("geometryLongitude", Schema.Type.DOUBLE));
        list.add(JsonUtils.getDefaultNullableObject("orientationMajorAxis", Schema.Type.FLOAT));
        list.add(JsonUtils.getDefaultNullableObject("innerSemiMajorRadius", Schema.Type.FLOAT));
        list.add(JsonUtils.getDefaultNullableObject("innerSemiMinorRadius", Schema.Type.FLOAT));
        list.add(JsonUtils.getDefaultNullableObject("outerSemiMajorRadius", Schema.Type.FLOAT));
        list.add(JsonUtils.getDefaultNullableObject("outerSemiMinorRadius", Schema.Type.FLOAT));
        list.add(JsonUtils.getDefaultNullableObject("startAngle", Schema.Type.FLOAT));
        list.add(JsonUtils.getDefaultNullableObject("stopAngle", Schema.Type.FLOAT));
    }

    Map<String, Integer> getOrCalculateRadius(List<Cell> cellList) {
        System.out.println("Cell list size before filtering: " + cellList.size());
        List<Cell> filteredCells = filterCells(cellList);
        System.out.println("Cell list size after filtering: " + filteredCells.size());
        Map<String, Integer> distances = new HashMap<>();
        for (Cell cell : filteredCells) {
            if (cell.getRadius() != null) {
                distances.put(cell.getIdentifier(), cell.getRadius().intValue());
                continue;
            }
            int intersiteDistance = calculateIntersiteDistance(cell, filteredCells);
            cell.setRadius((float) intersiteDistance);
            distances.put(cell.getIdentifier(), intersiteDistance);
        }
        return distances;
    }

    private int calculateIntersiteDistance(Cell cell, List<Cell> filteredCells) {
        List<Double> distances = Lists.newArrayList();
        Coordinate site = cell.getCoordinate();
        for (Cell other : filteredCells) {
            Coordinate otherSite = other.getCoordinate();
            if (!otherSite.equals(site)) {
                distances.add(site.distanceTo(otherSite));
            }
        }
        return (int) average(distances);
    }

    private static double average(List<Double> numbers) {
        Collections.sort(numbers);
        if (NEAREST_SITE_COUNT < numbers.size()) {
            numbers = numbers.subList(0, NEAREST_SITE_COUNT);
        }

        return numbers.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }

    private List<Cell> filterCells(List<Cell> cellList) {
        return cellList.stream()
                .distinct()
                .collect(Collectors.toList());
    }

    private void transformStopAngle(GenericData.Record record, GeometryType type) {
        switch (type) {
            case POINT:
                record.put("stopAngle", null);
                break;
            case CIRCLE:
                record.put("stopAngle", 360);
                break;
            default:
                break;
        }
    }

    private void transformStartAngle(GenericData.Record record, GeometryType type) {
        switch (type) {
            case POINT:
                record.put("startAngle", null);
                break;
            case CIRCLE:
                record.put("startAngle", 0);
                break;
            default:
                break;
        }
    }

    private void transformInnerRadiuses(GenericData.Record record, GeometryType type) {
        switch (type) {
            case POINT:
            case CIRCLE:
            default:
                record.put("innerSemiMajorRadius", null);
                record.put("innerSemiMinorRadius", null);
                break;
        }
    }

    private void transformOrientationMajorAxis(GenericData.Record record, GeometryType type) {
        switch (type) {
            case POINT:
            case CIRCLE:
            default:
                record.put("orientationMajorAxis", null);
                record.put("orientationMajorAxis", null);
                break;
        }
    }

    private void transformOuterRadiuses(GenericData.Record record, GeometryType type, Integer radius) {
        switch (type) {
            case POINT:
                record.put("outerSemiMajorRadius", null);
                record.put("outerSemiMinorRadius", null);
                break;
            case CIRCLE:
                record.put("outerSemiMajorRadius", radius);
                record.put("outerSemiMinorRadius", radius);
                break;
            default:
                break;
        }
    }

    private void trasformGeometryLatitudeLongitude(GenericData.Record record, Integer radius) {
        Double latitude = (Double) record.get("lat");
        Double longitude = (Double) record.get("lon");
        record.put("geometryLatitude", latitude);
        record.put("geometryLongitude", longitude);
        if (isCoordinateRandomizedInUncertainty) {
            Coordinate randomizedPosition = randomizeCoordinateInUncertainty(radius, latitude, longitude);
            record.put("lat", randomizedPosition.getLatitude());
            record.put("lon", randomizedPosition.getLongitude());
        }
    }

    private Coordinate randomizeCoordinateInUncertainty(double radius, Double latitude, Double longitude) {
        Coordinate originalPosition = new Coordinate(latitude, longitude);
        double distance = ThreadLocalRandom.current().nextDouble(0d, radius + 1);
        double azimuth = ThreadLocalRandom.current().nextDouble(-180d, 180d);
        return originalPosition.directTo(azimuth, distance);
    }

    private void transformGeometryType(GenericData.Record record, Integer radius) {
        record.put("geometryType", radius > 0 ? GeometryType.CIRCLE.name() : GeometryType.POINT.name());
    }
}
