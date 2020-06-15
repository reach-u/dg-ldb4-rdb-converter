package com.demograft.ldb4rdbconverter.geometry;

import avro.shaded.com.google.common.collect.Lists;
import com.demograft.ldb4rdbconverter.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.json.simple.JSONArray;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class GeometryTransformer {
    private final boolean isCoordinateRandomizedInUncertainty;
    private final String identifierFieldName;

    private final Map<String, Integer> radiuses;

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
        String cellIdentifier = (String) record.get(identifierFieldName);
        Integer radius = radiuses.get(cellIdentifier);

        if (radius == null) {
            log.warn("Radius not found for record. Geometry will not be added to record");
            return record;
        }

        transformGeometryType(record, radius);
        trasformGeometryLatitudeLongitude(record);

        // Methods below assume that geometryType has been set
        GeometryType type = GeometryType.valueOf((String) record.get("geometryType"));
        transformOrientationMajorAxis(record, type);
        transformInnerRadiuses(record, type);
        transformOuterRadiuses(record, type, radius);
        transformStartAngle(record, type);
        transformStopAngle(record, type);
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
        List<Cell> filteredCells = filterCells(cellList);
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

    private void trasformGeometryLatitudeLongitude(GenericData.Record record) {
        if (isCoordinateRandomizedInUncertainty) {
            // TODO: Randomize latitude and longitude
        } else {
            record.put("geometryLatitude", record.get("lat"));
            record.put("geometryLongitude", record.get("lon"));
        }
    }

    private void transformGeometryType(GenericData.Record record, Integer radius) {
        record.put("geometryType", radius > 0 ? GeometryType.CIRCLE.name() : GeometryType.POINT.name());
    }
}
