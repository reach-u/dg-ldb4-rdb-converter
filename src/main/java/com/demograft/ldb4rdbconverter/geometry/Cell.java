package com.demograft.ldb4rdbconverter.geometry;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericData;

import java.util.Objects;

@Data
@NoArgsConstructor
public class Cell {
    private String identifier;
    private Coordinate coordinate;
    private Float radius;
    private double locationEqualityTolerance;

    public Cell(GenericData.Record record, String identifierField, double locationEqualityTolerance) {
        try {
            this.identifier = (String) record.get(identifierField);
        }
        catch(Exception e){
            try{
                this.identifier = String.valueOf(record.get(identifierField));
            }
            catch(Exception e1){
                this.identifier = "unknown";
            }
        }
        this.radius = (Float) record.get("radius");
        this.coordinate = new Coordinate((Double) record.get("lat"), (Double) record.get("lon"));
        this.locationEqualityTolerance = locationEqualityTolerance;
    }

    /*
    Calculates fuzzy equals using the given tolerance
    on the latitude and longitude fields
    */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cell cell = (Cell) o;
        return Objects.equals(identifier, cell.identifier) &&
                Math.abs(this.coordinate.getLatitude() - cell.getCoordinate().getLatitude()) <= locationEqualityTolerance &&
                Math.abs(this.coordinate.getLongitude() - cell.coordinate.getLongitude()) <= locationEqualityTolerance;
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, coordinate, radius, locationEqualityTolerance);
    }
}
