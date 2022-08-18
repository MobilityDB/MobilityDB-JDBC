package com.mobilitydb.jdbc.boxes;

import java.io.Serializable;

public class Point implements Serializable {
    private Double x = null;
    private Double y = null;
    private Double z = null;

    public Point(){}

    public Point(Double x, Double y){
        this.x = x;
        this.y = y;
    }

    public Point(Double x, Double y, Double z){
        this.x = x;
        this.y = y;
        this.z = z;
    }

    protected Double getX() {
        return x;
    }

    protected void setX(Double x) {
        this.x = x;
    }

    protected Double getY() {
        return y;
    }

    protected void setY(Double y) {
        this.y = y;
    }

    protected Double getZ() {
        return z;
    }

    protected void setZ(Double z) {
        this.z = z;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Point) {
            Point other = (Point) obj;

            boolean xIsEqual;
            boolean yIsEqual;
            boolean zIsEqual;

            if (x != null && other.getX() != null) {
                xIsEqual = x.equals(other.getX());
            } else {
                xIsEqual = x == other.getX();
            }

            if (y != null && other.getY() != null) {
                yIsEqual = y.equals(other.getY());
            } else {
                yIsEqual = y == other.getY();
            }

            if (z != null && other.getZ() != null) {
                zIsEqual = z.equals(other.getZ());
            } else {
                zIsEqual = z == other.getZ();
            }

            return xIsEqual && yIsEqual && zIsEqual ;
        }
        return false;
    }

    @Override
    public int hashCode() {
        String value = String.format("%s %s %s",getX(),getY(),getZ());
        return value != null ? value.hashCode() : 0;
    }

}
