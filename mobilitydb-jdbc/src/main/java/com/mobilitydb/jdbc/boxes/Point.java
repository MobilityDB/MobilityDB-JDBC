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

    public Double getX() {
        return x;
    }

    public void setX(Double x) {
        this.x = x;
    }

    public Double getY() {
        return y;
    }

    public void setY(Double y) {
        this.y = y;
    }

    public Double getZ() {
        return z;
    }

    public void setZ(Double z) {
        this.z = z;
    }
}
