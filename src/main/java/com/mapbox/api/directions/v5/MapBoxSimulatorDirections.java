package com.mapbox.api.directions.v5;

public class MapBoxSimulatorDirections {

    public static MapboxDirections.Builder builder() {
        return new AutoValue_MapboxDirections.Builder()
                .profile(DirectionsCriteria.PROFILE_DRIVING)
                .user(DirectionsCriteria.PROFILE_DEFAULT_USER)
                .geometries(DirectionsCriteria.GEOMETRY_POLYLINE6);
    }

}
