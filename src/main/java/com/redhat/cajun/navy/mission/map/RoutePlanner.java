package com.redhat.cajun.navy.mission.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mapbox.api.directions.v5.DirectionsCriteria;
import com.mapbox.api.directions.v5.MapboxDirections;
import com.mapbox.api.directions.v5.MapBoxSimulatorDirections;
import com.mapbox.api.directions.v5.models.DirectionsResponse;
import com.mapbox.api.directions.v5.models.DirectionsRoute;
import com.mapbox.api.directions.v5.models.LegStep;
import com.mapbox.api.directions.v5.models.RouteLeg;
import com.mapbox.geojson.Point;
import com.redhat.cajun.navy.mission.data.Location;
import com.redhat.cajun.navy.mission.data.MissionStep;
import retrofit2.Response;
import rx.Observable;


public class RoutePlanner {

    private String MAPBOX_ACCESS_TOKEN = null;

    private String mapboxBaseUrl;

    private boolean hasWayPoint = false;
    public RoutePlanner(String MAPBOX_ACCESS_TOKEN, String mapboxBaseUrl) {
        this.MAPBOX_ACCESS_TOKEN = MAPBOX_ACCESS_TOKEN;
        this.mapboxBaseUrl = mapboxBaseUrl;
    }

    public List<MissionStep> getMapboxDirectionsRequest(Location origin, Location destination, Location waypoint) {

        MapboxDirections request = MapBoxSimulatorDirections.builder()
                .baseUrl(mapboxBaseUrl)
                .accessToken(MAPBOX_ACCESS_TOKEN)
                .origin(Point.fromLngLat(origin.getLong(), origin.getLat()))
                .destination(Point.fromLngLat(destination.getLong(), destination.getLat()))
                .addWaypoint(Point.fromLngLat(waypoint.getLong(), waypoint.getLat()))
                .overview(DirectionsCriteria.OVERVIEW_FULL)
                .profile(DirectionsCriteria.PROFILE_DRIVING)
                .steps(true)
                .build();
        System.out.println(request);
        List<MissionStep> missionSteps = new ArrayList<>();
        try {

            Response<DirectionsResponse> response = request.executeCall();

            if (response.body() == null) {
                System.err.println("No routes found check access token, rights and coordinates");
            } else if (response.body().routes().size() < 1) {
                System.err.println("No routes found!");
            } else {

                if (response.body().routes().size() > 0) {
                    DirectionsRoute route = response.body().routes().get(0);

                    List<RouteLeg> legs = route.legs();

                    Observable.from(legs).map(l -> {
                        List<LegStep> steps = l.steps();
                        Observable.from(steps).map(s -> {
                            MissionStep step = new MissionStep(s.maneuver().location());
                            if (s.maneuver().type().equalsIgnoreCase("arrive")) {
                                if (!hasWayPoint) {
                                    step.setWayPoint(true);
                                    hasWayPoint = true;
                                } else
                                    step.setDestination(true);


                            }
                            missionSteps.add(step);
                            return step;
                        }).subscribe();
                        return steps;
                    }).subscribe();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return missionSteps;
    }

}
