package com.redhat.cajun.navy.mission.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.redhat.cajun.navy.mission.util.DoubleContextualSerializer;
import com.redhat.cajun.navy.mission.util.Precision;
import io.vertx.core.json.Json;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Mission {
    private String id;
    private String incidentId;
    private String responderId;

    @JsonSerialize(using = DoubleContextualSerializer.class)
    @Precision(precision = 4)
    private double responderStartLat;

    @JsonSerialize(using = DoubleContextualSerializer.class)
    @Precision(precision = 4)
    private double responderStartLong;

    @JsonSerialize(using = DoubleContextualSerializer.class)
    @Precision(precision = 4)
    private double incidentLat;

    @JsonSerialize(using = DoubleContextualSerializer.class)
    @Precision(precision = 4)
    private double incidentLong;

    @JsonSerialize(using = DoubleContextualSerializer.class)
    @Precision(precision = 4)
    private double destinationLat;

    @JsonSerialize(using = DoubleContextualSerializer.class)
    @Precision(precision = 4)
    private double destinationLong;

    private List<ResponderLocationHistory> responderLocationHistory;
    private String status;

    private List<MissionStep> steps = null;

    public Mission(){
        id = UUID.randomUUID().toString();
        responderLocationHistory = new ArrayList<>();
        steps = new ArrayList<MissionStep>();
    }

    public String getId() {
        return id;
    }

    public void setId(String input) {
        this.id = input;
    }

    public String getIncidentId() {
        return incidentId;
    }

    public void setIncidentId(String input) {
        this.incidentId = input;
    }

    public String getResponderId() {
        return responderId;
    }

    public void setResponderId(String input) {
        this.responderId = input;
    }

    public double getResponderStartLat() {
        return responderStartLat;
    }

    public void setResponderStartLat(double input) {
        this.responderStartLat = input;
    }

    public double getResponderStartLong() {
        return responderStartLong;
    }

    public void setResponderStartLong(double input) {
        this.responderStartLong = input;
    }

    public double getIncidentLat() {
        return incidentLat;
    }

    public void setIncidentLat(double input) {
        this.incidentLat = input;
    }

    public double getIncidentLong() {
        return incidentLong;
    }

    public void setIncidentLong(double input) {
        this.incidentLong = input;
    }

    public double getDestinationLat() {
        return destinationLat;
    }

    public void setDestinationLat(double input) {
        this.destinationLat = input;
    }

    public double getDestinationLong() {
        return destinationLong;
    }

    public void setDestinationLong(double input) {
        this.destinationLong = input;
    }

    public List<ResponderLocationHistory> getResponderLocationHistory() {
        return responderLocationHistory;
    }

    public void setResponderLocationHistory(List<ResponderLocationHistory> input) {
        this.responderLocationHistory = input;
    }

    public void addResponderLocationHistory(ResponderLocationHistory history){
        responderLocationHistory.add(history);
        setResponderStartLat(history.getLat());
        setResponderStartLong(history.getLon());

    }
    public String getStatus() {
        return status;
    }

    public void setStatus(String input) {
        this.status = input;
    }

    public List<MissionStep> getSteps() {
        return steps;
    }

    public void setSteps(List<MissionStep> steps) {
        this.steps = steps;
    }

    public void addMissionStep(MissionStep step){
        if(this.steps != null && step != null){
            steps.add(step);
        }
        else throw new IllegalArgumentException("Null value not acceptable");
    }

    public String toJson() {
        return Json.encode(this);
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Mission mission = (Mission) o;
        return Objects.equals(responderId, mission.responderId) && Objects.equals(incidentId, mission.incidentId);
    }

    @JsonIgnore
    public String getKey(){
        return this.incidentId+this.responderId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(responderId+incidentId);
    }
}
