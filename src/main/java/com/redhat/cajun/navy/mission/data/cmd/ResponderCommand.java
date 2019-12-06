package com.redhat.cajun.navy.mission.data.cmd;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.redhat.cajun.navy.mission.data.Responder;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ResponderCommand {
    private String id;
    private String messageType;
    private String invokingService;
    private long timestamp;
    Responder responder = null;

    public ResponderCommand(Responder responder, String messageType) {
        id = UUID.randomUUID().toString();
        this.responder = responder;
        this.messageType = messageType;
        this. invokingService = "MissionService";
        this.timestamp = System.currentTimeMillis();
    }


    public String getResponderCommand(boolean available, boolean enrolled) {

        return "{ \"messageType\": \""+messageType+"\", " +
                "\"id\": \""+id+"\", " +
                "\"invokingService\": \""+invokingService+"\", " +
                "\"timestamp\": "+timestamp+", " +
                "\"body\": { \"responder\": { \"id\": "+responder.getResponderId()+", " +
                "\"latitude\": "+responder.getLat()+", " +
                "\"longitude\": "+responder.getLon()+", " +
                "\"enrolled\": " + enrolled + ", " +
                "\"available\": "+available+" } } }";
    }

}
