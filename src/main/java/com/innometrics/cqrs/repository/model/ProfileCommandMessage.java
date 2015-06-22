package com.innometrics.cqrs.repository.model;

import com.innometrics.key.ProfileKey;

/**
 * @author andrew, Innometrics
 */
public class ProfileCommandMessage<T> {
    public static enum Command {READ, MERGE, REPLACE}

    private Command command;
    private ProfileKey key;
    private T payload;
    private Class<T> payloadClass;

    private ProfileCommandMessage() {
    }

    public ProfileCommandMessage(Command command, ProfileKey key, T payload, Class<T> payloadClass) {
        this.command = command;
        this.key = key;
        this.payload = payload;
        this.payloadClass = payloadClass;
    }

    public Command getCommand() {
        return command;
    }

    public ProfileKey getKey() {
        return key;
    }

    public T getPayload() {
        return payload;
    }

    public Class<T> getPayloadClass() {
        return payloadClass;
    }
}
