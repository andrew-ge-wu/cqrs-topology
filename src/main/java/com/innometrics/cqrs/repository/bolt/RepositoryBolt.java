package com.innometrics.cqrs.repository.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.innometrics.cache.Cache;
import com.innometrics.cache.guava.GuavaMemoryCache;
import com.innometrics.commons.model.Profile;
import com.innometrics.cqrs.repository.model.ProfileCommandMessage;
import com.innometrics.key.Key;
import com.innometrics.util.JacksonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import static com.innometrics.cqrs.repository.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class RepositoryBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryBolt.class);
    private OutputCollector collector;
    private Cache<Profile> profileStorage;
    private ConcurrentLinkedQueue<Key> storingQueue;
    private Thread writer;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELD_ID, FIELD_RESULT));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        //TODO: use elastic cache instead
        this.profileStorage = new GuavaMemoryCache<>(100000, Integer.MAX_VALUE, Cache.Strategy.EXPIRE_AFTER_ACCESS);
        this.storingQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void execute(Tuple input) {
        try {
            collector.ack(input);
            ProfileCommandMessage profileCommandMessage = ProfileCommandMessage.class.cast(input.getValueByField(FIELD_PAYLOAD));
            final Key key = profileCommandMessage.getKey();
            final Object payload = profileCommandMessage.getPayload() == null ? null : JacksonUtil.getObjectMapper().convertValue(profileCommandMessage.getPayload(), profileCommandMessage.getPayloadClass());
            Object toReturn = null;
            boolean writeToDB = false;
            switch (profileCommandMessage.getCommand()) {
                case MERGE:
                    if (payload instanceof Profile) {
                        toReturn = mergeProfile(key, Profile.class.cast(payload));
                    }
                    writeToDB = true;
                    break;
                case READ:
                    toReturn = readProfile(key);
                    break;
                case REPLACE:
                    if (payload instanceof Profile) {
                        toReturn = replaceProfile(key, Profile.class.cast(payload));
                    }
                    writeToDB = true;
                    break;
                default:
                    toReturn = "Fail to execute, unknown command:" + profileCommandMessage.getCommand();
                    break;
            }
            collector.emit(new Values(input.getValueByField(FIELD_ID), JacksonUtil.getObjectMapper().writeValueAsString(toReturn)));
            if (writeToDB) {
                if (!storingQueue.contains(key)) {
                    storingQueue.add(key);
                }
                if (writer == null || !writer.isAlive()) {
                    writer = new Thread(new Flusher());
                    writer.start();
                }
            }
        } catch (JsonProcessingException | ExecutionException e) {
            LOGGER.warn("Error in process", e);
        }
    }

    private Profile readProfile(Key objectId) throws ExecutionException {
        if (!profileStorage.contains(objectId)) {
            return null;
        } else {
            return profileStorage.get(objectId);
        }
    }

    private Profile mergeProfile(Key objectId, Profile profile) throws ExecutionException {
        Profile currentProfile = readProfile(objectId);
        //TODO:Merge profile here
        return profile;
    }

    private Profile replaceProfile(Key objectId, Profile profile) throws ExecutionException {
        profileStorage.put(objectId, profile);
        return profile;
    }

    private class Flusher implements Runnable {
        @Override
        public void run() {
            while (storingQueue.size() > 0) {
                Key objectId = storingQueue.poll();
                LOGGER.info("Storing profile" + objectId);

            }
        }
    }
}
