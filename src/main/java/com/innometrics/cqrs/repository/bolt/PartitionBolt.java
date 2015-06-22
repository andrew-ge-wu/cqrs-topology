package com.innometrics.cqrs.repository.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.innometrics.cqrs.repository.model.ProfileCommandMessage;
import com.innometrics.util.JacksonUtil;

import java.io.IOException;
import java.util.Map;

import static com.innometrics.cqrs.repository.Constants.*;

/**
 * @author andrew, Innometrics
 */
public class PartitionBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            ProfileCommandMessage profileCommandMessage = JacksonUtil.getObjectMapper().readValue(tuple.getString(1), ProfileCommandMessage.class);
            String partition = profileCommandMessage.getKey().toString();
            collector.emit(tuple, new Values(tuple.getValue(0), partition, profileCommandMessage));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELD_ID, FIELD_PARTITION, FIELD_PAYLOAD));
    }
}
