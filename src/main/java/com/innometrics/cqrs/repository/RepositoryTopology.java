package com.innometrics.cqrs.repository;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.innometrics.commons.model.Profile;
import com.innometrics.cqrs.repository.Constants.Mode;
import com.innometrics.cqrs.repository.bolt.PartitionBolt;
import com.innometrics.cqrs.repository.bolt.RepositoryBolt;
import com.innometrics.cqrs.repository.builder.DRPCTopologyBuilder;
import com.innometrics.cqrs.repository.model.ProfileCommandMessage;
import com.innometrics.key.ProfileKey;
import com.innometrics.util.JacksonUtil;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * @author andrew, Innometrics
 */
public class RepositoryTopology {
    private final LocalDRPC drpcServer;
    private final LocalCluster cluster;

    public RepositoryTopology(Mode mode) {
        switch (mode) {
            case LOCAL:
                this.drpcServer = new LocalDRPC();
                this.cluster = new LocalCluster();
                getCluster().submitTopology("drpc-demo", getConfig(), getTopology(mode));
                break;
            case REMOTE:
                drpcServer = null;
                this.cluster = null;
                break;
            default:
                drpcServer = new LocalDRPC();
                this.cluster = new LocalCluster();
                break;
        }
    }

    public LocalDRPC getDrpcServer() {
        return drpcServer;
    }

    public LocalCluster getCluster() {
        return cluster;
    }

    public Config getConfig() {
        Config toReturn = new Config();
        toReturn.setNumWorkers(5);
        toReturn.registerSerialization(ProfileCommandMessage.class);
        toReturn.registerSerialization(ProfileKey.class);
        return toReturn;
    }

    public StormTopology getTopology(Mode mode) {
        StormTopology toReturn = null;
        DRPCTopologyBuilder topologyBuilder = new DRPCTopologyBuilder(getClass().getName());
        switch (mode) {
            case LOCAL:
                topologyBuilder.addBolt(new PartitionBolt(), 2);
                topologyBuilder.addBolt(new RepositoryBolt(), 4).fieldsGrouping(new Fields(Constants.FIELD_PARTITION));
                toReturn = topologyBuilder.createLocalTopology(getDrpcServer());
                break;
            case REMOTE:
                break;
            default:
                break;
        }
        return toReturn;
    }

    public static void main(String[] args) throws JsonProcessingException {
        RepositoryTopology repositoryTopology = new RepositoryTopology(Mode.LOCAL);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 3000; i++) {
            Profile testProfile = new Profile();
            testProfile.setId(RandomStringUtils.randomAlphanumeric(12));
            ProfileCommandMessage<Profile> toExecute = new ProfileCommandMessage<>(ProfileCommandMessage.Command.REPLACE, new ProfileKey("companyBuckert", testProfile.getId()), testProfile, Profile.class);
            repositoryTopology.getDrpcServer().execute(RepositoryTopology.class.getName(), JacksonUtil.getObjectMapper().writeValueAsString(toExecute));
            toExecute = new ProfileCommandMessage<>(ProfileCommandMessage.Command.READ, new ProfileKey("companyBuckert", testProfile.getId()), null, Profile.class);
            String result = repositoryTopology.getDrpcServer().execute(RepositoryTopology.class.getName(), JacksonUtil.getObjectMapper().writeValueAsString(toExecute));
            // System.out.println("Print!!!" + result);

        }
        System.out.println("rps:" + (1000 / ((System.currentTimeMillis() - start) / 6000)));
        repositoryTopology.cluster.shutdown();
        repositoryTopology.drpcServer.shutdown();
    }
}
