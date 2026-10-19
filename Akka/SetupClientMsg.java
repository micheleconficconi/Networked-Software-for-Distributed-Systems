package com.lab.evaluation25;

import akka.actor.ActorRef;

public class SetupClientMsg {
    private ActorRef balancer;

    public SetupClientMsg(ActorRef balancer) {
        this.balancer = balancer;
    }

    public ActorRef getBalancer() {
        return balancer;
    }
}
