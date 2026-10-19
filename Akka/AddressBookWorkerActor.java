package com.lab.evaluation25;

import java.util.HashMap;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class AddressBookWorkerActor extends AbstractActor {

	private HashMap<String, String> primaryAddresses;
	private HashMap<String, String> replicaAddresses;

	public AddressBookWorkerActor() {
		this.primaryAddresses = new HashMap<String, String>();
		this.replicaAddresses = new HashMap<String, String>();
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
                .match(GetMsg.class, this::generateReply)
                .match(PutSplitMsg.class, this::putEntry)
                .match(RestMsg.class, this::onRestMsg)
                .build();
	}

    public Receive restingBehaviour(){
        // We choose to handle the put entry message even in resting behaviour
        return receiveBuilder()
                .match(ResumeMsg.class, this::onResumeMsg)
                .match(PutSplitMsg.class, this::putEntry)
                .build();
    }
	
	void generateReply(GetMsg msg) {
        if(primaryAddresses.containsKey(msg.getName())){
            sender().tell(new ReplyMsg(primaryAddresses.get(msg.getName())), self());
        }
        else if(replicaAddresses.containsKey(msg.getName())){
            sender().tell(new ReplyMsg(replicaAddresses.get(msg.getName())), self());
        }
        else{
            // We are using null to tell the balancer that the name is not present
            sender().tell(new ReplyMsg(null), self());
        }
        System.out.println(this.toString() + ": Received query for name " + msg.getName());
	}

    //1 if primary and 2 if secondary storage
    void putEntry(PutSplitMsg msg) {
        if(msg.getStorage() == 1){
            primaryAddresses.put(msg.getName(), msg.getEmail());
            System.out.println("Added to primary " + msg.getName() + " " + msg.getEmail()+ " " + getSelf().path().name());
        }
        else{
            System.out.println("Added to secondary " + msg.getName() + " " + msg.getEmail() + " " + getSelf().path().name());
            replicaAddresses.put(msg.getName(), msg.getEmail());
        }
    }

    void onRestMsg(RestMsg msg){
        System.out.println("going to rest ... " + getSelf().path().name());
        this.getContext().become(this.restingBehaviour());
    }

    void onResumeMsg(ResumeMsg msg){
        System.out.println("resuming ... " + getSelf().path().name());
        this.getContext().become(this.createReceive());
    }

	static Props props() {
		return Props.create(AddressBookWorkerActor.class);
	}
}
