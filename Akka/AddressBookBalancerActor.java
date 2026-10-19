package com.lab.evaluation25;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AddressBookBalancerActor extends AbstractActor {

	ActorRef worker1 = null;
	ActorRef worker0 = null;
    final Duration timeout = Duration.create(1, TimeUnit.SECONDS);


    public AddressBookBalancerActor() {
	}

	@Override
	public Receive createReceive() {
		return idle();
	}

	private Receive idle() {
		return receiveBuilder()
				.match(SetupMsg.class, this::onSetup)
				.build();
	}

	private Receive normal() {
		return receiveBuilder()
				.match(PutMsg.class, this::storeEntry)
				.match(GetMsg.class, this::routeQuery)
				.build();
	}

	private void onSetup(SetupMsg msg) {
		worker0 = msg.getWorker0();
		worker1 = msg.getWorker1();
		getContext().become(normal());
	}

	int splitByInitial(String s) {
		char firstChar = s.charAt(0);

		// Normalize case for comparison
		char upper = Character.toUpperCase(firstChar);

		if (upper >= 'A' && upper <= 'M') {
			return 0;
		} else {
			return 1;
		}
	}

	void routeQuery(GetMsg msg) {
		
		System.out.println("BALANCER: Received query for name " + msg.getName());

		ReplyMsg response;
		try {
			// Checking for primary copy
			if (splitByInitial(msg.getName()) == 0) {
				Future<Object> future = Patterns.ask(worker0, msg, 1000);
				response = (ReplyMsg) Await.result(future, timeout);
			} else {
				Future<Object> future = Patterns.ask(worker1, msg, 1000);
				response = (ReplyMsg) Await.result(future, timeout);
			}
			sender().tell(response, self());
		} catch (TimeoutException | InterruptedException e0) {
			System.out.println("BALANCER: Primary copy query for name " + msg.getName() + " is resting!");

			try {
				// Checking for secondary copy
				if (splitByInitial(msg.getName()) == 0) {
					Future<Object> future = Patterns.ask(worker1, msg, 1000);
					response = (ReplyMsg) Await.result(future, timeout);
				} else {
					Future<Object> future = Patterns.ask(worker0, msg, 1000);
					response = (ReplyMsg) Await.result(future, timeout);
				}
				sender().tell(response, self());
			} catch (TimeoutException | InterruptedException e1) {
				System.out.println("BALANCER: Both copies are resting for name " + msg.getName() + "!");
				sender().tell(new TimeoutMsg(), self());
			}
		}
	}

	void storeEntry(PutMsg msg) {
		if(splitByInitial(msg.getName()) == 0) {
			// Send primary entry to worker0 and send secondary entry to worker1
			worker0.tell(new PutSplitMsg(msg.getName(), msg.getEmail(), 1), self());
			worker1.tell(new PutSplitMsg(msg.getName(), msg.getEmail(), 2), self());
		} else {
			// Send primary entry to worker1 and send secondary entry to worker0
			worker0.tell(new PutSplitMsg(msg.getName(), msg.getEmail(), 2), self());
			worker1.tell(new PutSplitMsg(msg.getName(), msg.getEmail(), 1), self());
		}
		System.out.println("BALANCER: Received new entry " + msg.getName() + " - " + msg.getEmail());
	}

	static Props props() {
		return Props.create(AddressBookBalancerActor.class);
	}

}
