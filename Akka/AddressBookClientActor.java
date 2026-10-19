package com.lab.evaluation25;

import static java.util.concurrent.TimeUnit.SECONDS;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.concurrent.TimeoutException;

public class AddressBookClientActor extends AbstractActor {

	private scala.concurrent.duration.Duration timeout = scala.concurrent.duration.Duration.create(3, SECONDS);
	private ActorRef balancer;

	@Override
	public Receive createReceive() {
		return idle();
	}

	public Receive idle() {
		return receiveBuilder()
				.match(SetupClientMsg.class, this::onSetup)
				.build();
	}

	public Receive normal() {
		return receiveBuilder()
				.match(PutMsg.class, this::putEntry)
				.match(GetMsg.class, this::query)
				.build();
	}

	private void onSetup(SetupClientMsg msg) {
		balancer = msg.getBalancer();
		getContext().become(normal());
	}

	void putEntry(PutMsg msg) {
		System.out.println("CLIENT: Sending new entry " + msg.getName() + " - " + msg.getEmail());
		balancer.tell(msg, getSelf());
	}

	void query(GetMsg msg) {
		System.out.println("CLIENT: Issuing query for " + msg.getName());

		try {
			Future<Object> future = Patterns.ask(balancer, msg, 3000);
			ReplyMsg response = (ReplyMsg) Await.result(future, timeout);

			if(response.getEmail() == null) {
				System.out.println("CLIENT: Received reply, no email found!");
			} else {
				System.out.println("CLIENT: Received reply!");
			}
		} catch (ClassCastException e0) {
			// received TimeoutMsg instead of ReplyMsg
			System.out.println("CLIENT: Received timeout, both copies are resting!");
		} catch (TimeoutException | InterruptedException  e1) {
			e1.printStackTrace();
		}
	}

	static Props props() {
		return Props.create(AddressBookClientActor.class);
	}

}
