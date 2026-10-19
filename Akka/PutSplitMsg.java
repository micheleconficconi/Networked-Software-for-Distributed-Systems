package com.lab.evaluation25;

public class PutSplitMsg {
    private String name;
    private String email;
    private int storage;

    public PutSplitMsg (String name, String email, int storage) {
        this.name = name;
        this.email = email;
        this.storage = storage;
    }

    public String getName() {
        return name;
    }

    public String getEmail() {
        return email;
    }

    public int getStorage() {
        return storage;
    }
}
