package edu.buffalo.cse.cse486586.simpledht;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by aneesh on 4/9/17.
 */

public class Request {
    private static String[] jsonFields = {"type", "originalPort", "response"};
    private String type;
    private String originalPort;
    private String response;

    public Request() {
        this.type = this.originalPort = this.response = null;
    }

    public Request(String type, String originalPort, String response) {
        this.type = type;
        this.originalPort = originalPort;
        this.response = response;
    }

    public Request(String type, String originalPort) {
        this.type = type;
        this.originalPort = originalPort;
        this.response = "";
    }

    public Request(String json) {
        try {
            JSONObject jsonObject = new JSONObject(json);
            this.type = jsonObject.getString(jsonFields[0]);
            this.originalPort = jsonObject.getString(jsonFields[1]);
            this.response = jsonObject.getString(jsonFields[2]);
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public String getOriginalPort() {
        return originalPort;
    }

    public String getJson() {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(jsonFields[0], type);
            jsonObject.put(jsonFields[1], originalPort);
            jsonObject.put(jsonFields[2], response);
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
        return jsonObject.toString();
    }


}
