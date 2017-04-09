package edu.buffalo.cse.cse486586.simpledht;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by aneesh on 4/9/17.
 */

public class Request {
    private String type;
    private String originalPort;
    private String response;
    private static String[] jsonFields = {"type","originalPort", "response"};

    Request(String type, String originalPort){
        this.type = type;
        this.originalPort = originalPort;
        this.response = "";
    }

    Request(String json){
        try{
            JSONObject jsonObject = new JSONObject(json);
            this.type = jsonObject.getString(jsonFields[0]);
            this.originalPort = jsonObject.getString(jsonFields[1]);
            this.response = jsonObject.getString(jsonFields[2]);
        }catch (JSONException e){
            e.printStackTrace();
        }
    }

    public String getType() {
        return type;
    }

    public String getResponse(){
        return response;
    }

    public String getOriginalPort() {
        return originalPort;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setResponse(String response){
        this.response = response;
    }

    public String getJson(){
        JSONObject jsonObject = new JSONObject();
        try{
            jsonObject.put(jsonFields[0],type);
            jsonObject.put(jsonFields[1],originalPort);
            jsonObject.put(jsonFields[2],response);
        }catch(JSONException e){
            e.printStackTrace();
            return null;
        }
        return jsonObject.toString();
    }


}
