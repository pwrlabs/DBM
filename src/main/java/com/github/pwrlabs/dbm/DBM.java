package com.github.pwrlabs.dbm;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DBM {

    public final String id;
    public final String rootPath;

    public DBM(String id) {
        this.id = id;

        rootPath = "database/" + this.getClass().getSimpleName() + "/" + id + "/";
        try {
            Files.createDirectories(Paths.get(rootPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean store(Object... namesAndValues) {
        if(namesAndValues.length % 2 != 0) throw new RuntimeException("Names and values must be in pairs");

        JSONObject json = new JSONObject();

        File jsonFile = new File(rootPath + "data.json");
        if(jsonFile.exists()) {
            try {
                json = new JSONObject(Files.readString(Paths.get(rootPath + "data.json")));
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        for(int t=0; t < namesAndValues.length; t+= 2) {
            if(namesAndValues[t+1] == null) continue;
            json.put(namesAndValues[t].toString(), namesAndValues[t+1].toString());
        }

        try {
            Files.writeString(Paths.get(rootPath + "data.json"), json.toString());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    public JSONObject getDataFile() {
        File jsonFile = new File(rootPath + "data.json");
        if(jsonFile.exists()) {
            try {
                return new JSONObject(Files.readString(Paths.get(rootPath + "data.json")));
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        } else {
            return null;
        }
    }

    public byte[] load(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return null;
        return Hex.decode(data.getString(valueName));
    }

    public String loadString(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return null;
        if(!data.has(valueName)) {
            return null;
        } else {
            return data.getString(valueName);
        }
    }

    public Short loadShort(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return 0;
        if(!data.has(valueName)) {
            return 0;
        } else {
            return Short.parseShort(data.getString(valueName));
        }
    }

    public Integer loadInt(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return 0;
        if(!data.has(valueName)) {
            return 0;
        } else {
            return Integer.parseInt(data.getString(valueName));
        }
    }

    public double loadDouble(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return 0;
        if(!data.has(valueName)) {
            return 0;
        } else {
            return Double.parseDouble(data.getString(valueName));
        }
    }

    public boolean loadBoolean(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return false;
        if(!data.has(valueName)) {
            return false;
        } else {
            return Boolean.parseBoolean(data.getString(valueName));
        }
    }

    public Long loadLong(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return 0L;
        if(!data.has(valueName)) {
            return 0L;
        } else {
            return Long.parseLong(data.getString(valueName));
        }
    }

    public BigInteger loadBigInt(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return BigInteger.ZERO;
        if(!data.has(valueName)) {
            return BigInteger.ZERO;
        } else {
            return new BigInteger(data.getString(valueName));
        }
    }

    public BigDecimal loadBigDec(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return BigDecimal.ZERO;
        if(!data.has(valueName)) {
            return BigDecimal.ZERO;
        } else {
            return new BigDecimal(data.getString(valueName));
        }
    }

    // Returns creation time of the folder holding the data of this class/The
    // creation time of the class
    public long getCreationTime() {
        Path path = Paths.get(rootPath);

        try {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
            return attrs.creationTime().toInstant().getEpochSecond();
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public void delete(String valueName) {
        String path = rootPath + valueName;
        File file = new File(path);

        file.delete();
    }

    public void deleteAll() {

        File folder = new File(rootPath);

        for (File file : folder.listFiles()) {
            file.delete();
        }

        folder.delete();
    }

    public static <T> void loadAllObjectsFromDatabase(Class<T> c) throws NoSuchMethodException, SecurityException {

        // Get the constructor object for the Person class that takes a String and an
        // int as arguments
        Constructor<T> constructor = c.getConstructor(String.class);

        String dirPath = ("database/" + c.getSimpleName());
        File dir = new File(dirPath);

        if (!dir.exists()) {
            System.out.println("No dir found for " + c.getSimpleName());
            return;
        }

        for (String key : dir.list()) {
            try {
                System.out.println("key: " + key);
                constructor.newInstance(key);
                System.out.println("Loaded " + c.getSimpleName() + " Object");
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

}