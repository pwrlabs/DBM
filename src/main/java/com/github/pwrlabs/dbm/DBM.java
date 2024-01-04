package com.github.pwrlabs.dbm;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DBM {

    public final String id;
    public final String dataFile;

    public DBM(String id) {
        this.id = id;

        String rootPath = "database/" + this.getClass().getSimpleName() + "/";
        this.dataFile = rootPath + id + "-data.json";

        File dir = new File(rootPath);
        if(!dir.exists()) {
            dir.mkdirs();
        }

        File file = new File(dataFile);
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to create file: " + dataFile);
            }
        }

    }

    public JSONObject getDataFile() {
        try {
            JSONObject data = (JSONObject) TimedCache.get(dataFile);

            if(data == null) {
                String dataStr = Files.readString(Paths.get(dataFile));
                if(dataStr == null || dataStr.isEmpty()) return new JSONObject();
                data = new JSONObject(dataStr);
                TimedCache.put(dataFile, data);
            }

            return data;
        } catch (IOException e) {
            e.printStackTrace();
            return new JSONObject();
        }
    }

    public boolean store(Object... namesAndValues) {
        if(namesAndValues.length % 2 != 0) throw new RuntimeException("Names and values must be in pairs");

        JSONObject json = getDataFile();

        for(int t=0; t < namesAndValues.length; t+= 2) {
            if(namesAndValues[t+1] == null) continue;
            json.put(namesAndValues[t].toString(), namesAndValues[t+1].toString());
        }

        try {
            TimedCache.put(dataFile, json);
            Files.writeString(Paths.get(dataFile), json.toString());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    public byte[] load(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return null;
        if(!data.has(valueName)) return null;
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

    public JSONObject loadJSON(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return new JSONObject();
        if(!data.has(valueName)) {
            return new JSONObject();
        } else {
            return new JSONObject(data.getString(valueName));
        }
    }

    public JSONArray loadJSONArray(String valueName) {
        JSONObject data = getDataFile();
        if(data == null) return new JSONArray();
        if(!data.has(valueName)) {
            return new JSONArray();
        } else {
            return new JSONArray(data.getString(valueName));
        }
    }

    // Returns creation time of the folder holding the data of this class/The
    // creation time of the class
    public long getCreationTime() {
        Path path = Paths.get(dataFile);

        try {
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
            return attrs.creationTime().toInstant().getEpochSecond();
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public void delete(String valueName) {
        File file = new File(dataFile);

        file.delete();
    }

    public void deleteObject() {
        File directory = new File(dataFile);

        // Make sure the file or directory exists and is not null
        if (directory != null && directory.exists()) {
            deleteDirectory(directory);
        }
    }

    private void deleteDirectory(File file) {
        File[] contents = file.listFiles(); // List all the directory contents
        if (contents != null) {
            for (File f : contents) {
                if (f.isDirectory()) {
                    deleteDirectory(f); // Recursive call for sub-directories
                } else {
                    f.delete(); // Delete files
                }
            }
        }
        file.delete(); // Delete the directory (now empty)
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
                String id = key.replace("-data.json", "");
                System.out.println("id: " + id);
                constructor.newInstance(id);
                System.out.println("Loaded " + c.getSimpleName() + " Object");
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

}