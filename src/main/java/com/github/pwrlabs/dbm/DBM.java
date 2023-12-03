package com.github.pwrlabs.dbm;


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
    }

    public boolean store(String valueName, Object value) {
        if(value instanceof Number) {
            store(valueName, (Number) value);
        }
        else if(value instanceof String) {
            store(valueName, value.toString());
        }
        else if (value instanceof byte[]){
        	store(valueName, (byte[]) value);
        }

        return true;
    }

    public boolean store(String valueName, byte[] value) {
        String path = rootPath + valueName;

        // Create parent directories if necessary
        try {
            Files.createDirectories(Paths.get(path).getParent());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(path))) {
            bos.write(value);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean store(String valueName, String value) {
    	if(value == null) return true;
        return store(valueName, value.getBytes(StandardCharsets.UTF_8));
    }

    public boolean store(String valueName, Number value) {
    	if(value == null) return true;
    	return store(valueName, getByteArrayValue(value));
    }

    public boolean store(String valueName, Boolean value) {
    	if(value == null) return true;
    	return store(valueName, new byte[] { value ? (byte)1 : (byte)0 });
    }

    public byte[] load(String valueName) {
        String path = rootPath + valueName;
        if (!Files.exists(Paths.get(path))) return null;

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(path))) {
            List<Byte> byteList = new ArrayList<>();
            int byteRead;
            while ((byteRead = bis.read()) != -1) {
                byteList.add((byte) byteRead);
            }

            byte[] data = new byte[byteList.size()];
            for (int i = 0; i < byteList.size(); i++) {
                data[i] = byteList.get(i);
            }

            return data;
        } catch (IOException e) {
            return null;
        }
    }

    public String loadString(String valueName) {
        byte[] value = load(valueName);
        return value != null ? new String(value, StandardCharsets.UTF_8) : null;
    }

    public Short loadShort(String valueName) {
        byte[] value = load(valueName);
        return value != null ? (short) getNumberValue(value) : 0;
    }

    public Integer loadInt(String valueName) {
        byte[] value = load(valueName);
        return value != null ? (Integer) getNumberValue(value) : 0;
    }

    public double loadDouble(String valueName) {
        byte[] value = load(valueName);
        return value != null ? ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getDouble() : 0;
    }

    public boolean loadBoolean(String valueName) {
        byte[] value = load(valueName);
        return value != null ? value[0] != 0 : false;
    }

    public Long loadLong(String valueName) {
        byte[] value = load(valueName);
        return value != null ? (Long) getNumberValue(value) : 0;
    }

    public BigInteger loadBigInt(String valueName) {
        byte[] value = load(valueName);
        return value != null ? new BigInteger(1, value) : BigInteger.ZERO;
    }

    public BigDecimal loadBigDec(String valueName) {
        byte[] value = load(valueName);
        return value != null ? (BigDecimal) getNumberValue(value) : BigDecimal.ZERO;
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

    public static byte[] getByteArrayValue(Number value) {
        if (value instanceof Integer) {
            return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                    .putInt((int)value).array();
        } else if (value instanceof Short) {
            return ByteBuffer.allocate(Short.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                    .putShort((short)value).array();
        } else if (value instanceof Long) {
            return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                    .putLong((long)value).array();
        } else if (value instanceof BigInteger) {
            byte[] unscaledByteArray = ((BigInteger)value).toByteArray();
            return ByteBuffer.allocate(unscaledByteArray.length + 4).order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(0)
                    .put(unscaledByteArray)
                    .array();
        } else if (value instanceof BigDecimal) {
            BigInteger unscaledValue = ((BigDecimal)value).unscaledValue();
            byte[] unscaledByteArray = unscaledValue.toByteArray();
            return ByteBuffer.allocate(unscaledByteArray.length + 4).order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(((BigDecimal)value).scale())
                    .put(unscaledByteArray)
                    .array();
        } else if (value instanceof Double) {
            return ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                    .putDouble((double)value).array();
        } else {
            throw new IllegalArgumentException("Unsupported numeric type: " + value.getClass());
        }
    }

    public static Number getNumberValue(byte[] byteArray) {
        ByteBuffer buffer = ByteBuffer.wrap(byteArray).order(ByteOrder.LITTLE_ENDIAN);
        if (byteArray.length == Integer.BYTES) {
            return buffer.getInt();
        } else if (byteArray.length == Short.BYTES) {
            return buffer.getShort();
        } else if (byteArray.length == Long.BYTES) {
            return buffer.getLong();
        } else if (byteArray.length > 4) {
            int scale = buffer.getInt();
            byte[] unscaledByteArray = Arrays.copyOfRange(byteArray, 4, byteArray.length);
            BigInteger unscaledValue = new BigInteger(1, unscaledByteArray);
            return new BigDecimal(unscaledValue, scale);
        } else if (byteArray.length == Double.BYTES) {
            return buffer.getDouble();
        } else {
            throw new IllegalArgumentException("Invalid byte array length: " + byteArray.length);
        }
    }


}