package org.petuum.ps.config;

import org.petuum.ps.common.util.ByteBufferInputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * Created by aqiao on 2/24/15.
 */
public class Config {

    private Properties properties;

    public Config() {
        this.properties = new Properties();
    }

    public Config(Config other) {
        this.properties = new Properties();
        this.properties.putAll(other.properties);
    }

    private Config(Properties properties) {
        this.properties = properties;
    }

    public static Config load(String configFile) throws FileNotFoundException {
        File file = new File(configFile);
        FileInputStream fileInputStream = new FileInputStream(file);
        Config config = null;
        try {
            Properties properties = new Properties();
            properties.load(fileInputStream);
            config =  new Config(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return config;
    }

    public Config getCopy() {
        return new Config(this);
    }

    public boolean hasKey(String key) {
        return this.properties.containsKey(key);
    }

    public int getInt(String key) {
        return Integer.parseInt(this.properties.getProperty(key));
    }

    public int getOrDefaultInt(String key, int def) {
        if (this.hasKey(key)) {
            return this.getInt(key);
        } else {
            return def;
        }
    }

    public long getLong(String key) {
        return Long.parseLong(this.properties.getProperty(key));
    }

    public long getOrDefaultLong(String key, long def) {
        if (this.hasKey(key)) {
            return this.getLong(key);
        } else {
            return def;
        }
    }

    public float getFloat(String key) {
        return Float.parseFloat(this.properties.getProperty(key));
    }

    public float getOrDefaultFloat(String key, float def) {
        if (this.hasKey(key)) {
            return this.getFloat(key);
        } else {
            return def;
        }
    }

    public double getDouble(String key) {
        return Double.parseDouble(this.properties.getProperty(key));
    }

    public double getOrDefaultDouble(String key, double def) {
        if (this.hasKey(key)) {
            return this.getDouble(key);
        } else {
            return def;
        }
    }

    public boolean getBoolean(String key) {
        return Boolean.parseBoolean(this.properties.getProperty(key));
    }

    public boolean getOrDefaultBoolean(String key, boolean def) {
        if (this.hasKey(key)) {
            return this.getBoolean(key);
        } else {
            return def;
        }
    }

    public String getString(String key) {
        return this.properties.getProperty(key);
    }

    public String getOrDefaultString(String key, String def) {
        if (this.hasKey(key)) {
            return this.getString(key);
        } else {
            return def;
        }
    }

    public void putInt(String key, int value) {
        this.properties.setProperty(key, Integer.toString(value));
    }

    public void putIfAbsentInt(String key, int value) {
        if (!this.hasKey(key)) {
            this.putInt(key, value);
        }
    }

    public void putLong(String key, long value) {
        this.properties.setProperty(key, Long.toString(value));
    }

    public void putIfAbsentLong(String key, long value) {
        if (!this.hasKey(key)) {
            this.putLong(key, value);
        }
    }

    public void putFloat(String key, float value) {
        this.properties.setProperty(key, Float.toString(value));
    }

    public void putIfAbsentFloat(String key, float value) {
        if (!this.hasKey(key)) {
            this.putFloat(key, value);
        }
    }

    public void putDouble(String key, double value) {
        this.properties.setProperty(key, Double.toString(value));
    }

    public void putIfAbsentDouble(String key, double value) {
        if (!this.hasKey(key)) {
            this.putDouble(key, value);
        }
    }

    public void putBoolean(String key, boolean value) {
        this.properties.setProperty(key, Boolean.toString(value));
    }

    public void putIfAbsentBoolean(String key, boolean value) {
        if (!this.hasKey(key)) {
            this.putBoolean(key, value);
        }
    }

    public void putString(String key, String value) {
        this.properties.setProperty(key, value);
    }

    public void putIfAbsentString(String key, String value) {
        if (!this.hasKey(key)) {
            this.putString(key, value);
        }
    }

    public ByteBuffer serialize() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(this.properties);
            objectOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    }

    public static Config deserialize(ByteBuffer data) {
        ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(data);
        Config config = null;
        try {
            ObjectInputStream objectInputStream = new ObjectInputStream(byteBufferInputStream);
            Properties properties = (Properties) objectInputStream.readObject();
            config = new Config(properties);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return config;
    }
}
