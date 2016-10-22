/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.soft160.app.blobstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

/**
 *
 * @author yuri
 */
public class LocalStore
{

    static
    {
        RocksDB.loadLibrary();
    }

    public LocalStore(File root) throws RocksDBException
    {
        baseFilePath = new File(root, "files");
        if (!baseFilePath.exists())
        {
            baseFilePath.mkdirs();
        }
        options = new Options().setCreateIfMissing(true);
        mainDb = RocksDB.open(options, baseFilePath.getAbsolutePath() + File.separator + "mainDB");
        fileDb = RocksDB.open(options, baseFilePath.getAbsolutePath() + File.separator + "fileDB");
        mainDbLock = new Object();
        fileDbLock = new Object();
    }

    public void Close()
    {
        if (options != null)
        {
            options.close();
            options = null;
        }
        if (mainDb != null)
        {
            mainDb.close();
            mainDb = null;
        }
        if (fileDb != null)
        {
            fileDb.close();
            fileDb = null;
        }
    }

    public BlobInfo GetBlob(UUID bucket, UUID guid) throws RocksDBException, IOException, ClassNotFoundException
    {
        byte[] data;
        synchronized (fileDbLock)
        {
            data = fileDb.get(CreateKey(bucket, guid));
        }
        if (data == null)
        {
            return null;
        }
        try (ObjectInputStream dataStream = new ObjectInputStream(new ByteArrayInputStream(data)))
        {
            return (BlobInfo) dataStream.readObject();
        }
    }

    public boolean AddBlob(UUID bucket, UUID guid, File tmpBlob) throws IOException, RocksDBException
    {
        if (!tmpBlob.exists())
        {
            return false;
        }
        File bucketDir = new File(baseFilePath.getAbsolutePath() + File.separator + bucket.toString());
        
        BlobInfo info = new BlobInfo();
        info.path = bucketDir.getAbsolutePath() + File.separator + guid.toString();
        info.size = (int) tmpBlob.length();
        byte[] infoData;
        
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(); ObjectOutputStream stream = new ObjectOutputStream(byteStream))
        {
            stream.writeObject(info);
            stream.flush();
            infoData = byteStream.toByteArray();
        }
        
        synchronized (fileDbLock)
        {
            byte[] key = CreateKey(bucket, guid);
            if (fileDb.get(key) != null)
            {
                //already exists
                return false;
            }
            fileDb.put(CreateKey(bucket, guid), infoData);
        }

        if (!bucketDir.exists())
        {
            bucketDir.mkdirs();
        }
        File file = new File(info.path);
        if (file.exists())
        {
            //log
            file.delete();
        }
        
        return tmpBlob.renameTo(file);
    }

    private byte[] CreateKey(UUID bucket, UUID fileId)
    {
        ByteBuffer bb = ByteBuffer.wrap(new byte[32]);
        bb.putLong(bucket.getMostSignificantBits());
        bb.putLong(bucket.getLeastSignificantBits());
        bb.putLong(fileId.getMostSignificantBits());
        bb.putLong(fileId.getLeastSignificantBits());
        return bb.array();
    }

    private Options options;
    private RocksDB mainDb;
    private RocksDB fileDb;
    private final File baseFilePath;
    private final Object mainDbLock;
    private final Object fileDbLock;
}
