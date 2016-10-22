/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.soft160.app.blobstore.test;

import com.soft160.app.blobstore.BlobInfo;
import com.soft160.app.blobstore.LocalStore;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.RocksDBException;

/**
 *
 * @author yuri
 */
public class LocalStoreTest
{
    @Test
    public void Test() throws RocksDBException, IOException, ClassNotFoundException
    {
        File baseDir = new File("/tmp/" + UUID.randomUUID().toString());
        baseDir.mkdirs();
        
        File tmpDir = new File(baseDir, "tmp");
        UUID bucket1 = UUID.randomUUID();
        UUID bucket2 = UUID.randomUUID();
        UUID blob1 = UUID.randomUUID();
        UUID blob2 = UUID.randomUUID();
        UUID blob3 = UUID.randomUUID();
        
        if (!tmpDir.exists())
        {
            tmpDir.mkdirs();
        }
        
        File tmpBlob1 = File.createTempFile(UUID.randomUUID().toString(), ".tmp", tmpDir);
        File tmpBlob2 = File.createTempFile(UUID.randomUUID().toString(), ".tmp", tmpDir);
        File tmpBlob3 = File.createTempFile(UUID.randomUUID().toString(), ".tmp", tmpDir);
        
        LocalStore store = new LocalStore(baseDir);
        
        Assert.assertTrue(store.AddBlob(bucket1, blob1, tmpBlob1));
        Assert.assertTrue(store.AddBlob(bucket2, blob2, tmpBlob2));
        Assert.assertFalse(store.AddBlob(bucket2, blob3, tmpBlob2));
        Assert.assertFalse(store.AddBlob(bucket2, blob2, tmpBlob3));
        
        String storePath = baseDir.getAbsoluteFile() + File.separator + "files";
        
        BlobInfo info1 = store.GetBlob(bucket1, blob1);
        Assert.assertNotNull(info1);
        Assert.assertEquals(storePath + File.separator + bucket1.toString() + File.separator + blob1.toString(), info1.path);
        Assert.assertEquals(0, info1.size);

        BlobInfo info2 = store.GetBlob(bucket2, blob2);
        Assert.assertNotNull(info2);
        Assert.assertEquals(storePath + File.separator + bucket2.toString() + File.separator + blob2.toString(), info2.path);
        Assert.assertEquals(0, info2.size);

        BlobInfo info3 = store.GetBlob(bucket2, blob3);
        Assert.assertNull(info3);
    }
}
