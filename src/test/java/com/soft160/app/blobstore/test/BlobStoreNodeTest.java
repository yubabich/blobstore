/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.soft160.app.blobstore.test;

import com.soft160.app.blobstore.Main;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;
import java.util.UUID;
import org.junit.After;
import org.junit.Test;
import org.junit.Assert;

/**
 *
 * @author yuri
 */
public class BlobStoreNodeTest
{

    Thread mainThread;

    public BlobStoreNodeTest() throws InterruptedException
    {
        mainThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                String basePath = "/tmp" + File.separator + UUID.randomUUID().toString();
                String[] args = new String[]
                {
                    "-host", "localhost", "-port", "54678", "-path", basePath
                };

                Main.main(args);
            }
        });
        
        mainThread.start();
        Thread.sleep(1000);
    }

    @Test
    public void SimpleTest() throws Exception
    {

        Info m1 = Send();
        Info m2 = Send();

        byte[] d1 = Receive(m1);
        Assert.assertArrayEquals(m1.data, d1);
        byte[] d2 = Receive(m2);
        Assert.assertArrayEquals(m2.data, d2);
    }
    
    @After
    public void Close()
    {
        mainThread.stop();
    }

    private static class Info
    {

        public UUID bucket;
        public UUID blob;
        public byte[] data;
    }

    private Info Send() throws MalformedURLException, IOException
    {
        Info info = new Info();
        info.bucket = UUID.randomUUID();
        info.blob = UUID.randomUUID();
        info.data = CreateData();

        URL url = new URL(baseUrl + "?bucket=" + info.bucket.toString() + "&blob=" + info.blob.toString());
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("PUT");
        connection.setRequestProperty("Content-Type", "application/octet-stream");
        connection.setRequestProperty("Content-Length", Integer.toString(info.data.length));
        connection.setDoOutput(true);
        connection.getOutputStream().write(info.data);
        connection.getOutputStream().close();

        int status = connection.getResponseCode();
        Assert.assertEquals(200, status);
        return info;
    }

    private byte[] Receive(Info input) throws MalformedURLException, IOException
    {
        URL url = new URL(baseUrl + "?bucket=" + input.bucket.toString() + "&blob=" + input.blob.toString());
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setDoOutput(true);
        connection.setDoInput(true);

        int status = connection.getResponseCode();
        Assert.assertEquals(200, status);
        Assert.assertEquals("application/octet-stream", connection.getContentType());
        byte[] data = new byte[connection.getContentLength()];
        int pos = 0;
        while (pos < data.length)
        {
            int len = connection.getInputStream().read(data, pos, data.length - pos);
            Assert.assertNotEquals(-1, len);
            pos += len;
        }
        return data;
    }

    private byte[] CreateData()
    {
        byte[] data = new byte[random.nextInt(100)];
        random.nextBytes(data);
        return data;
    }

    private final Random random = new Random(101);
    private final String baseUrl = "http://localhost:54678/blobNode";
}
