/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.soft160.app.blobstore;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;

/**
 *
 * @author yuri
 */
public class HttpMessageHandler implements HttpHandler
{

    private static final Logger LOGGER = LogManager.getLogger("HttpMessageHandler");

    public HttpMessageHandler(LocalStore localStore, String tmpFilePath)
    {
        this.localStore = localStore;
        this.tmpFileDir = new File(tmpFilePath);
        if (!tmpFileDir.exists())
        {
            tmpFileDir.mkdirs();
        }
        else if (!tmpFileDir.isDirectory())
        {
            throw new IllegalArgumentException(tmpFilePath + " is not directory");
        }
    }

    @Override
    public void handle(HttpExchange he) throws IOException
    {
        try
        {
            switch (he.getRequestMethod())
            {
                case "GET":
                    Get(he);
                    break;
                case "POST":
                    Post(he);
                    break;
                case "PUT":
                    Put(he);
                    break;
                case "DELETE":
                    Delete(he);
                    break;
                default:
                    break;
            }
        }
        catch (Exception e)
        {
            SendResponse(he, HttpStatus.SC_INTERNAL_SERVER_ERROR, e.toString());
            LOGGER.error(e);
        }
    }

    private void Get(HttpExchange he) throws IOException, RocksDBException, ClassNotFoundException
    {
        List<NameValuePair> pairs = URLEncodedUtils.parse(he.getRequestURI().getQuery(), StandardCharsets.UTF_8);
        UUID bucket = null, file = null;
        for (NameValuePair pair : pairs)
        {
            if (pair.getName().equalsIgnoreCase("bucket"))
            {
                bucket = parseUUID(pair.getValue());
            }
            else if (pair.getName().equalsIgnoreCase("blob"))
            {
                file = parseUUID(pair.getValue());
            }
        }
        if (bucket == null || file == null)
        {
            SendResponse(he, HttpStatus.SC_BAD_REQUEST, "Unknown bucket/file");
        }

        BlobInfo info = localStore.GetBlob(bucket, file);
        if (info == null)
        {
            SendResponse(he, HttpStatus.SC_NOT_FOUND, "bucket/file not found");
        }
        else
        {
            he.getResponseHeaders().add("Content-Type", "application/octet-stream");
            he.getResponseHeaders().add("Content-Length", Integer.toString(info.size));
            try (FileInputStream inFile = new FileInputStream(info.path))
            {
                he.sendResponseHeaders(HttpStatus.SC_OK, inFile.getChannel().size());
                IOUtils.copy(inFile, he.getResponseBody());
            }
        }
    }

    private void Post(HttpExchange he) throws IOException
    {
        SendResponse(he, HttpStatus.SC_METHOD_NOT_ALLOWED, null);
    }

    private void Put(HttpExchange he) throws IOException, RocksDBException
    {
        List<NameValuePair> pairs = URLEncodedUtils.parse(he.getRequestURI().getQuery(), StandardCharsets.UTF_8);
        UUID bucket = null, file = null;
        for (NameValuePair pair : pairs)
        {
            if (pair.getName().equalsIgnoreCase("bucket"))
            {
                bucket = parseUUID(pair.getValue());
            }
            else if (pair.getName().equalsIgnoreCase("blob"))
            {
                file = parseUUID(pair.getValue());
            }
        }
        if (bucket == null || file == null)
        {
            SendResponse(he, HttpStatus.SC_BAD_REQUEST, "Unknown bucket/file");
        }

        String tmpName = UUID.randomUUID().toString() + ".tmp";
        File tmpFile = File.createTempFile(tmpName, "", tmpFileDir);

        try (FileOutputStream tmpStream = new FileOutputStream(tmpFile))
        {
            IOUtils.copy(he.getRequestBody(), tmpStream);
        }
        if (localStore.AddBlob(bucket, file, tmpFile))
        {
            SendResponse(he, HttpStatus.SC_OK, null);
        }
        else
        {
            SendResponse(he, HttpStatus.SC_BAD_REQUEST, "Insert error");
        }

    }

    private void Delete(HttpExchange he) throws IOException
    {
        SendResponse(he, HttpStatus.SC_METHOD_NOT_ALLOWED, null);
    }

    private UUID parseUUID(String s)
    {
        try
        {
            return UUID.fromString(s);
        }
        catch (Exception e)
        {
            LOGGER.warn("Parse UUID error " + s + ", " + e);
        }
        return null;
    }

    private void SendResponse(HttpExchange he, int status, String text) throws IOException
    {
        byte[] data = null;
        if (text != null && !text.isEmpty())
        {
            data = text.getBytes(StandardCharsets.UTF_8);
        }
        he.sendResponseHeaders(status, data == null ? 0 : data.length);
        if (data != null)
        {
            he.getResponseBody().write(data);
        }
        he.getResponseBody().flush();
    }

    private final LocalStore localStore;
    private final File tmpFileDir;
}
