/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.soft160.app.blobstore;

import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.net.InetSocketAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author yuri
 */
public class Main
{
    private static final Logger LOGGER = LogManager.getLogger("BlobNode");
    
    public static void main(String[] args)
    {
        try
        {
            String host = "localhost";
            int port = 11889;
            String rootPath = "./Data";
            
            for(int i = 0; i < args.length; ++i)
            {
                if (args[i].equals("-host") && (i+1 < args.length))
                {
                    host = args[++i];
                }
                else if (args[i].equals("-port") && (i+1 < args.length))
                {
                    port = Integer.parseInt(args[++i]);
                }
                else if (args[i].equals("-path") && (i+1 < args.length))
                {
                    rootPath = args[++i];
                }
            }
            
            LOGGER.info("Start blobNode");
            LocalStore localStore = new LocalStore(new File(rootPath));
            HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
            server.createContext("/blobNode", new HttpMessageHandler(localStore, rootPath + File.separator + "Tmp"));
            server.start();
            Thread.sleep(Long.MAX_VALUE);
            server.stop(30);
        }
        catch (Exception e)
        {
            LOGGER.error(e);
        }
    }
}
