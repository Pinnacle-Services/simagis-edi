package com.berryworks.edireader.demo;


import org.exist.xmldb.XmldbURI;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.modules.CollectionManagementService;
import org.xmldb.api.modules.XMLResource;

import java.io.*;

/**
 * Created by avylegzhanin12897 on 1/30/2017.
 */
public class ToDB {


    private final static String URI = "xmldb:exist://localhost:8080/exist/xmlrpc";

    protected static void usage() {
        System.out.println("usage: ToDB collection ediDir");
        System.exit(0);
    }

    public static void main(String args[]) throws Exception {
        if(args.length < 2)
            usage();

        String collection = args[0], dir = args[1];

        // initialize driver
        String driver = "org.exist.xmldb.DatabaseImpl";
        Class<?> cl = Class.forName(driver);
        Database database = (Database)cl.newInstance();
        database.setProperty("create-database", "true");
        DatabaseManager.registerDatabase(database);

        // try to get collection
        Collection col =
                DatabaseManager.getCollection(URI + collection);
        if(col == null) {
            // collection does not exist: get root collection and create.
            // for simplicity, we assume that the new collection is a
            // direct child of the root collection, e.g. /db/test.
            // the example will fail otherwise.
            Collection root = DatabaseManager.getCollection(URI + XmldbURI.ROOT_COLLECTION);
            CollectionManagementService mgtService =
                    (CollectionManagementService)root.getService("CollectionManagementService", "1.0");
            col = mgtService.createCollection(collection.substring((XmldbURI.ROOT_COLLECTION + "/").length()));
        }
        File[] files = new File(dir).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile();
            }
        });
        assert files != null;
        for (File file : files) {
            final Object xml;
            {
                ByteArrayOutputStream buff = new ByteArrayOutputStream();
                BufferedWriter outputWriter = new BufferedWriter(new OutputStreamWriter(buff, "ISO-8859-1"));
                try {
                    FileReader inputReader = new FileReader(file);
                    try {
                        new EDItoXML(inputReader, outputWriter).run();
                    } finally {
                        inputReader.close();
                    }
                } finally {
                    outputWriter.close();
                }
                xml = buff.toByteArray();
            }
            // create new XMLResource
            XMLResource document = (XMLResource)col.createResource(file.getName() + ".xml", "XMLResource");
            document.setContent(xml);
            System.out.println("storing document " + document.getId() + "...");
            col.storeResource(document);
        }
        System.out.println("ok.");
    }
}
