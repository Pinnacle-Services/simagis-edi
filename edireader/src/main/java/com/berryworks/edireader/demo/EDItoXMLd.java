package com.berryworks.edireader.demo;

import com.berryworks.edireader.util.CommandLine;

import java.io.File;
import java.io.FileFilter;
import java.io.IOError;
import java.io.IOException;

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 1/30/2017.
 */
public class EDItoXMLd {
    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(args) {
            @Override
            public String usage() {
                return "EDItoXMLd [inputDir] [-o outputDir]";
            }
        };
        File inputDir = new File(commandLine.getPosition(0));
        if (inputDir.isDirectory()) {
            File outputDir = new File(commandLine.getOption("o"));
            File[] files = inputDir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.isFile();
                }
            });
            assert files != null;
            for (File file : files) {
                File xmlFile;
                String[] args1;
                try {
                    xmlFile = new File(outputDir, file.getName() + ".xml");
                    args1 = new String[]{
                            file.getCanonicalPath(),
                            "-o",
                            xmlFile.getCanonicalPath()
                    };
                } catch (IOException e) {
                    throw new IOError(e);
                }
                try {
                    System.out.println("EDItoXML" + args1[0] + " " + args1[1] + " " + args1[2]);
                    EDItoXML.main(args1);
                } catch (Exception e) {
                    e.printStackTrace();
                    xmlFile.renameTo(new File(outputDir, xmlFile.getName() + ".err"));
                }
            }
        }
    }
}
