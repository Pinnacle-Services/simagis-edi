/*
 * Copyright 2005-2011 by BerryWorks Software, LLC. All rights reserved.
 *
 * This file is part of EDIReader. You may obtain a license for its use directly from
 * BerryWorks Software, and you may also choose to use this software under the terms of the
 * GPL version 3. Other products in the EDIReader software suite are available only by licensing
 * with BerryWorks. Only those files bearing the GPL statement below are available under the GPL.
 *
 * EDIReader is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * EDIReader is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with EDIReader.  If not,
 * see <http://www.gnu.org/licenses/>.
 */

package com.berryworks.edireader.demo;

import com.berryworks.edireader.EDIReader;
import com.berryworks.edireader.EDIReaderFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.*;

/**
 * A sample program using EDIReader to scan an EDIFACT UTILTS message extracting
 * certain fields from every GPS segment, illustrating the use of a custom SAX
 * DocumentHandler.
 */
public class UTILTSScanner
{
  private InputSource inputSource;
  private PrintWriter scannerOutput;
  private EDIReader parser;

  private UTILTSScanner(String inputFileName, String outputFileName)
  {
    try
    {
      OutputStream outputStream;
      if (outputFileName == null)
      {
        outputStream = System.out;
      }
      else
      {
        outputStream = new FileOutputStream(outputFileName);
      }
      scannerOutput = new PrintWriter(new OutputStreamWriter(
        outputStream, "ISO-8859-1"));
      inputSource = new InputSource(new InputStreamReader(
        new FileInputStream(inputFileName), "ISO-8859-1"));
    } catch (IOException e)
    {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  void run()
  {

    ContentHandler handler = new ScanningHandler();

    try
    {
      while (true)
      {
        // The following line creates an EDIReader explicitly
        // as an alternative to the JAXP-based technique.
        parser = EDIReaderFactory.createEDIReader(inputSource);
        if (parser == null)
        {
          // end of input
          break;
        }
        parser.setContentHandler(handler);

        parser.parse(inputSource);
      }

    } catch (IOException e)
    {
      System.out.println(e.getMessage());
    } catch (SAXException e)
    {
      System.err.println("\nEDI input not well-formed:\n" + e.toString());
    }
  }

  public static void main(String args[])
  {
    String outputFileName;
    String inputFileName;
    if (args.length < 2)
      badArgs();
    inputFileName = args[0];
    outputFileName = args[1];
    System.out.println("Reading UTILTS interchange from file "
      + inputFileName);
    System.out.println("Writing GPS summary to file " + outputFileName);

    long before = System.currentTimeMillis();
    UTILTSScanner scanner = new UTILTSScanner(inputFileName, outputFileName);
    scanner.run();
    long diff = System.currentTimeMillis() - before;

    System.out.println("Program termination after " + diff
      + " milliseconds");
  }

  private static void badArgs()
  {
    System.out.println("Usage: UTILTSScanner inputfile outputfile");
    throw new RuntimeException("Missing or invalid command line arguments");
  }

  /**
   * An inner class serving as the ContentHandler for the scanning program. It
   * implements only those methods of a ContentHandler that it needs to. The
   * others are provided in stub form by DefaultHandler.
   */
  class ScanningHandler extends DefaultHandler
  {
    int gpoCount;
    boolean gpo;
    String gValue = "";

    @Override
    public void startElement(String namespace, String localName,
                             String qName, Attributes atts) throws SAXException
    {
      if (localName.startsWith(parser.getXMLTags().getSegTag()))
      {
        String segmentType = atts.getValue(0);
        if (segmentType.equals("GPO"))
        {
          gpoCount++;
          gpo = true;
          scannerOutput.println();
          scannerOutput.print(gpoCount);
        }
        else
        {
          gpo = false;
        }
      }
      else if (localName.startsWith(parser.getXMLTags().getElementTag()))
      {
        if (gpo)
        {
          String gId = atts.getValue("Id");
        }
      }
    }

    @Override
    public void endElement(String namespace, String localName, String qName)
      throws SAXException
    {
      if (gpo && localName.startsWith(parser.getXMLTags().getElementTag()))
      {
        scannerOutput.print(", " + gValue);
        gValue = "";
      }
    }

    @Override
    public void endDocument() throws SAXException
    {
      scannerOutput.println();
      scannerOutput.flush();
      scannerOutput.close();
    }

    @Override
    public void characters(char[] cdata, int start, int length)
    {
      if (gpo)
      {
        gValue += new String(cdata, start, length);
      }
    }
  }

}
