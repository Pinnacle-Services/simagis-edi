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

package com.berryworks.edireader;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Inspects runtime environment for diagnostic purposes.
 * <p/>
 * This class is not used by EDIWriter, but is included to help identify environmental issues.
 * It inspects the runtime environment and documents the platform, version of Java, presence
 * of critical EDIReader classes, and the full classpath.
 */
public class Inspect
{

  private boolean ediViewerPresent;
  private final String lineBreak = System.getProperty("line.separator");

  public static void main(String[] args)
  {
    (new Inspect()).inspect();
  }

  public String getVersion()
  {
    String version = "unknown";
    InputStream inputStream = getClass().getClassLoader()
      .getResourceAsStream("META-INF/maven/com.berryworks/edireader/pom.properties");
    if (inputStream != null)
    {
      Properties properties = new Properties();
      try
      {
        properties.load(inputStream);
        version = properties.getProperty("version");
      } catch (IOException ignore)
      {
      } finally
      {
        try
        {
          inputStream.close();
        } catch (IOException ignore)
        {
        }
      }
    }
    return version;
  }

  private void inspect()
  {
    System.out.println(lineBreak + lineBreak
      + "Copyright 2004 - 2011 by BerryWorks Software LLC" + lineBreak + lineBreak +
      "EDIReader, version " + getVersion() + lineBreak
      + ediWriterInstallation() + lineBreak
      + ediStAXInstallation() + lineBreak
      + ediViewerInstallation() + lineBreak + lineBreak +
      "OS:           " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
      + System.getProperty("os.arch") + lineBreak +
      "Java Runtime: " + System.getProperty("java.vendor") + " " + System.getProperty("java.version"));

    System.out.println(lineBreak + getClasspath() + lineBreak + lineBreak);
  }

  private void confirm(PrintStream out, String className)
  {
    try
    {
      Class.forName(className);
      out.println("confirmed:    " + className);
    } catch (Throwable e)
    {
      out.println("not found:    " + className + " (" + e + ")");
    }
  }

  private void confirm(PrintStream out, String className, boolean conditional)
  {
    if (conditional)
    {
      confirm(out, className);
    }
  }

  private String getClasspath()
  {
    String classPath = System.getProperty("java.class.path");
    StringBuilder result = new StringBuilder("Classpath:" + lineBreak);
    for (String anElement : classPath.split(":"))
    {
      result.append("              ").append(anElement).append(lineBreak);
    }
    return result.toString();
  }

  private String ediWriterInstallation()
  {
    String result = "EDIWriter";
    if (isClassPresent("com.berryworks.ediwriter.EDIWriter"))
    {
      result += ", version "
        + getInspectedVersion("com.berryworks.ediwriter.Inspect");
    }
    else
    {
      result += " not found";
    }
    result += " (optional BerryWorks product for generating EDI from XML)";
    return result;
  }

  private String ediStAXInstallation()
  {
    String result = "EDIStAX";
    if (isClassPresent("com.berryworks.edireader.stax.EDIEventReader"))
    {
      ediViewerPresent = true;
      result += ",   version "
        + getInspectedVersion("com.berryworks.edireader.stax.Inspect");
    }
    else
    {
      ediViewerPresent = false;
      result += " not found";
    }
    result += " (optional BerryWorks product support StAX API)";
    return result;
  }

  private String ediViewerInstallation()
  {
    String result = "EDIViewer";
    if (isClassPresent("com.berryworks.ediviewer.formatter.html.HtmlFormatter"))
    {
      ediViewerPresent = true;
      result += ", version "
        + getInspectedVersion("com.berryworks.ediviewer.Inspect");
    }
    else
    {
      ediViewerPresent = false;
      result += " not found";
    }
    result += " (optional BerryWorks product for viewing EDI documents)";
    return result;
  }

  private boolean isEDIViewerPresent()
  {
    return ediViewerPresent;
  }

  private String getInspectedVersion(String inspectClassName)
  {
    String result = "???";

    try
    {
      Class inspectClass = Class.forName(inspectClassName);
      Object inspectInstance = inspectClass.newInstance();
      Method method = inspectClass.getMethod("getVersion");
      result = (String) method.invoke(inspectInstance);
    } catch (Throwable e)
    {
      return result;
    }

    return result;
  }

  private boolean isClassPresent(String className)
  {
    try
    {
      Class.forName(className);
      return true;
    } catch (Throwable e)
    {
      return false;
    }
  }

}
