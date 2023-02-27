package com.lukasnt.spark.io

import java.nio.file.{FileSystem, FileSystems, Files, Paths}
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

object DirUtils {

  def listFilesInsideJar(dirPath: String): List[String] = {
    val jarLocation            = getClass.getProtectionDomain.getCodeSource.getLocation
    val jarFile                = Paths.get(jarLocation.toString.substring("file:".length))
    val fileSystem: FileSystem = FileSystems.newFileSystem(jarFile, getClass.getClassLoader)
    val result                 = Files.newDirectoryStream(fileSystem.getPath(dirPath)).asScala.toList.map(_.toString)
    result
  }

}
