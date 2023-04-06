package com.lukasnt.spark.io

import java.nio.file.{FileSystem, FileSystems, Files, Paths}

object DirUtils {

  def listFilesInsideJar(dirPath: String): List[String] = {
    val jarLocation            = getClass.getProtectionDomain.getCodeSource.getLocation
    val jarFile                = Paths.get(jarLocation.toString.substring("file:".length))
    val fileSystem: FileSystem = FileSystems.newFileSystem(jarFile, getClass.getClassLoader)
    val dictIterator           = Files.newDirectoryStream(fileSystem.getPath(dirPath)).iterator()
    var result: List[String]   = List()
    while (dictIterator.hasNext) result = result :+ dictIterator.next().toString
    result
  }

}
