Hadoop_Sample Project
===========

Pom.xml

Included a patch in lib/ folder to fix the permission issue of tmp folders under Windows.

Add below statement in your code:

conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
