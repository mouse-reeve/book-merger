rm -rf output/
hadoop com.sun.tools.javac.Main BookMerger.java
jar cf bookmerger.jar BookMerger*.class
hadoop jar bookmerger.jar BookMerger input/ output
cat output/part-r-00000
