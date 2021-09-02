package minsait.ttaa.datio.engine;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ReadProperties {
	private String INPUT_PATH_CSV;
	private String OUTPUT_PATH;
	private String OUTPUT_PARQUET;
	  ReadProperties() {
  	  try {
  		     Properties propiedades = new Properties();
  		     propiedades
  		       .load(new FileInputStream("src/test/resources/data/project.properties"));
  		     INPUT_PATH_CSV= propiedades.getProperty("read.input.path.csv");
  		     OUTPUT_PATH = propiedades.getProperty("read.output.path");
  		     OUTPUT_PARQUET = propiedades.getProperty("read.output.parquet");
  		    } catch (FileNotFoundException e) {
  		     System.out.println("Error, El archivo no exite");
  		    } catch (IOException e) {
  		     System.out.println("Error, No se puede leer el archivo");
  		    }
  		   }
	public String getINPUT_PATH_CSV() {
		return INPUT_PATH_CSV;
	}
	public String getOUTPUT_PATH() {
		return OUTPUT_PATH;
	}
	public String getOUTPUT_PARQUET() {
		return OUTPUT_PARQUET;
	}

}
