package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocalFileUpload {

	public static void main(String[] args) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://192.168.26.129:9000");
			FileSystem hdfs = FileSystem.get(conf);
			
			System.out.println(hdfs.getHomeDirectory());
			System.out.println(hdfs.getWorkingDirectory());
			
			Path path = new Path("/localFile2");
			Path localPath = new Path("C:\\Users\\DATA8320-09\\Downloads\\example_kbo2015.csv");

			System.out.println(hdfs.exists(path));
			if (hdfs.exists(path)) {
				hdfs.delete(path, true);
			}
			
			//파일 업로드 끝!
			hdfs.copyFromLocalFile(localPath, path);
			
			System.out.println("Local File Upload Finished!!");
			
	
			hdfs.close();
		
		}catch(Exception e) {
			System.out.println(e.toString());
		}
	}

}
