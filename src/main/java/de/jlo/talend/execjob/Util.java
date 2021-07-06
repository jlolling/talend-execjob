package de.jlo.talend.execjob;

import java.io.File;

public class Util {

	public static File getFile(String path, boolean dir, String pathMeaning) throws Exception {
		if (pathMeaning == null) {
			pathMeaning = "";
		} else {
			pathMeaning = pathMeaning + ": ";
		}
		File f = new File(path);
		if (f.exists()) {
			if (dir) {
				if (f.isDirectory()) {
					return f;
				} else {
					throw new Exception(pathMeaning + path + " exist but is not a directory");
				}
			} else {
				if (f.isDirectory() == false) {
					return f;
				} else {
					throw new Exception(pathMeaning + path + " exist but is not a file");
				}
			}
		} else {
			throw new Exception(pathMeaning + f.getAbsolutePath() + " does not exist");
		}
	}
	
}
