package de.jlo.talend.execjob;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TalendJob {
	
	private String project = null;
	private String jobName = null;
	private String jobVersion = null;
	private String jobRootPath = null;
	private List<File> jarFiles = new ArrayList<>();
	private ClassLoader jobClassLoader = null;
	private Properties contextVars = new Properties();
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private String[][] jobResult = null;
	public static final String TALEND_NULL = "<TALEND_NULL>";
	private boolean jobHasOutputFlow = false;
	private Map<String, Object> globalMap = null;
	private HttpClient httpClient = null;
	private String nexusBaseUrl = null;
	private String nexusDownloadPathPattern = "/repository/{repository}/{groupIdPath}/{jobName}/{jobVersion}/{jobName}-{jobVersion}";
	private String nexusRepository = "releases";
	private String nexusLogin = null;
	private String nexusPassword = null;
	private Properties jobInfoProperties = new Properties();
	
	private void collectJarFiles(File dir) {
		File[] jars = dir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if (name.endsWith(".jar")) {
					return true;
				}
				return false;
			}
		});
		for (File jar : jars) {
			jarFiles.add(jar);
		}
	}
	
	private void collectJarFiles() throws Exception {
		File dir = Util.getFile(jobRootPath, true, "job-root-dir");
		File libdir = Util.getFile(dir.getAbsolutePath() + "/lib", true, "job-lib-dir");
		collectJarFiles(libdir);
		File jobdir = Util.getFile(dir.getAbsolutePath() + "/" + jobName, true, "job-dir");
		collectJarFiles(jobdir);
	}
	
	private void setupJobClassLoader() throws Exception {
		collectJarFiles();
		URL[] urls = new URL[jarFiles.size()+1];
		int i = 0;
		for ( ; i < jarFiles.size(); i++) {
			urls[i] = new URL("file:" + jarFiles.get(i).getAbsolutePath());
		}
		urls[i] = new URL("file:" + jobRootPath);
		try {
			jobClassLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
		} catch (Throwable t) {
			throw new Exception("Create classloader for jars failed: " + t.getMessage(), t);
		}
	}
	
	public void setContext(String varname, Object value) {
		if (varname == null || varname.trim().isEmpty()) {
			throw new IllegalArgumentException("context varname cannot be null or empty");
		} else if (value != null) {
			contextVars.put(varname, value);
		} else {
			contextVars.put(varname, TALEND_NULL);
		}
	}
	
	private String convertType(Object value) {
		if (value != null) {
			if (value instanceof String) {
				return (String) value;
			} else if (value instanceof Integer) {
				return String.valueOf(value);
			} else if (value instanceof Long) {
				return String.valueOf(value);
			} else if (value instanceof Double) {
				return String.valueOf(value);
			} else if (value instanceof Float) {
				return String.valueOf(value);
			} else if (value instanceof Short) {
				return String.valueOf(value);
			} else if (value instanceof BigDecimal) {
				return String.valueOf(value);
			} else if (value instanceof Date) {
				return sdf.format((Date) value);
			} else if (value instanceof Boolean) {
				return String.valueOf(value);
			} else {
				return String.valueOf(value);
			}
		} else {
			return TALEND_NULL;
		}
	}
	
	private String[] buildContextParams() {
		String[] args = new String[contextVars.size()];
		int index = 0;
		for (Map.Entry<Object, Object> entry : contextVars.entrySet()) {
			args[index++] = "--context_param " + entry.getKey() + "=" + convertType(entry.getValue());
		}
		return args;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getJobVersion() {
		return jobVersion;
	}

	public void setJobVersion(String jobVersion) {
		this.jobVersion = jobVersion;
	}
	
	private void readJobInfo() throws Exception {
		File jobInfoFile = new File(jobRootPath, "jobInfo.properties");
		if (jobInfoFile.exists() == false) {
			throw new Exception("For the job with the root path: " + jobRootPath + " the jobInfo.properties file is missing. Please take care the path refers to the job root dir.");
		}
		jobInfoProperties.clear();
		InputStream in = new FileInputStream(jobInfoFile);
		try {
			jobInfoProperties.load(in);
		} catch (Exception e) {
			throw new Exception("Error while reading jobInfo properties from file: " + jobInfoFile.getAbsolutePath() + ": " + e.getMessage(), e);
		} finally {
			if (in != null) {
				try { in.close(); } catch (Exception ex) {}
			}
		}
		if (project == null) {
			project = jobInfoProperties.getProperty("project");
		}
		if (project == null) {
			throw new IllegalStateException("project not set or could not determined from jobInfo.properties");
		}
		if (jobName == null) {
			jobName = jobInfoProperties.getProperty("job");
		}
		if (jobName == null) {
			throw new IllegalStateException("jobName not set or could not determined from jobInfo.properties");
		}
		if (jobVersion == null) {
			jobVersion = jobInfoProperties.getProperty("jobVersion");
		}
		if (jobVersion == null) {
			throw new IllegalStateException("jobVersion not set or could not determined from jobInfo.properties");
		}
	}
	
	/**
	 * Start the Talend job and get the result if there are some
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public void start() throws Exception {
		readJobInfo();
		setupJobClassLoader();
		jobResult = null;
		jobHasOutputFlow = false;
		String className = project.toLowerCase() + "." + jobName + "_" + jobVersion.replace('.', '_') + "." + jobName;
		Class<?> jobClass = null;
		try {
			jobClass = Class.forName(className, true, jobClassLoader);
		} catch (Throwable e) {
			throw new Exception("Load job class: " + className + " failed: " + e.getMessage(), e);
		}
		Object instance = null;
		try {
			instance = jobClass.getDeclaredConstructor().newInstance();
		} catch (Throwable e) {
			throw new Exception("Create instance of job class: " + className + " failed: " + e.getMessage(), e);
		}
		Field fieldGlobalMap = null;
		try {
			fieldGlobalMap = jobClass.getDeclaredField("globalMap");
			fieldGlobalMap.setAccessible(true);
		} catch (Throwable t) {
			System.err.println("Field globalMap is not reachable: " + t.getMessage());
		}
		if (fieldGlobalMap != null) {
			try {
				Object test = fieldGlobalMap.get(instance);
				if (test instanceof Map) {
					globalMap = (Map<String, Object>) test;
				} else {
					System.err.println("Field globalMap is null");
				}
			} catch (Throwable t) {
				throw new Exception("Cannot access globalMap: " + t.getMessage(), t);
			}
		}
		Method methodHasBufferedOutput = null;
		try {
			methodHasBufferedOutput = jobClass.getDeclaredMethod("hastBufferOutputComponent");
		} catch (Exception e) {
			throw new Exception("Method: hastBufferOutputComponent does not exist in job class: " + className, e);
		}
		Boolean hasBufferOutput = (Boolean) methodHasBufferedOutput.invoke(instance);
		if (hasBufferOutput != null) {
			jobHasOutputFlow = hasBufferOutput;
		}
		Method methodRunJob = null;
		try {
			methodRunJob = jobClass.getDeclaredMethod("runJob", String[].class);
		} catch (Throwable e) {
			throw new Exception("Method: runJob with String[] as argument does not exist", e);
		}
		Object args = buildContextParams();
		try {
			System.out.println("Start job: " + jobName + " version: " + jobVersion);
			jobResult = (String[][]) methodRunJob.invoke(instance, args);
		} catch (InvocationTargetException te) {
			throw new Exception("Run job with class: " + className + " failed: " + te.getMessage(), te);
		} catch (Throwable t) {
			throw new Exception("Invoke runJob method in class: " + className + " failed: " + t.getMessage(), t);
		}
	}

	public String getAlljobsRootPath() {
		return jobRootPath;
	}

	/**
	 * Root for the current job
	 * This is the folder where the jobInfo.properties file resists
	 * @param jobRootPath
	 */
	public void setJobRootPath(String jobRootPath) {
		this.jobRootPath = jobRootPath;
	}
	
	public boolean jobHasResultOutputFlow() {
		return jobHasOutputFlow;
	}

	/**
	 * the job can optionally return a data table
	 * @return String[row][col]
	 */
	public String[][] getJobResult() {
		return jobResult;
	}
	
	public Object getGlobalMapValue(String varName) {
		if (globalMap != null) {
			return globalMap.get(varName);
		} else {
			return null;
		}
	}
	
}
