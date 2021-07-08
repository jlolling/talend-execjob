package de.jlo.talend.execjob;

import java.io.IOException;
import java.net.URL;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public class HttpClient {
	
    private static Logger LOG = Logger.getLogger(HttpClient.class);
	private int statusCode = 0;
	private String statusMessage = null;
	private int maxRetriesInCaseOfErrors = 0;
	private int currentAttempt = 0;
	private long waitMillisAfterError = 1000l;
	private CloseableHttpClient closableHttpClient = null;
	private HttpClientContext context = null;
	private boolean useUnsafeCertManager = false;
	
	public HttpClient(String urlStr, String user, String password, int timeout) throws Exception {
		closableHttpClient = createCloseableClient(urlStr, user, password, timeout);
	}
	
	/**
	 * returns true if the string is not filled or contains "null"
	 * @param s the string
	 * @returns true if empty 
	 */
	public static boolean isEmpty(String s) {
		if (s == null) {
			return true;
		}
		if (s.trim().isEmpty()) {
			return true;
		}
		if (s.trim().equalsIgnoreCase("null")) {
			return true;
		}
		return false;
	}
	
	public String execute(HttpRequestBase request, boolean expectResponse) throws Exception {
		String responseContent = "";
		currentAttempt = 0;
		for (currentAttempt = 0; currentAttempt <= maxRetriesInCaseOfErrors; currentAttempt++) {
			if (Thread.currentThread().isInterrupted()) {
				break;
			}
            CloseableHttpResponse httpResponse = null;
            request.setHeader("User-Agent", "Apache-Maven/3.2.1");
            try {
            	if (context != null) {
                	httpResponse = closableHttpClient.execute(request, context);
            	} else {
                	httpResponse = closableHttpClient.execute(request);
            	}
            	statusCode = httpResponse.getStatusLine().getStatusCode();
            	statusMessage = httpResponse.getStatusLine().getReasonPhrase();
            	if (statusCode >= 200 && statusCode < 300) {
                	if (expectResponse) {
                    	responseContent = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
                    	if (isEmpty(responseContent)) {
                    		throw new Exception("Empty response received.");
                    	}
                	}
            	}
            	try {
                	httpResponse.close();
                	httpResponse = null;
            	} catch (Exception ce) {
            		// ignore
            	}
            	if (statusCode > 300) {
            		throw new Exception("Got status-code: " + statusCode + ", reason-phrase: " + statusMessage + ", response: " + responseContent);
            	}
            	break;
            } catch (Throwable e) {
            	if (currentAttempt < maxRetriesInCaseOfErrors && (statusCode != 403 && statusCode != 404 && statusCode != 405)) {
                	// this can happen, we try it again
                	LOG.warn("POST request: " + request.getURI() + " failed (" + (currentAttempt + 1) + ". attempt, " + (maxRetriesInCaseOfErrors - currentAttempt) + " retries left). \n Waiting " + waitMillisAfterError + "ms and retry request.", e);
                	Thread.sleep(waitMillisAfterError);
            	} else {
                	throw new Exception("POST request: " + request.getURI() + " failed. No retry left, max: " + maxRetriesInCaseOfErrors, e);
            	}
            } finally {
            	if (httpResponse != null) {
                	try {
                    	httpResponse.close();
                	} catch (Exception ce) {
                		// ignore
                	}
            	}
            }
		} // for
        return responseContent;
	}

	private CloseableHttpClient createCloseableClient(String urlStr, String user, String password, int timeout) throws Exception {
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        if (closableHttpClient == null) {
            if (user != null && user.trim().isEmpty() == false) {
        		URL url = new URL(urlStr);
                credsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(user, password));
                RequestConfig requestConfig = RequestConfig.custom()
                        .setSocketTimeout(timeout)
                        .setConnectTimeout(timeout)
                        .setConnectionRequestTimeout(timeout)
                        .setRedirectsEnabled(true)
                        .setRelativeRedirectsAllowed(false)
                        .setAuthenticationEnabled(true)
                        .build();
                AuthCache authCache = new BasicAuthCache();
                HttpHost httpHost = new HttpHost(url.getHost(), url.getPort());
                authCache.put(httpHost, new BasicScheme());
                context = HttpClientContext.create();
                context.setCredentialsProvider(credsProvider);
                context.setAuthCache(authCache);
                if (useUnsafeCertManager) {
                    UnsafeSSLHelper unsafeSSLHelper = new UnsafeSSLHelper();
                    CloseableHttpClient client = HttpClients.custom()
                            .setDefaultCredentialsProvider(credsProvider)
                            .setDefaultRequestConfig(requestConfig)
                            .setSSLContext(unsafeSSLHelper.createUnsecureSSLContext())
                            .setSSLHostnameVerifier(unsafeSSLHelper.getPassiveHostnameVerifier())
                            .build();
                	closableHttpClient = client;
                } else {
                    CloseableHttpClient client = HttpClients.custom()
                            .setDefaultCredentialsProvider(credsProvider)
                            .setDefaultRequestConfig(requestConfig)
                            .build();
                	closableHttpClient = client;
                }
                return closableHttpClient;
            } else {
                RequestConfig requestConfig = RequestConfig.custom()
                        .setSocketTimeout(timeout)
                        .setConnectTimeout(timeout)
                        .setConnectionRequestTimeout(timeout)
                        .setRedirectsEnabled(true)
                        .setRelativeRedirectsAllowed(true)
                        .build();
                CloseableHttpClient client = HttpClients.custom()
                        .setDefaultRequestConfig(requestConfig)
                        .build();
            	closableHttpClient = client;
                return client;
            }
        } else {
        	return closableHttpClient;
        }
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	public int getMaxRetriesInCaseOfErrors() {
		return maxRetriesInCaseOfErrors;
	}

	public void setMaxRetriesInCaseOfErrors(Integer maxRetriesInCaseOfErrors) {
		if (maxRetriesInCaseOfErrors != null) {
			this.maxRetriesInCaseOfErrors = maxRetriesInCaseOfErrors;
		}
	}

	public int getCurrentAttempt() {
		return currentAttempt;
	}

	public long getWaitMillisAfterError() {
		return waitMillisAfterError;
	}

	public void setWaitMillisAfterError(Long waitMillisAfterError) {
		if (waitMillisAfterError != null) {
			this.waitMillisAfterError = waitMillisAfterError;
		}
	}

	public void close() {
		if (closableHttpClient != null) {
			try {
				closableHttpClient.close();
			} catch (IOException e) {
				// ignore
			}
		}
	}

	public CloseableHttpClient getClosableHttpClient() {
		return closableHttpClient;
	}

	public void setClosableHttpClient(CloseableHttpClient closableHttpClient) {
		this.closableHttpClient = closableHttpClient;
	}

}
