import java.math.BigDecimal;
import java.util.Date;

import de.jlo.talend.execjob.TalendJob;

public class PlayTalendJob {
	
	public static void main(String[] args) throws Exception {
		String jobPath = "/home/jan-lolling/temp/job";
		TalendJob ex = new TalendJob();
		ex.setJobRootPath(jobPath);
//		ex.setJobName("test_job_with_context");
//		ex.setJobVersion("0.1");
		ex.setProject("talend_common");
		ex.setContext("var_boolean", null);
		ex.setContext("var_int", null);
		ex.setContext("var_double", 0.987d);
		ex.setContext("var_float", 0.5f);
		ex.setContext("var_short", (short) 2);
		ex.setContext("var_long", 9999l);
		ex.setContext("var_bigdecimal", new BigDecimal("123456.789"));
		ex.setContext("var_date", new Date());
		ex.setContext("var_string", "äöü\nabc");
		ex.start();
		Object value = ex.getGlobalMapValue("tLogRow_1_NB_LINE");
		System.out.println(value);
	}

}
