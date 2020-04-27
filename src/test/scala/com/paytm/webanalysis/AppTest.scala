package com.paytm.webanalysis

import com.paytm.webanalysis.utils.Utils
import org.junit.Assert._
import org.junit._

@Test
class AppTest {

    @Test
    def testUtilsExtractorList() = {
        assertEquals(Some(List("WEBLOG_SOURCE")), Utils.getETLConfig().extractorList)
    }

    @Test
    def testUtilsTransformerList():Unit = {
        assertEquals(Some(List("PREPARE_SESSION", "SESSION_ID", "AGGREGATE_BY_IP_1",
            "AVERAGE_SESSION_TIME_2", "UNIQUE_URL_COUNT_3", "MOST_ENGAGED_USER_4")), Utils.getETLConfig().transformerList)
    }

    @Test
    def testUtilsLoaderList():Unit = {
        assertEquals(Some(List("AGGREGATION_LOADER", "AVERAGE_SESSION_TIME_LOADER", "UNIQUE_URL_LOADER", "BUSY_AGGR_LOADER")), Utils.getETLConfig().loaderList)
    }

}


