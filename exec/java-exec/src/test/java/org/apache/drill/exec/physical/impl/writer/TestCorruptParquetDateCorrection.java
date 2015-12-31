/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.writer;

import org.apache.drill.PlanTestBase;
import org.apache.drill.TestBuilder;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests for compatibility reading old parquet files after date corruption
 * issue was fixed in DRILL-4203.
 *
 * Drill was writing non-standard dates into parquet files for all releases
 * before 1.9.0. The values have been read by Drill correctly by Drill, but
 * external tools like Spark reading the files will see corrupted values for
 * all dates that have been written by Drill.
 *
 * This change corrects the behavior of the Drill parquet writer to correctly
 * store dates in the format given in the parquet specification.
 *
 * To maintain compatibility with old files, the parquet reader code has
 * been updated to check for the old format and automatically shift the
 * corrupted values into corrected ones automatically.
 *
 * The test cases included here should ensure that all files produced by
 * historical versions of Drill will continue to return the same values they
 * had in previous releases. For compatibility with external tools, any old
 * files with corrupted dates can be re-written using the CREATE TABLE AS
 * command (as the writer will now only produce the specification-compliant
 * values, even if after reading out of older corrupt files).
 *
 * While the old behavior was a consistent shift into an unlikely range
 * to be used in a modern database (over 10,000 years in the future), these are still
 * valid date values. In the case where these may have been written into
 * files intentionally, and we cannot be certain from the metadata if Drill
 * produced the files, an option is included to turn off the auto-correction.
 * Use of this option is assumed to be extremely unlikely, but it is included
 * for completeness.
 */
public class TestCorruptParquetDateCorrection extends PlanTestBase {

  // 4 files are in the directory:
  //    - one created with the fixed version of the reader, right before 1.9
  //        - the code was changed to write the version number 1.9 (without snapshot) into the file
  //        - for compatibility all 1.9-SNAPSHOT files are read to correct the corrupt dates
  //    - one from and old version of Drill, before we put in proper created by in metadata
  //        - this is read properly by looking at a Max value in the file statistics, to see that
  //          it is way off of a typical date value
  //        - this behavior will be able to be turned off, but will be on by default
  //    - one from the 0.6 version of Drill, before files had min/max statistics
  //        - detecting corrupt values must be deferred to actual data page reading
  //    - one from 1.4, where there is a proper created-by, but the corruption is present
  private static final String MIXED_CORRUPTED_AND_CORRECTED_DATES_PATH =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/mixed_drill_versions";
  // partitioned with 1.4.0, date values are known to be corrupt
  private static final String CORRUPTED_PARTITIONED_DATES_1_4_0_PATH =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/partitioned_with_corruption_4203";
  // partitioned with 1.2.0, no certain metadata that these were written with Drill
  // the value will be checked to see that they look corrupt and they will be corrected
  // by default. Users can use the format plugin option autoCorrectCorruptDates to disable
  // this behavior if they have foreign parquet files with valid rare date values that are
  // in the similar range as Drill's corrupt values
  private static final String CORRUPTED_PARTITIONED_DATES_1_2_PATH =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/partitioned_with_corruption_4203_1_2";
  private static final String PARQUET_DATE_FILE_WITH_NULL_FILLED_COLS =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/null_date_cols_with_corruption_4203.parquet";
  private static final String CORRECTED_PARTITIONED_DATES_1_9_PATH =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/1_9_0_partitioned_no_corruption";
  private static final String VARCHAR_PARTITIONED =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/fewtypes_varcharpartition";
  private static final String DATE_PARTITIONED =
      "[WORKING_PATH]/src/test/resources/parquet/4203_corrupt_dates/fewtypes_varcharpartition";

  private static FileSystem fs;
  private static Path path;
  static String PARTITIONED_1_2_FOLDER = "partitioned_with_corruption_4203_1_2";
  static String MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER = "mixed_partitioned";

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "local");
    fs = FileSystem.get(conf);
    path = new Path(getDfsTestTmpSchemaLocation());

    // Move files into temp directory, rewrite the metadata cache file to contain the appropriate absolute
    // path
    copyDirectoryIntoTempSpace(CORRUPTED_PARTITIONED_DATES_1_2_PATH);
    copyMetaDataCacheToTempReplacingInternalPaths("parquet/4203_corrupt_dates/drill.parquet.metadata_1_2.requires_replace.txt",
        PARTITIONED_1_2_FOLDER);
    copyDirectoryIntoTempSpace(CORRUPTED_PARTITIONED_DATES_1_2_PATH, MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER);
    copyDirectoryIntoTempSpace(CORRECTED_PARTITIONED_DATES_1_9_PATH, MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER);
    copyDirectoryIntoTempSpace(CORRUPTED_PARTITIONED_DATES_1_4_0_PATH, MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER);
  }

  /**
   * Test reading a directory full of partitioned parquet files with dates, these files have a drill version
   * number of 1.9.0 in their footers, so we can be certain they do not have corruption. The option to disable the
   * correction is passed, but it will not change the result in the case where we are certain correction
   * is NOT needed. For more info see DRILL-4203.
   */
  @Test
  public void testReadPartitionedOnCorrectedDates() throws Exception {
    try {
      for (String selection : new String[]{"*", "date_col"}) {
        // for sanity, try reading all partitions without a filter
        TestBuilder builder = testBuilder()
            .sqlQuery("select " + selection + " from table(dfs.`" + CORRECTED_PARTITIONED_DATES_1_9_PATH + "`" +
                "(type => 'parquet', autoCorrectCorruptDates => false))")
            .unOrdered()
            .baselineColumns("date_col");
        addDateBaselineVals(builder);
        builder.go();

        String query = "select " + selection + " from table(dfs.`" + CORRECTED_PARTITIONED_DATES_1_9_PATH + "` " +
            "(type => 'parquet', autoCorrectCorruptDates => false))" + " where date_col = date '1970-01-01'";
        // verify that pruning is actually taking place
        testPlanMatchingPatterns(query, new String[]{"numFiles=1"}, null);

        // read with a filter on the partition column
        testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("date_col")
            .baselineValues(new DateTime(1970, 1, 1, 0, 0))
            .go();
      }
    } finally {
      test("alter session reset all");
    }
  }

  @Test
  public void testVarcharPartitionedReadWithCorruption() throws Exception {
    testBuilder()
        .sqlQuery("select date_col from " +
            "dfs.`" + VARCHAR_PARTITIONED + "`" +
            "where length(varchar_col) = 12")
        .baselineColumns("date_col")
        .unOrdered()
        .baselineValues(new DateTime(2039, 4, 9, 0, 0))
        .baselineValues(new DateTime(1999, 1, 8, 0, 0))
        .go();
  }

  @Test
  public void testDatePartitionedReadWithCorruption() throws Exception {
    testBuilder()
        .sqlQuery("select date_col from " +
            "dfs.`" + DATE_PARTITIONED + "`" +
            "where date_col = '1999-04-08'")
        .baselineColumns("date_col")
        .unOrdered()
        .baselineValues(new DateTime(1999, 4, 8, 0, 0))
        .go();

    String sql = "select date_col from dfs.`" + DATE_PARTITIONED + "` where date_col > '1999-04-08'";
    testPlanMatchingPatterns(sql, new String[]{"numFiles=6"}, null);
  }

  /**
   * Test reading a directory full of partitioned parquet files with dates, these files have a drill version
   * number of 1.4.0 in their footers, so we can be certain they are corrupt. The option to disable the
   * correction is passed, but it will not change the result in the case where we are certain correction
   * is needed. For more info see DRILL-4203.
   */
  @Test
  public void testReadPartitionedOnCorruptedDates() throws Exception {
    try {
      for (String selection : new String[]{"*", "date_col"}) {
        // for sanity, try reading all partitions without a filter
        TestBuilder builder = testBuilder()
            .sqlQuery("select " + selection + " from table(dfs.`" + CORRUPTED_PARTITIONED_DATES_1_4_0_PATH + "`" +
                "(type => 'parquet', autoCorrectCorruptDates => false))")
            .unOrdered()
            .baselineColumns("date_col");
        addDateBaselineVals(builder);
        builder.go();

        String query = "select " + selection + " from table(dfs.`" + CORRUPTED_PARTITIONED_DATES_1_4_0_PATH + "` " +
            "(type => 'parquet', autoCorrectCorruptDates => false))" + " where date_col = date '1970-01-01'";
        // verify that pruning is actually taking place
        testPlanMatchingPatterns(query, new String[]{"numFiles=1"}, null);

        // read with a filter on the partition column
        testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("date_col")
            .baselineValues(new DateTime(1970, 1, 1, 0, 0))
            .go();
      }
    } finally {
      test("alter session reset all");
    }
  }

  @Test
  public void testReadPartitionedOnCorruptedDates_UserDisabledCorrection() throws Exception {
    try {
      for (String selection : new String[]{"*", "date_col"}) {
        // for sanity, try reading all partitions without a filter
        TestBuilder builder = testBuilder()
            .sqlQuery("select " + selection + " from table(dfs.`" + CORRUPTED_PARTITIONED_DATES_1_2_PATH + "`" +
                "(type => 'parquet', autoCorrectCorruptDates => false))")
            .unOrdered()
            .baselineColumns("date_col");
        addCorruptedDateBaselineVals(builder);
        builder.go();

        String query = "select " + selection + " from table(dfs.`" + CORRUPTED_PARTITIONED_DATES_1_2_PATH + "` " +
            "(type => 'parquet', autoCorrectCorruptDates => false))" + " where date_col = cast('15334-03-17' as date)";
        // verify that pruning is actually taking place
        testPlanMatchingPatterns(query, new String[]{"numFiles=1"}, null);

        // read with a filter on the partition column
        testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("date_col")
            .baselineValues(new DateTime(15334, 03, 17, 0, 0))
            .go();
      }
    } finally {
      test("alter session reset all");
    }
  }

  @Test
  public void testCorruptValDetectionDuringPruning() throws Exception {
    try {
      for (String selection : new String[]{"*", "date_col"}) {
        // for sanity, try reading all partitions without a filter
        TestBuilder builder = testBuilder()
            .sqlQuery("select " + selection + " from dfs.`" + CORRUPTED_PARTITIONED_DATES_1_2_PATH + "`")
            .unOrdered()
            .baselineColumns("date_col");
        addDateBaselineVals(builder);
        builder.go();

        String query = "select " + selection + " from dfs.`" + CORRUPTED_PARTITIONED_DATES_1_2_PATH + "`" +
            " where date_col = date '1970-01-01'";
        // verify that pruning is actually taking place
        testPlanMatchingPatterns(query, new String[]{"numFiles=1"}, null);

        // read with a filter on the partition column
        testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("date_col")
            .baselineValues(new DateTime(1970, 1, 1, 0, 0))
            .go();
      }
    } finally {
      test("alter session reset all");
    }
  }

  /**
   * To fix some of the corrupted dates fixed as part of DRILL-4203 it requires
   * actually looking at the values stored in the file. A column with date values
   * actually stored must be located to check a value. Just because we find one
   * column where the all values are null does not mean we can safely avoid reading
   * date columns with auto-correction, although null values do not need fixing,
   * other columns may contain actual corrupt date values.
   *
   * This test checks the case where the first columns in the file are all null filled
   * and a later column must be found to identify that the file is corrupt.
   */
  @Test
  public void testReadCorruptDatesWithNullFilledColumns() throws Exception {
    testBuilder()
        .sqlQuery("select null_dates_1, null_dates_2, non_existent_field, date_col from dfs.`" + PARQUET_DATE_FILE_WITH_NULL_FILLED_COLS + "`")
        .unOrdered()
        .baselineColumns("null_dates_1", "null_dates_2", "non_existent_field", "date_col")
        .baselineValues(null, null, null, new DateTime(1970, 1, 1, 0, 0))
        .baselineValues(null, null, null, new DateTime(1970, 1, 2, 0, 0))
        .baselineValues(null, null, null, new DateTime(1969, 12, 31, 0, 0))
        .baselineValues(null, null, null, new DateTime(1969, 12, 30, 0, 0))
        .baselineValues(null, null, null, new DateTime(1900, 1, 1, 0, 0))
        .baselineValues(null, null, null, new DateTime(2015, 1, 1, 0, 0))
        .go();
  }

  @Test
  public void testUserOverrideDateCorrection() throws Exception {
    // read once with the flat reader
    readFilesWithUserDisabledAutoCorrection();

    try {
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_NEW_RECORD_READER));
      // read all of the types with the complex reader
      readFilesWithUserDisabledAutoCorrection();
    } finally {
      test("alter session reset all");
    }

  }

  /**
   * Test reading a directory full of parquet files with dates, some of which have corrupted values
   * due to DRILL-4203.
   *
   * Tests reading the files with both the vectorized and complex parquet readers.
   *
   * @throws Exception
   */
  @Test
  public void testReadMixedOldAndNewBothReaders() throws Exception {
    /// read once with the flat reader
    readMixedCorruptedAndCorrectedDates();

    try {
      // read all of the types with the complex reader
      test(String.format("alter session set %s = true", ExecConstants.PARQUET_NEW_RECORD_READER));
      readMixedCorruptedAndCorrectedDates();
    } finally {
      test(String.format("alter session set %s = false", ExecConstants.PARQUET_NEW_RECORD_READER));
    }
  }

  public void addDateBaselineVals(TestBuilder builder) {
    builder
        .baselineValues(new DateTime(1970, 1, 1, 0, 0))
        .baselineValues(new DateTime(1970, 1, 2, 0, 0))
        .baselineValues(new DateTime(1969, 12, 31, 0, 0))
        .baselineValues(new DateTime(1969, 12, 30, 0, 0))
        .baselineValues(new DateTime(1900, 1, 1, 0, 0))
        .baselineValues(new DateTime(2015, 1, 1, 0, 0));
  }

  /**
   * These are the same values added in the addDateBaselineVals, shifted as corrupt values
   */
  public void addCorruptedDateBaselineVals(TestBuilder builder) {
    builder
        .baselineValues(new DateTime(15334, 03, 17, 0, 0))
        .baselineValues(new DateTime(15334, 03, 18, 0, 0))
        .baselineValues(new DateTime(15334, 03, 15, 0, 0))
        .baselineValues(new DateTime(15334, 03, 16, 0, 0))
        .baselineValues(new DateTime(15264, 03, 16, 0, 0))
        .baselineValues(new DateTime(15379, 03, 17, 0, 0));
  }

  public void readFilesWithUserDisabledAutoCorrection() throws Exception {
    // ensure that selecting the date column explicitly or as part of a star still results
    // in checking the file metadata for date columns (when we need to check the statistics
    // for bad values) to set the flag that the values are corrupt
    for (String selection : new String[] {"*", "date_col"}) {
      TestBuilder builder = testBuilder()
          .sqlQuery("select " + selection + " from table(dfs.`" + MIXED_CORRUPTED_AND_CORRECTED_DATES_PATH + "`" +
              "(type => 'parquet', autoCorrectCorruptDates => false))")
          .unOrdered()
          .baselineColumns("date_col");
      addDateBaselineVals(builder);
      addDateBaselineVals(builder);
      addCorruptedDateBaselineVals(builder);
      addCorruptedDateBaselineVals(builder);
      builder.go();
    }
  }

  private static String replaceWorkingPathInString(String orig) {
    return orig.replaceAll(Pattern.quote("[WORKING_PATH]"), Matcher.quoteReplacement(TestTools.getWorkingPath()));
  }

  private static void copyDirectoryIntoTempSpace(String resourcesDir) throws IOException {
    copyDirectoryIntoTempSpace(resourcesDir, null);
  }

  private static void copyDirectoryIntoTempSpace(String resourcesDir, String destinationSubDir) throws IOException {
    Path destination = path;
    if (destinationSubDir != null) {
      destination = new Path(path, destinationSubDir);
    }
    fs.copyFromLocalFile(
        new Path(replaceWorkingPathInString(resourcesDir)),
        destination);
  }

  /**
   * Metadata cache files include full paths to the files that have been scanned.
   *
   * There is no way to generate a metadata cache file with absolute paths that
   * will be guarenteed to be available on an arbitrary test machine.
   *
   * To enable testing older metadata cache files, they were generated manually
   * using older drill versions, and the absolute path up to the folder where
   * the metadata cache file appeared was manually replaced with the string
   * REPLACED_IN_TEST. Here the file is re-written into the given temporary
   * location after the REPLACED_IN_TEST string has been replaced by the actual
   * location generated during this run of the tests.
   *
   * @param srcFileOnClassPath
   * @param destFolderInTmp
   * @throws IOException
   */
  private static void copyMetaDataCacheToTempReplacingInternalPaths(String srcFileOnClassPath, String destFolderInTmp) throws IOException {
    String metadataFileContents = getFile(srcFileOnClassPath);
    Path newMetaCache = new Path(new Path(path, destFolderInTmp), ".drill.parquet_metadata");
    FSDataOutputStream outSteam = fs.create(newMetaCache);
    outSteam.writeBytes(metadataFileContents.replace("REPLACED_IN_TEST", path.toString()));
    outSteam.close();
  }

  @Test
  public void testReadOldMetadataCacheFile() throws Exception {
    // for sanity, try reading all partitions without a filter
    String query = "select date_col from dfs.`" + new Path(path, PARTITIONED_1_2_FOLDER) + "`";
    TestBuilder builder = testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("date_col");
    addDateBaselineVals(builder);
    builder.go();
    testPlanMatchingPatterns(query, new String[]{"usedMetadataFile=true"}, null);
  }

  @Test
  public void testReadOldMetadataCacheFileWithPruning() throws Exception {
    String query = "select date_col from dfs.`" + new Path(path, PARTITIONED_1_2_FOLDER) + "`" +
        " where date_col = date '1970-01-01'";
    // verify that pruning is actually taking place
    testPlanMatchingPatterns(query, new String[]{"numFiles=1", "usedMetadataFile=true"}, null);

    // read with a filter on the partition column
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("date_col")
        .baselineValues(new DateTime(1970, 1, 1, 0, 0))
        .go();
  }

  @Test
  public void testReadOldMetadataCacheFileOverrideCorrection() throws Exception {
    // for sanity, try reading all partitions without a filter
    TestBuilder builder = testBuilder()
        .sqlQuery("select date_col from table(dfs.`" + new Path(path, PARTITIONED_1_2_FOLDER) + "`" +
            "(type => 'parquet', autoCorrectCorruptDates => false))")
        .unOrdered()
        .baselineColumns("date_col");
    addCorruptedDateBaselineVals(builder);
    builder.go();

    String query = "select date_col from table(dfs.`" + new Path(path, PARTITIONED_1_2_FOLDER) + "` " +
        "(type => 'parquet', autoCorrectCorruptDates => false))" + " where date_col = cast('15334-03-17' as date)";
    // verify that pruning is actually taking place
    testPlanMatchingPatterns(query, new String[]{"numFiles=1", "usedMetadataFile=true"}, null);

    // read with a filter on the partition column
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("date_col")
        .baselineValues(new DateTime(15334, 03, 17, 0, 0))
        .go();
  }

  @Test
  public void testReadNewMetadataCacheFileOverOldAndNewFiles() throws Exception {
    String table = "dfs.`" + new Path(path, MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER) + "`";
    copyMetaDataCacheToTempReplacingInternalPaths("parquet/4203_corrupt_dates/mixed_version_partitioned_metadata.requires_replace.txt",
        MIXED_CORRUPTED_AND_CORRECTED_PARTITIONED_FOLDER);
    // for sanity, try reading all partitions without a filter
    TestBuilder builder = testBuilder()
        .sqlQuery("select date_col from " + table)
        .unOrdered()
        .baselineColumns("date_col");
    addDateBaselineVals(builder);
    addDateBaselineVals(builder);
    addDateBaselineVals(builder);
    builder.go();

    String query = "select date_col from " + table +
        " where date_col = date '1970-01-01'";
    // verify that pruning is actually taking place
    testPlanMatchingPatterns(query, new String[]{"numFiles=3", "usedMetadataFile=true"}, null);

    // read with a filter on the partition column
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("date_col")
        .baselineValues(new DateTime(1970, 1, 1, 0, 0))
        .baselineValues(new DateTime(1970, 1, 1, 0, 0))
        .baselineValues(new DateTime(1970, 1, 1, 0, 0))
        .go();
  }

  /**
   * Read a directory with parquet files where some have corrupted dates, see DRILL-4203.
   * @throws Exception
   */
  public void readMixedCorruptedAndCorrectedDates() throws Exception {
    // ensure that selecting the date column explicitly or as part of a star still results
    // in checking the file metadata for date columns (when we need to check the statistics
    // for bad values) to set the flag that the values are corrupt
    for (String selection : new String[] {"*", "date_col"}) {
      TestBuilder builder = testBuilder()
          .sqlQuery("select " + selection + " from dfs.`" + MIXED_CORRUPTED_AND_CORRECTED_DATES_PATH + "`")
          .unOrdered()
          .baselineColumns("date_col");
      for (int i = 0; i < 4; i++) {
        addDateBaselineVals(builder);
      }
      builder.go();
    }
  }

}
